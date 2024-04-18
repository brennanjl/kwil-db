package statesync

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/kwilteam/kwil-db/core/log"
	"github.com/kwilteam/kwil-db/internal/snapshots"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"go.uber.org/zap"
)

const (
	ABCISnapshotQueryPath = "/snapshot/height"
)

type SnapshotStore interface {
	RegisterSnapshot(snapshot *snapshots.Snapshot) error
}

type StateSyncer struct {
	// statesync configuration
	dbConfig *snapshots.DBConfig

	// directory to store snapshots and chunks
	// same as the snapshot store directory
	// as we allow to reuse the received snapshots
	snapshotsDir string

	// trusted snapshot providers for verification - cometbft rfc servers
	snapshotProviders []*rpchttp.HTTP

	// State syncer state
	inProgress bool
	snapshot   *snapshots.Snapshot
	chunks     map[uint32]bool // Chunks received till now
	rcvdChunks uint32          // Number of chunks received till now

	// snapshotStore
	snapshotStore SnapshotStore
	// Logger
	log log.Logger
}

func NewStateSyncer(ctx context.Context, cfg *snapshots.DBConfig, snapshotDir string, providers []string, snapshotStore SnapshotStore, logger log.Logger) *StateSyncer {

	ss := &StateSyncer{
		dbConfig:      cfg,
		snapshotsDir:  snapshotDir,
		snapshotStore: snapshotStore,
		log:           logger,
	}

	for _, s := range providers {
		clt, err := rpcClient(s)
		if err != nil {
			logger.Error("Failed to create rpc client", zap.String("server", s), zap.Error(err))
			return nil
		}
		ss.snapshotProviders = append(ss.snapshotProviders, clt)
	}

	return ss
}

// OfferSnapshot checks if the snapshot is valid and kicks off the state sync process
// accepted snapshot is stored on disk for later processing
func (ss *StateSyncer) OfferSnapshot(snapshot *snapshots.Snapshot) error {
	ss.log.Info("Offering snapshot", zap.Int64("height", int64(snapshot.Height)), zap.Uint32("format", snapshot.Format), zap.String("App Hash", fmt.Sprintf("%x", snapshot.SnapshotHash)))

	// Check if we are already in the middle of a snapshot
	if ss.inProgress {
		return ErrStateSyncInProgress
	}

	// Validate the snapshot
	err := ss.validateSnapshot(*snapshot)
	if err != nil {
		return err
	}

	ss.snapshot = snapshot
	ss.inProgress = true
	ss.chunks = make(map[uint32]bool, snapshot.ChunkCount)
	ss.rcvdChunks = 0

	dir := snapshots.SnapshotChunkDir(ss.snapshotsDir, snapshot.Height, snapshot.Format)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		ss.resetStateSync()
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// store snapshot header on disk
	headerFile := snapshots.SnapshotHeaderFile(ss.snapshotsDir, snapshot.Height, snapshot.Format)
	err = snapshot.SaveAs(headerFile)
	if err != nil {
		ss.resetStateSync()
		return fmt.Errorf("failed to save snapshot header: %w", err)
	}

	return nil
}

// ApplySnapshotChunk accepts a chunk and stores it on disk for later processing if valid
// If all chunks are received, it starts the process of restoring the database
func (ss *StateSyncer) ApplySnapshotChunk(ctx context.Context, chunk []byte, index uint32) (bool, error) {
	if !ss.inProgress {
		return false, ErrStateSyncNotInProgress
	}

	// Check if the chunk has already been applied
	applied, ok := ss.chunks[index]
	if ok && applied {
		return false, nil
	}

	// Check if the chunk index is valid
	if index >= ss.snapshot.ChunkCount {
		ss.log.Error("Invalid chunk index", zap.Uint32("index", index), zap.Uint32("chunkCount", ss.snapshot.ChunkCount))
		return false, ErrRejectSnapshotChunk
	}

	// Validate the chunk hash
	hash := sha256.New()
	hash.Write(chunk)
	chunkHash := hash.Sum(nil)
	if !bytes.Equal(chunkHash[:], ss.snapshot.ChunkHashes[index]) {
		return true, ErrRejectSnapshotChunk
	}

	// store the chunk on disk
	chunkFileName := snapshots.SnapshotChunkFile(ss.snapshotsDir, ss.snapshot.Height, ss.snapshot.Format, index)
	chunkFile, err := os.Create(chunkFileName)
	if err != nil {
		return false, ErrRetrySnapshotChunk
	}
	defer chunkFile.Close()

	_, err = chunkFile.Write(chunk)
	if err != nil {
		os.Remove(chunkFileName)
		return false, ErrRetrySnapshotChunk
	}

	ss.log.Info("Applied snapshot chunk", zap.Uint64("height", ss.snapshot.Height), zap.Uint32("index", index))

	ss.chunks[index] = true
	ss.rcvdChunks += 1

	// Kick off the process of restoring the database if all chunks are received
	if ss.rcvdChunks == ss.snapshot.ChunkCount {
		ss.log.Info("All chunks received - Starting DB restore process")
		// Restore the DB from the chunks
		err := ss.restoreDB(ctx)
		if err != nil {
			ss.resetStateSync()
			return false, ErrRejectSnapshot
		}
		ss.log.Info("DB restored")

		ss.inProgress = false
		ss.chunks = nil
		ss.rcvdChunks = 0

		// Register the snapshot with the snapshot store for future use
		err = ss.snapshotStore.RegisterSnapshot(ss.snapshot)
		if err != nil {
			ss.snapshot = nil
			return false, nil // fail silently? as its okay to not register this snapshot
		}

		ss.snapshot = nil
	}

	return false, nil
}

// restoreDB restores the database from the logical sql dump
// This decompresses the chunks and streams the decompressed sql dump
// to psql command for restoring the database
func (ss *StateSyncer) restoreDB(ctx context.Context) error {
	// unzip and stream the sql dump to psql
	cmd := exec.CommandContext(ctx, "psql", "-U", "kwild", "-h", "localhost", "-p", "5435", "-d", "kwild")

	stdinPipe, err := cmd.StdinPipe() // stdin for psql command
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	// decompress the chunk streams and stream the sql dump to psql stdinPipe
	if err := ss.decompressAndValidateChunkStreams(stdinPipe); err != nil {
		return err
	}

	stdinPipe.Close() // end of the input

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}

// decompressAndValidateChunkStreams decompresses the chunk streams and validates the snapshot hash
func (ss *StateSyncer) decompressAndValidateChunkStreams(output io.Writer) error {
	chunksDir := snapshots.SnapshotChunkDir(ss.snapshotsDir, ss.snapshot.Height, ss.snapshot.Format)
	streamer := NewStreamer(ss.snapshot.ChunkCount, chunksDir, ss.log)
	defer streamer.Close()

	gzipReader, err := gzip.NewReader(streamer)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(output, hasher), gzipReader)
	if err != nil {
		return fmt.Errorf("failed to decompress chunk streams: %w", err)
	}
	ss.log.Debug("Decompressed chunk streams", zap.Int64("bytes compressed", n))
	hash := hasher.Sum(nil)

	// Validate the hash of the decompressed chunks
	if !bytes.Equal(hash, ss.snapshot.SnapshotHash) {
		return fmt.Errorf("invalid snapshot hash")
	}
	return nil
}

// validateSnapshot validates the snapshot against the trusted snapshot providers
func (ss *StateSyncer) validateSnapshot(snapshot snapshots.Snapshot) error {
	// Check if the snapshot's contents are valid
	if snapshot.Format != snapshots.DefaultSnapshotFormat {
		return ErrUnsupportedSnapshotFormat
	}

	if snapshot.Height <= 0 || snapshot.ChunkCount <= 0 ||
		snapshot.ChunkCount != uint32(len(snapshot.ChunkHashes)) {
		return ErrInvalidSnapshot
	}

	// Query the snapshot providers to check if the snapshot is valid
	height := fmt.Sprintf("%d", snapshot.Height)
	verified := false
	for _, clt := range ss.snapshotProviders {
		res, err := clt.ABCIQuery(context.Background(), ABCISnapshotQueryPath, []byte(height))
		if err != nil {
			ss.log.Info("Failed to query snapshot", zap.Error(err)) // failover to next provider
			continue
		}

		if res.Response.Value != nil {
			var snap snapshots.Snapshot
			err = json.Unmarshal(res.Response.Value, &snap)
			if err != nil {
				ss.log.Error("Failed to unmarshal snapshot", zap.Error(err))
				continue
			}

			if snap.Height != snapshot.Height || snap.SnapshotSize != snapshot.SnapshotSize ||
				snap.ChunkCount != snapshot.ChunkCount || !bytes.Equal(snap.SnapshotHash, snapshot.SnapshotHash) {
				ss.log.Error("Invalid snapshot", zap.Uint64("height", snapshot.Height), zap.Any("Expected ", snap), zap.Any("Actual", snapshot))
				break
			}

			verified = true
			break
		}
	}

	if !verified {
		return ErrInvalidSnapshot
	}

	return nil
}

func (ss *StateSyncer) resetStateSync() {
	ssDir := snapshots.SnapshotHeightDir(ss.snapshotsDir, ss.snapshot.Height)
	os.RemoveAll(ssDir)

	ss.inProgress = false
	ss.snapshot = nil
	ss.chunks = nil
	ss.rcvdChunks = 0
}

// rpcClient sets up a new RPC client
func rpcClient(server string) (*rpchttp.HTTP, error) {
	if !strings.Contains(server, "://") {
		server = "http://" + server
	}
	c, err := rpchttp.New(server, "/websocket")
	if err != nil {
		return nil, err
	}
	return c, nil
}
