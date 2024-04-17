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

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"go.uber.org/zap"
)

type StateSyncer struct {
	dbConfig DBConfig
	// TODO: Statesync should reference snapshotter, so that it can register the snapshot with which it bootstrapped.

	inProgress   bool
	snapshotsDir string
	rpcServers   []string
	snapshot     *Snapshot
	// Chunks received till now
	chunks     map[uint32]bool
	rcvdChunks uint32

	snapshotProviders []*rpchttp.HTTP // cometbft rpc servers

	// Logger
	log log.Logger
}

// Do we always expect the chunks to be received in order?
func NewStateSyncer(ctx context.Context, cfg DBConfig, dir string, chainID string, providers []string, logger log.Logger) *StateSyncer {

	ss := &StateSyncer{
		dbConfig:     cfg,
		snapshotsDir: dir,
		inProgress:   false,
		rcvdChunks:   0,
		log:          logger,
		rpcServers:   providers,
	}

	for _, s := range ss.rpcServers {
		clt, err := rpcClient(s)
		if err != nil {
			logger.Error("Failed to create rpc client", zap.String("server", s), zap.Error(err))
			return nil
		}
		ss.snapshotProviders = append(ss.snapshotProviders, clt)
	}

	return ss
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

func (ss *StateSyncer) OfferSnapshot(snapshot *Snapshot) error {
	ss.log.Info("Offering snapshot", zap.Int64("height", int64(snapshot.Height)), zap.Uint32("format", snapshot.Format), zap.String("App Hash", fmt.Sprintf("%x", snapshot.SnapshotHash)))

	// Check if we are already in the middle of a snapshot
	if ss.inProgress {
		return ErrStateSyncInProgress
	}

	// Validate the snapshot header
	err := ss.validateSnapshotHeader(*snapshot)
	if err != nil {
		return err
	}

	ss.snapshot = snapshot
	ss.inProgress = true
	ss.chunks = make(map[uint32]bool, snapshot.ChunkCount)
	ss.rcvdChunks = 0

	dir := snapshotChunkDir(ss.snapshotsDir, snapshot.Height, snapshot.Format)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		ss.resetStateSync()
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// store snapshot header on disk
	headerFile := snapshotHeaderFile(ss.snapshotsDir, snapshot.Height, snapshot.Format)
	err = snapshot.SaveAs(headerFile)
	if err != nil {
		ss.resetStateSync()
		return fmt.Errorf("failed to save snapshot header: %w", err)
	}

	return nil
}

func (ss *StateSyncer) ApplySnapshotChunk(ctx context.Context, chunk []byte, index uint32) (bool, error) {
	if !ss.inProgress {
		return false, ErrStateSyncNotInProgress
	}

	applied, ok := ss.chunks[index]
	if ok && applied {
		return false, nil
	}

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
	chunkFileName := snapshotChunkFile(ss.snapshotsDir, ss.snapshot.Height, ss.snapshot.Format, index)
	chunkFile, err := os.Create(chunkFileName)
	if err != nil {
		return false, ErrRetrySnapshotChunk
	}
	defer chunkFile.Close()

	_, err = chunkFile.Write(chunk)
	if err != nil {
		return false, ErrRetrySnapshotChunk
	}

	ss.log.Info("Applied chunk", zap.Uint32("index", index))

	ss.chunks[index] = true
	ss.rcvdChunks += 1

	// Check if all chunks have been received
	if ss.rcvdChunks == ss.snapshot.ChunkCount {
		ss.log.Info("All chunks received")
		// Restore the DB from the chunks
		err := ss.restoreDB(ctx)
		if err != nil {
			ss.resetStateSync()
			return false, ErrRejectSnapshot
		}
		ss.log.Info("DB restored")
		ss.inProgress = false
		ss.snapshot = nil
		ss.chunks = nil
		ss.rcvdChunks = 0
	}

	return false, nil
}

func (ss *StateSyncer) restoreDB(ctx context.Context) error {
	// Unzip the chunks and restore the db
	// Restore the db from the sql dump
	cmd := exec.CommandContext(ctx, "psql", "-U", "kwild", "-h", "localhost", "-p", "5435", "-d", "kwild")

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	if err := ss.decompressAndValidateChunkStreams(stdinPipe); err != nil {
		return err
	}

	stdinPipe.Close() // signals the end of the input

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func (ss *StateSyncer) decompressAndValidateChunkStreams(output io.Writer) error {
	chunksDir := snapshotChunkDir(ss.snapshotsDir, ss.snapshot.Height, ss.snapshot.Format)
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
	ss.log.Info("Decompressed chunk streams", zap.Int64("bytes compressed", n))
	hash := hasher.Sum(nil)

	// Validate the hash of the decompressed chunks
	if !bytes.Equal(hash, ss.snapshot.SnapshotHash) {
		return fmt.Errorf("invalid snapshot hash")
	}
	return nil
}

func (ss *StateSyncer) validateSnapshotHeader(snapshot Snapshot) error {
	// Check if the snapshot format is valid
	if snapshot.Format != 0 {
		return ErrUnsupportedSnapshotFormat
	}

	// Check if the snapshot height is valid
	if snapshot.Height <= 0 {
		return ErrInvalidSnapshot
	}

	// Check if the snapshot chunk count is valid
	if snapshot.ChunkCount <= 0 {
		return ErrInvalidSnapshot
	}

	if snapshot.ChunkCount != uint32(len(snapshot.ChunkHashes)) {
		return ErrInvalidSnapshot
	}

	// Query the snapshot providers to check if the snapshot is valid
	height := fmt.Sprintf("%d", snapshot.Height)
	verified := false
	for _, clt := range ss.snapshotProviders {
		res, err := clt.ABCIQuery(context.Background(), "/snapshot/height", []byte(height))
		if err != nil {
			ss.log.Error("Failed to query snapshot", zap.Error(err))
			continue
		}

		if res.Response.Value != nil {
			var snap Snapshot
			err = json.Unmarshal(res.Response.Value, &snap)
			if err != nil {
				ss.log.Error("Failed to unmarshal snapshot", zap.Error(err))
				continue
			}

			if snap.Height != snapshot.Height || snap.SnapshotSize != snapshot.SnapshotSize ||
				snap.ChunkCount != snapshot.ChunkCount || !bytes.Equal(snap.SnapshotHash, snapshot.SnapshotHash) {
				ss.log.Error("Invalid snapshot", zap.Uint64("height", snapshot.Height))
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
	ssDir := snapshotHeightDir(ss.snapshotsDir, ss.snapshot.Height)
	os.RemoveAll(ssDir)

	ss.inProgress = false
	ss.snapshot = nil
	ss.chunks = nil
	ss.rcvdChunks = 0
}
