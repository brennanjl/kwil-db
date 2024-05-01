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
	"path/filepath"
	"strings"

	"github.com/kwilteam/kwil-db/core/log"
	"github.com/kwilteam/kwil-db/internal/snapshots"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"go.uber.org/zap"
)

const (
	ABCISnapshotQueryPath = "/snapshot/height"
)

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

	// Logger
	log log.Logger
}

func NewStateSyncer(ctx context.Context, cfg *snapshots.DBConfig, snapshotDir string, providers []string, logger log.Logger) *StateSyncer {

	ss := &StateSyncer{
		dbConfig:     cfg,
		snapshotsDir: snapshotDir,
		log:          logger,
	}

	for _, s := range providers {
		clt, err := ChainRPCClient(s)
		if err != nil {
			logger.Error("Failed to create rpc client", zap.String("server", s), zap.Error(err))
			return nil
		}
		ss.snapshotProviders = append(ss.snapshotProviders, clt)
	}

	// Ensure that the snasphot directory exists and is empty
	if err := os.RemoveAll(snapshotDir); err != nil {
		logger.Error("Failed to delete snapshot directory", zap.String("dir", snapshotDir), zap.Error(err))
		return nil
	}
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		logger.Error("Failed to create snapshot directory", zap.String("dir", snapshotDir), zap.Error(err))
		return nil
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
	return nil
}

// ApplySnapshotChunk accepts a chunk and stores it on disk for later processing if valid
// If all chunks are received, it starts the process of restoring the database
func (ss *StateSyncer) ApplySnapshotChunk(ctx context.Context, chunk []byte, index uint32) (bool, bool, error) {
	if !ss.inProgress {
		return false, false, ErrStateSyncNotInProgress
	}

	// Check if the chunk has already been applied
	applied, ok := ss.chunks[index]
	if ok && applied {
		return false, false, nil
	}

	// Check if the chunk index is valid
	if index >= ss.snapshot.ChunkCount {
		ss.log.Error("Invalid chunk index", zap.Uint32("index", index), zap.Uint32("chunkCount", ss.snapshot.ChunkCount))
		return false, false, ErrRejectSnapshotChunk
	}

	// Validate the chunk hash
	chunkHash := sha256.Sum256(chunk)
	if !bytes.Equal(chunkHash[:], ss.snapshot.ChunkHashes[index][:]) {
		return false, true, ErrRejectSnapshotChunk
	}

	// store the chunk on disk
	chunkFileName := filepath.Join(ss.snapshotsDir, fmt.Sprintf("chunk-%d.sql.gz", index))
	chunkFile, err := os.Create(chunkFileName)
	if err != nil {
		return false, false, ErrRetrySnapshotChunk
	}
	defer chunkFile.Close()

	err = os.WriteFile(chunkFileName, chunk, 0755)
	if err != nil {
		os.Remove(chunkFileName)
		return false, false, ErrRetrySnapshotChunk
	}

	ss.log.Info("Applied snapshot chunk", zap.Uint64("height", ss.snapshot.Height), zap.Uint32("index", index))

	ss.chunks[index] = true
	ss.rcvdChunks += 1

	// Kick off the process of restoring the database if all chunks are received
	if ss.rcvdChunks == ss.snapshot.ChunkCount {
		ss.log.Info("All chunks received - Starting DB restore process")
		// Restore the DB from the chunks
		streamer := NewStreamer(ss.snapshot.ChunkCount, ss.snapshotsDir, ss.log)
		defer streamer.Close()
		reader, err := gzip.NewReader(streamer)
		if err != nil {
			ss.resetStateSync()
			return false, false, ErrRejectSnapshot
		}
		defer reader.Close()

		err = RestoreDB(ctx, reader, ss.dbConfig.DBName, ss.dbConfig.DBUser,
			ss.dbConfig.DBPass, ss.dbConfig.DBHost, ss.dbConfig.DBPort,
			ss.snapshot.SnapshotHash, ss.log)
		if err != nil {
			ss.resetStateSync()
			return false, false, ErrRejectSnapshot
		}
		ss.log.Info("DB restored")

		ss.inProgress = false
		ss.chunks = nil
		ss.rcvdChunks = 0
		ss.snapshot = nil
		return true, false, nil
	}

	return false, false, nil
}

// RestoreDB restores the database from the logical sql dump using psql command
// It also validates the snapshot hash, before restoring the database
func RestoreDB(ctx context.Context, snapshot io.Reader,
	dbName, dbUser, dbPass, dbHost, dbPort string,
	snapshotHash []byte, logger log.Logger) error {
	// unzip and stream the sql dump to psql
	cmd := exec.CommandContext(ctx,
		"psql",
		"--username", dbUser,
		"--host", dbHost,
		"--port", dbPort,
		"--dbname", dbName,
		"--no-password",
	)
	if dbPass != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+dbPass)
	}

	// cmd.Stdout = &stderr
	stdinPipe, err := cmd.StdinPipe() // stdin for psql command
	if err != nil {
		return err
	}
	defer stdinPipe.Close()

	logger.Info("Restore DB: ", zap.String("command", cmd.String()))

	if err := cmd.Start(); err != nil {
		return err
	}

	// decompress the chunk streams and stream the sql dump to psql stdinPipe
	if err := decompressAndValidateSnapshotHash(stdinPipe, snapshot, snapshotHash); err != nil {
		return err
	}

	stdinPipe.Close() // signifies the end of the input stream to the psql command

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}

// decompressAndValidateChunkStreams decompresses the chunk streams and validates the snapshot hash
func decompressAndValidateSnapshotHash(output io.Writer, reader io.Reader, snapshotHash []byte) error {
	hasher := sha256.New()
	_, err := io.Copy(io.MultiWriter(output, hasher), reader)
	if err != nil {
		return fmt.Errorf("failed to decompress chunk streams: %w", err)
	}
	hash := hasher.Sum(nil)

	// Validate the hash of the decompressed chunks
	if !bytes.Equal(hash, snapshotHash) {
		return fmt.Errorf("invalid snapshot hash %x, expected %x", hash, snapshotHash)
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
	ss.inProgress = false
	ss.snapshot = nil
	ss.chunks = nil
	ss.rcvdChunks = 0

	os.RemoveAll(ss.snapshotsDir)
	os.MkdirAll(ss.snapshotsDir, 0755)
}

// rpcClient sets up a new RPC client
func ChainRPCClient(server string) (*rpchttp.HTTP, error) {
	if !strings.Contains(server, "://") {
		server = "http://" + server
	}
	c, err := rpchttp.New(server, "/websocket")
	if err != nil {
		return nil, err
	}
	return c, nil
}
