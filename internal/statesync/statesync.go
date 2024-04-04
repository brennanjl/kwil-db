package statesync

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kwilteam/kwil-db/core/log"
	"go.uber.org/zap"
)

/*
	What should this package do>?
	- basically hadnle the bootstrapping process
	that includes:
	- tracking the chunks we received from cometbft
	- validating the chunks
	- restoring the final db from the chunks
	- verifying the sanity of the sql dump: how?
	- relaunching the db from the sql dump
	- handling failure scenarios?

	What is the flow?
	1. CometBFT sends the snapshot header to the ABCI application and the app can decide whether to accept the snapshot or not: OfferSnapshot(snapshot *snapshots.Snapshot) error
	2. Once the snapshot is accept, cometbft requests for the chunks and sends them one by one to you.
	3. You validate the chunks and apply them to the db.
	4. Once all the chunks are received, you restore the db from the chunks.
	5. You verify the sanity of the sql dump.
	6. You relaunch the db from the sql dump.
	7. You handle failure scenarios.

	Things to keep track of:
	Snapshot: the metadata of the snapshot under use
	Chunks: the chunks received from cometbft

	Delete the chunks stored on disk when there are any failires and remove any in-memory state corresponding to the snapshot


*/

/*
What if different nodes use different pg_dump versions and end up with different sql dumps?

Trust related:
- Should we only accept dumps from validators?
- How do we sanitize the sql dump?
*/
type StateSyncer struct {
	dbConfig DBConfig

	inProgress           bool
	receivedSnapshotsDir string

	snapshot *SnapshotHeader
	// Chunks received till now
	chunks      map[uint32]bool
	chunkHashes map[uint32][]byte
	rcvdChunks  uint32

	// Logger
	log log.Logger
}

// Do we always expect the chunks to be received in order?

func NewStateSyncer(cfg DBConfig, dir string, logger log.Logger) *StateSyncer {
	return &StateSyncer{
		dbConfig:             cfg,
		receivedSnapshotsDir: dir,
		inProgress:           false,
		rcvdChunks:           0,
		log:                  logger,
	}
}

func (ss *StateSyncer) OfferSnapshot(snapshot *SnapshotHeader) error {
	ss.log.Info("Offering snapshot", zap.Int64("height", int64(snapshot.Height)), zap.Uint32("format", snapshot.Format), zap.String("App Hash", fmt.Sprintf("%x", snapshot.SnapshotHash)))

	// Check if we are already in the middle of a snapshot
	if ss.inProgress {
		ss.log.Error("Snapshot already in progress")
		return fmt.Errorf("snapshot already in progress")
	}

	ss.snapshot = snapshot
	ss.inProgress = true
	ss.chunks = make(map[uint32]bool, snapshot.ChunkCount)
	ss.chunkHashes = make(map[uint32][]byte, snapshot.ChunkCount)
	ss.rcvdChunks = 0

	// TODO: WHy not use the snapshot dir to store received snapshots?
	// Create a directory to store the chunks
	dir := filepath.Join(ss.receivedSnapshotsDir, fmt.Sprintf("%d", snapshot.Height), "chunks")
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		ss.log.Error("Failed to create directory", zap.String("dir", dir), zap.Error(err))
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// store snapshot header on disk
	headerFile := filepath.Join(ss.receivedSnapshotsDir, fmt.Sprintf("%d", snapshot.Height), "header.json")
	err = snapshot.SaveAs(headerFile)
	if err != nil {
		ss.log.Error("Failed to save snapshot header", zap.String("file", headerFile), zap.Error(err))
		return fmt.Errorf("failed to save snapshot header: %w", err)
	}

	return nil
}

func (ss *StateSyncer) ApplySnapshotChunk(ctx context.Context, chunk []byte, index uint32) error {
	if !ss.inProgress {
		ss.log.Error("Snapshot not in progress")
		return fmt.Errorf("snapshot not in progress")
	}

	applied, ok := ss.chunks[index]
	if ok && applied {
		ss.log.Error("Chunk already applied", zap.Uint32("index", index))
		return fmt.Errorf("chunk already applied")
	}

	if index >= ss.snapshot.ChunkCount {
		ss.log.Error("Invalid chunk index", zap.Uint32("index", index), zap.Uint32("chunkCount", ss.snapshot.ChunkCount))
		return fmt.Errorf("invalid chunk index")
	}

	ss.chunks[index] = true
	ss.rcvdChunks++

	// store the chunk on disk
	chunkFileName := filepath.Join(ss.receivedSnapshotsDir, fmt.Sprintf("%d", ss.snapshot.Height), "chunks", fmt.Sprintf("chunk-%d", index))
	chunkFile, err := os.Create(chunkFileName)
	if err != nil {
		ss.log.Error("Failed to create chunk file", zap.String("file", chunkFileName), zap.Error(err))
		return fmt.Errorf("failed to create chunk file: %w", err)
	}
	defer chunkFile.Close()

	_, err = chunkFile.Write(chunk)
	if err != nil {
		ss.log.Error("Failed to write chunk to file", zap.String("file", chunkFileName), zap.Error(err))
		return fmt.Errorf("failed to write chunk to file: %w", err)
	}

	ss.log.Info("Applied chunk", zap.Uint32("index", index))

	// Check if all chunks have been received
	if ss.rcvdChunks == ss.snapshot.ChunkCount {
		ss.log.Info("All chunks received")
		// Restore the DB from the chunks
		err := ss.restoreDB(ctx)
		if err != nil {
			ss.log.Error("Failed to restore DB", zap.Error(err))
			return fmt.Errorf("failed to restore DB: %w", err)
		}
		ss.log.Info("DB restored")
		ss.inProgress = false
		ss.snapshot = nil
		ss.chunks = nil
		ss.chunkHashes = nil
		ss.rcvdChunks = 0

		// TODO: Delete the chunks stored on disks, probably not if using the snapshot dir to store received snapshots and reuse them for future nodes?
	}

	return nil
}

func (ss *StateSyncer) IsDBRestored() bool {
	return ss.rcvdChunks == ss.snapshot.ChunkCount
}

func (ss *StateSyncer) restoreDB(ctx context.Context) error {
	// Unzip the chunks and restore the db
	// Restore the db from the sql dump

	snapshotDir := filepath.Join(ss.receivedSnapshotsDir, fmt.Sprintf("%d", ss.snapshot.Height), "chunks")
	streamer := NewStreamer(ss.snapshot.ChunkCount, snapshotDir, ss.log)
	defer streamer.Close()

	gzipReader, err := gzip.NewReader(streamer)
	if err != nil {
		ss.log.Error("Failed to create gzip reader", zap.Error(err))
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	cmd := exec.CommandContext(ctx, "psql", "-U", "kwild", "-h", "localhost", "-p", "5435", "-d", "kwild")
	cmd.Stdin = gzipReader

	if err := cmd.Start(); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}
