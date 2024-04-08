package statesync

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"

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
	// TODO: Statesync should reference snapshotter, so that it can register the snapshot with which it bootstrapped.

	inProgress   bool
	snapshotsDir string

	snapshot *SnapshotHeader
	// Chunks received till now
	chunks     map[uint32]bool
	rcvdChunks uint32

	// Logger
	log log.Logger
}

// Do we always expect the chunks to be received in order?
func NewStateSyncer(cfg DBConfig, dir string, logger log.Logger) *StateSyncer {
	return &StateSyncer{
		dbConfig:     cfg,
		snapshotsDir: dir,
		inProgress:   false,
		rcvdChunks:   0,
		log:          logger,
	}
}

// TODO: maybe change the behavior of this function
func (ss *StateSyncer) IsDBRestored() bool {
	return ss.rcvdChunks == ss.snapshot.ChunkCount
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
	ss.rcvdChunks = 0

	dir := snapshotChunkDir(ss.snapshotsDir, snapshot.Height, snapshot.Format)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		ss.log.Error("Failed to create directory", zap.String("dir", dir), zap.Error(err))
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// store snapshot header on disk
	headerFile := SnapshotHeaderFile(ss.snapshotsDir, snapshot.Height, snapshot.Format)
	err = snapshot.SaveAs(headerFile)
	if err != nil {
		// TODO: delete the directory created above
		deleteSnapshotDir(snapshotHeightDir(ss.snapshotsDir, snapshot.Height))

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

	// Validate the chunk hash
	hash := sha256.New()
	hash.Write(chunk)
	chunkHash := hash.Sum(nil)

	if !bytes.Equal(chunkHash[:], ss.snapshot.ChunkHashes[index]) {
		ss.log.Error("Invalid chunk hash", zap.Uint32("index", index), zap.String("Chunk Hash", fmt.Sprintf("%x", chunkHash)), zap.String("Expected Hash", fmt.Sprintf("%x", ss.snapshot.ChunkHashes[index])))
		return fmt.Errorf("invalid chunk hash")
		// TODO: delete the chunks stored on disk
	}

	// store the chunk on disk
	chunkFileName := snapshotChunkFile(ss.snapshotsDir, ss.snapshot.Height, ss.snapshot.Format, index)
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

	ss.chunks[index] = true
	ss.rcvdChunks++

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
		ss.rcvdChunks = 0

		// TODO: Delete the chunks stored on disks, probably not if using the snapshot dir to store received snapshots and reuse them for future nodes?
	}

	return nil
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

	if err := ss.DecompressChunkStreams(stdinPipe); err != nil {
		return err
	}

	stdinPipe.Close() // signals the end of the input

	if err := cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func (ss *StateSyncer) DecompressChunkStreams(output io.Writer) error {
	chunksDir := snapshotChunkDir(ss.snapshotsDir, ss.snapshot.Height, ss.snapshot.Format)
	streamer := NewStreamer(ss.snapshot.ChunkCount, chunksDir, ss.log)
	defer streamer.Close()

	// Create a gzip reader
	gzipReader, err := gzip.NewReader(streamer)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	_, err = io.Copy(output, gzipReader)
	if err != nil {
		return fmt.Errorf("failed to decompress chunk streams: %w", err)
	}
	return nil
}
