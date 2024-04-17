package statesync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/kwilteam/kwil-db/core/log"
	"go.uber.org/zap"
)

/*
	SnapshotStore Layout on disk

	SnapshotsDir:
		snapshot-<height1>:
			snapshot-format-0
				header.json
				chunks:
					chunk-0
					chunk-1
					...
					chunk-n
			snapshot-format-1
				header.json
				chunks:
					chunk-0
					chunk-1
					...
					chunk-n

		snapshot-<height2>:
			snapshot-format-0
				header.json
				chunks:
					chunk-0
					chunk-1
					...
					chunk-n
*/

// const (
// 	// chunkSize = 16 * 1024 * 1024 // 16MB
// 	chunkSize int64 = 5 * 1024 // 10KB
// )

type DBConfig struct {
	DBUser string
	DBPass string
	DBHost string
	DBPort string
}

type SnapshotConfig struct {
	// Snapshot store configuration
	SnapshotDir     string
	MaxSnapshots    int
	RecurringHeight uint64

	// Database configuration'
	DbConfig DBConfig
}

type SnapshotStore struct {
	cfg *SnapshotConfig
	// Snapshot store state
	snapshots       map[uint64]*Snapshot // Map of snapshot height to snapshot header
	snapshotHeights []uint64             // List of snapshot heights
	snapshotsMtx    sync.RWMutex         // Protects access to snapshots and snapshotHeights

	log log.Logger
}

// Snapshot is the header of a snapshot file representing the snapshot of the database at a certain height.
// It contains the height, format, chunk count, hash, size, and name of the snapshot.
type Snapshot struct {
	Height       uint64   `json:"height"`
	Format       uint32   `json:"format"`
	ChunkHashes  [][]byte `json:"chunk_hashes"`
	ChunkCount   uint32   `json:"chunk_count"`
	SnapshotHash []byte   `json:"hash"`
	SnapshotSize uint64   `json:"size"`
}

func (s *Snapshot) SaveAs(file string) error {
	bts, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(file, bts, 0644)
}

func LoadSnapshotHeader(file string) (*Snapshot, error) {
	bts, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var header Snapshot
	if err := json.Unmarshal(bts, &header); err != nil {
		return nil, err
	}
	return &header, nil
}

func NewSnapshotStore(cfg *SnapshotConfig, logger log.Logger) (*SnapshotStore, error) {
	ss := &SnapshotStore{
		cfg:             cfg,
		snapshots:       make(map[uint64]*Snapshot),
		snapshotHeights: make([]uint64, 0),
		log:             logger,
	}

	// List number of snapshots in the snapshot directory
	files, err := os.ReadDir(cfg.SnapshotDir)
	if err != nil {
		return ss, nil
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		fileName := file.Name() // format: block-<height>
		names := strings.Split(fileName, "-")
		if len(names) != 2 {
			logger.Error("Invalid snapshot directory", zap.String("dir", cfg.SnapshotDir))
			return nil, fmt.Errorf("invalid snapshot directory: %s", cfg.SnapshotDir)
		}
		height := names[1]
		heightInt, err := strconv.ParseUint(height, 10, 64)
		if err != nil {
			logger.Error("Failed to parse snapshot directory", zap.String("dir", cfg.SnapshotDir), zap.Error(err))
			return nil, fmt.Errorf("failed to parse snapshot directory: %w", err)
		}
		ss.snapshotHeights = append(ss.snapshotHeights, heightInt)

		// Load snapshot header
		headerFile := snapshotHeaderFile(cfg.SnapshotDir, heightInt, 0)
		header, err := LoadSnapshotHeader(headerFile)
		if err != nil {
			logger.Error("Failed to load snapshot header", zap.String("file", headerFile), zap.Error(err))
			return nil, fmt.Errorf("failed to load snapshot header: %w", err)
		}
		ss.snapshots[heightInt] = header
	}

	return ss, nil
}

// IsSnapshotDue checks if a snapshot is due at the given height.
// Snapshots should be only enabled after catch up.
func (s *SnapshotStore) IsSnapshotDue(height uint64) bool {
	return (height % uint64(s.cfg.RecurringHeight)) == 0
}

// CreateSnapshot creates a snapshot at the given height.
// It ensures that the number of snapshots does not exceed the maximum number of snapshots.
// It deletes the oldest snapshot if the maximum number of snapshots has been reached.
// It creates a directory based on the height of the snapshot and stores the snapshot chunks and header file in the directory.
func (s *SnapshotStore) CreateSnapshot(ctx context.Context, height uint64, snapshotID string) error {
	// Create a snapshot of the database at the given height
	if err := s.createSnapshotAtHeight(ctx, height, snapshotID); err != nil {
		return fmt.Errorf("failed to create snapshot at height %d: %w", height, err)
	}

	// Check if the number of snapshots exceeds the maximum number of snapshots
	for len(s.snapshotHeights) > s.cfg.MaxSnapshots {
		if err := s.deleteOldestSnapshot(); err != nil {
			return fmt.Errorf("failed to delete oldest snapshot: %w", err)
		}
	}

	return nil
}

func (s *SnapshotStore) createSnapshotAtHeight(ctx context.Context, height uint64, snapshotID string) error {
	// Stage1: Dump the database at the given height and snapshot ID
	dump1, err := s.dbSnapshot(ctx, height, 0, snapshotID)
	if err != nil {
		return fmt.Errorf("failed to create snapshot at height %d: %w", height, err)
	}

	// Stage2: Sanitize the dump
	dump2, hash, err := s.sanitizeDump(height, 0, dump1)
	if err != nil {
		return fmt.Errorf("failed to sanitize snapshot at height %d: %w", height, err)
	}

	// Stage3: Compress the dump
	dump3, err := s.compressDump(ctx, height, 0, dump2, true)
	if err != nil {
		return fmt.Errorf("failed to compress snapshot at height %d: %w", height, err)
	}

	// Stage4: Split the dump into chunks
	if err := s.splitDump(height, 0, dump3, hash); err != nil {
		return fmt.Errorf("failed to split snapshot into chunks at height %d: %w", height, err)
	}
	return nil
}

func deleteSnapshotDir(snapshotDir string) error {
	if err := os.RemoveAll(snapshotDir); err != nil {
		return fmt.Errorf("failed to delete snapshot directory %s: %w", snapshotDir, err)
	}
	return nil
}

// DeleteOldestSnapshot deletes the oldest snapshot.
// Deletes the internal and fs snapshot files and references corresponding to the oldest snapshot.
func (s *SnapshotStore) deleteOldestSnapshot() error {
	s.snapshotsMtx.Lock()
	defer s.snapshotsMtx.Unlock()

	if len(s.snapshotHeights) == 0 {
		return nil
	}

	// Delete the oldest snapshot
	snapshot := s.snapshots[s.snapshotHeights[0]]
	heightDir := snapshotHeightDir(s.cfg.SnapshotDir, snapshot.Height)
	if err := deleteSnapshotDir(heightDir); err != nil {
		return fmt.Errorf("failed to delete snapshot directory %s: %w", heightDir, err)
	}

	delete(s.snapshots, s.snapshotHeights[0])
	s.snapshotHeights = s.snapshotHeights[1:]
	return nil
}

// List snapshots should return the metadata corresponding to all the existing snapshots.
func (s *SnapshotStore) ListSnapshots() []*Snapshot {
	// Make copy of snapshots
	s.snapshotsMtx.RLock()
	defer s.snapshotsMtx.RUnlock()

	snaps := make([]*Snapshot, len(s.snapshots))
	counter := 0
	for _, snap := range s.snapshots {
		// Deep copy the snapshot header
		header := &Snapshot{
			Height:       snap.Height,
			Format:       snap.Format,
			ChunkCount:   snap.ChunkCount,
			ChunkHashes:  make([][]byte, len(snap.ChunkHashes)),
			SnapshotHash: make([]byte, len(snap.SnapshotHash)),
			SnapshotSize: snap.SnapshotSize,
		}

		for j, hash := range snap.ChunkHashes {
			header.ChunkHashes[j] = make([]byte, len(hash))
			copy(header.ChunkHashes[j], hash)
		}
		copy(header.SnapshotHash, snap.SnapshotHash)
		snaps[counter] = header
		counter++
	}

	return snaps
}

// LoadSnapshotChunk loads a snapshot chunk at the given height and chunk index of given format.
// It returns the snapshot chunk as a byte slice of max size 16MB.
// returns an err if the snapshot chunk of given format at given height and chunk index does not exist.
func (s *SnapshotStore) LoadSnapshotChunk(height uint64, format uint32, chunk uint32) ([]byte, error) {
	// Check if snapshot exists
	s.snapshotsMtx.RLock()
	defer s.snapshotsMtx.RUnlock()

	header, ok := s.snapshots[height]
	if !ok {
		return nil, fmt.Errorf("snapshot at height %d does not exist", height)
	}

	// Check if chunk exists
	if chunk >= header.ChunkCount {
		return nil, fmt.Errorf("chunk %d does not exist in snapshot at height %d", chunk, height)
	}

	chunkFile := filepath.Join(snapshotChunkDir(s.cfg.SnapshotDir, height, format), fmt.Sprintf("chunk-%d", chunk))
	if _, err := os.Open(chunkFile); err != nil {
		return nil, fmt.Errorf("chunk %d does not exist in snapshot at height %d", chunk, height)
	}

	// Read the chunk file
	bts, err := os.ReadFile(chunkFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk %d at height %d: %w", chunk, height, err)
	}

	return bts, nil
}

func snapshotHeightDir(snapshotDir string, height uint64) string {
	return filepath.Join(snapshotDir, fmt.Sprintf("block-%d", height))
}

func snapshotFormatDir(snapshotDir string, height uint64, format uint32) string {
	return filepath.Join(snapshotHeightDir(snapshotDir, height), fmt.Sprintf("format-%d", format))
}

func snapshotChunkDir(snapshotDir string, height uint64, format uint32) string {
	return filepath.Join(snapshotFormatDir(snapshotDir, height, format), "chunks")
}

func snapshotChunkFile(snapshotDir string, height uint64, format uint32, chunkIdx uint32) string {
	return filepath.Join(snapshotChunkDir(snapshotDir, height, format), fmt.Sprintf("chunk-%d", chunkIdx))
}

func snapshotHeaderFile(snapshotDir string, height uint64, format uint32) string {
	return filepath.Join(snapshotFormatDir(snapshotDir, height, format), "header.json")
}
