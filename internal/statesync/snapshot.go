package statesync

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/kwilteam/kwil-db/core/log"
	"github.com/kwilteam/kwil-db/internal/utils"
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

const (
	// chunkSize = 16 * 1024 * 1024 // 16MB
	chunkSize = 5 * 1024 // 10KB
)

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
	snapshots       map[uint64]*SnapshotHeader // Map of snapshot height to snapshot header
	snapshotHeights []uint64                   // List of snapshot heights
	snapshotsMtx    sync.RWMutex               // Protects access to snapshots and snapshotHeights

	log log.Logger
}

// SnapshotHeader is the header of a snapshot file representing the snapshot of the database at a certain height.
// It contains the height, format, chunk count, hash, size, and name of the snapshot.
type SnapshotHeader struct {
	Height       uint64   `json:"height"`
	Format       uint32   `json:"format"`
	ChunkHashes  [][]byte `json:"chunk_hashes"`
	ChunkCount   uint32   `json:"chunk_count"`
	SnapshotHash []byte   `json:"hash"`
	SnapshotSize uint64   `json:"size"`
}

func (s *SnapshotHeader) SaveAs(file string) error {
	bts, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(file, bts, 0644)
}

func LoadSnapshotHeader(file string) (*SnapshotHeader, error) {
	bts, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var header SnapshotHeader
	if err := json.Unmarshal(bts, &header); err != nil {
		return nil, err
	}
	return &header, nil
}

func NewSnapshotStore(cfg *SnapshotConfig, logger log.Logger) (*SnapshotStore, error) {
	ss := &SnapshotStore{
		cfg:             cfg,
		snapshots:       make(map[uint64]*SnapshotHeader),
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
		headerFile := SnapshotHeaderFile(cfg.SnapshotDir, heightInt, 0)
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
	if err := s.dbSnapshot(ctx, height, snapshotID); err != nil {
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

func (s *SnapshotStore) dbSnapshot(ctx context.Context, height uint64, snapshotID string) error {
	pgDumpCmd := exec.CommandContext(ctx,
		"pg_dump",
		// File format options
		"--dbname", "kwild",
		"--format", "plain",
		// List of schemas to include in the dump
		"--schema", "kwild_accts",
		"--schema", "kwild_voting",
		"--schema", "kwild_chain",
		"--schema", "ds_*",
		"--no-unlogged-table-data",
		"--no-comments",
		"--no-publications",
		"--no-unlogged-table-data",
		"--no-tablespaces",
		"--no-table-access-method",
		"--no-security-labels",
		"--no-subscriptions",
		"--large-objects",
		// Snapshot options
		"--snapshot", snapshotID, // Snapshot ID ensures a consistent snapshot taken at the given block boundary across all nodes
		// Connection options
		"-U", s.cfg.DbConfig.DBUser, // Is this needed?
		"-h", s.cfg.DbConfig.DBHost,
		"-p", s.cfg.DbConfig.DBPort,
	)

	fmt.Println("Executing snapshots:   ", pgDumpCmd.String())
	// Set up the sed command to remove comments, empty lines, and SET and SELECT statements
	sedCmd := exec.CommandContext(ctx, "sed", "-e", "/^--/d", "-e", "/^$/d", "-e", "/^SET/d", "-e", "/^SELECT/d")

	// Create a pipe between pg_dump and sed commands
	reader, writer := io.Pipe()
	pgDumpCmd.Stdout = writer
	sedCmd.Stdin = reader

	// Capture stdout and stderr from sed (the final output)
	sedStdoutPipe, err := sedCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe from sed command: %w", err)
	}
	sedStderrPipe, err := sedCmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe from sed command: %w", err)
	}

	// Start the sed command first
	if err := sedCmd.Start(); err != nil {
		return fmt.Errorf("failed to start sed command: %w", err)
	}

	// Then start the pg_dump command
	if err := pgDumpCmd.Start(); err != nil {
		return fmt.Errorf("failed to start pg_dump command: %w", err)
	}

	// Close the writer when pg_dump completes to signal EOF to sed
	go func() {
		if err := pgDumpCmd.Wait(); err != nil {
			fmt.Printf("pg_dump command failed: %v\n", err)
		}
		writer.Close()
	}()

	// Check for errors from sed (optional)
	if err, _ := io.ReadAll(sedStderrPipe); err != nil {
		fmt.Printf("Error from sed command: %s\n", err)
	}

	chunks := 1
	var fileSz uint64
	var hashes [][]byte
	hasher := sha256.New()

	// Default format 0 is gzip compressed plain SQL dump
	// Dir name
	snapshotHeightDir := snapshotHeightDir(s.cfg.SnapshotDir, height)
	snapshotDir := snapshotFormatDir(s.cfg.SnapshotDir, height, 0)
	chunkDir := snapshotChunkDir(s.cfg.SnapshotDir, height, 0)

	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory %s at height %d: %w", chunkDir, height, err)
	}

	// split the output into chunks of 16MB (Max chunks allowed by CometBFT)
	// https://docs.cometbft.com/v0.38/spec/p2p/legacy-docs/messages/state-sync#chunkresponse
	var chunkErr error
	for {
		outputFile := snapshotChunkFile(s.cfg.SnapshotDir, height, 0, uint32(chunks-1))
		chunkFile, err := os.Create(outputFile)
		if err != nil {
			chunkErr = fmt.Errorf("failed to create chunk file %s at height %d: %w", outputFile, height, err)
			break
		}

		// copy 16MB of data from the stdout pipe to the chunk file
		// copyN copies chunks of 32KB from the stdout pipe to the chunk file until EOF or 16MB is reached
		n, err := io.CopyN(chunkFile, sedStdoutPipe, chunkSize)
		if err != nil && err != io.EOF {
			chunkErr = fmt.Errorf("failed to copy chunk %d at height %d: %w", chunks, height, err)
			break
		}
		fileSz += uint64(n)
		hash, err := utils.HashFile(outputFile)
		if err != nil {
			return fmt.Errorf("failed to hash chunk %d at height %d: %w", chunks, height, err)
		}
		hashes = append(hashes, hash)
		hasher.Write(hash)

		if n < chunkSize {
			// Last chunk
			break
		}
		chunks++
	}

	if chunkErr != nil {
		deleteSnapshotDir(snapshotHeightDir)
		return chunkErr
	}

	if err := sedCmd.Wait(); err != nil {
		deleteSnapshotDir(snapshotHeightDir)
		return fmt.Errorf("pg_dump command failed: %w", err)
	}

	// Create the snapshot header file

	header := &SnapshotHeader{
		Height:       height,
		Format:       0, // Standard gzip compressed plain SQL dump
		ChunkCount:   uint32(chunks),
		ChunkHashes:  hashes,
		SnapshotHash: hasher.Sum(nil), // Calculate hash of the snapshot
		SnapshotSize: fileSz,          // Calculate size of the snapshot
	}
	if err := header.SaveAs(filepath.Join(snapshotDir, "header.json")); err != nil {
		deleteSnapshotDir(snapshotHeightDir)
		return fmt.Errorf("failed to save snapshot header at height %d: %w", height, err)
	}

	// Add the snapshot to the list of snapshots
	s.snapshotsMtx.Lock()
	s.snapshots[height] = header
	s.snapshotHeights = append(s.snapshotHeights, height)
	s.snapshotsMtx.Unlock()

	s.log.Info("Snapshot created successfully", zap.Int64("height", int64(height)), zap.String("snapshotID", snapshotID))
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
func (s *SnapshotStore) ListSnapshots() ([]*SnapshotHeader, error) {
	// Make copy of snapshots
	s.snapshotsMtx.RLock()
	defer s.snapshotsMtx.RUnlock()

	snaps := make([]*SnapshotHeader, len(s.snapshots))
	counter := 0
	for _, snap := range s.snapshots {
		// Deep copy the snapshot header
		header := &SnapshotHeader{
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

	return snaps, nil
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

func SnapshotHeaderFile(snapshotDir string, height uint64, format uint32) string {
	return filepath.Join(snapshotFormatDir(snapshotDir, height, format), "header.json")
}
