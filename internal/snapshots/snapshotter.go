package snapshots

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kwilteam/kwil-db/core/log"
	"github.com/kwilteam/kwil-db/internal/utils"
	"go.uber.org/zap"
)

const (
	chunkSize int64 = 16 * 1024 * 1024 // 16MB

	DefaultSnapshotFormat = 0

	stage1output = "stage1output.sql"
	stage2output = "stage2output.sql"
	stage3output = "stage3output.sql.gz"
)

// This file deals with creating a snapshot instance at a given snapshotID
// The whole process occurs in multiple stages:
// STAGE1: Dumping the database state using pg_dump and writing it to a file
// STAGE2: Sanitizing the dump file to make it deterministic
//   - Removing white spaces, comments and SET and SELECT statements
//   - Sorting the COPY blocks of data based on the hash of the row-data
//
// STAGE3: Compressing the sanitized dump file
// STAGE4: Splitting the compressed dump file into chunks of fixed size (16MB)
// TODO: STAGE2 could be optimized by sorting based on the first column,
// but it might not work if the first column is not unique.

type Snapshotter struct {
	dbConfig    *DBConfig
	snapshotDir string
	log         log.Logger
}

func NewSnapshotter(cfg *DBConfig, dir string, logger log.Logger) *Snapshotter {
	return &Snapshotter{
		dbConfig:    cfg,
		snapshotDir: dir,
		log:         logger,
	}
}

// CreateSnapshot creates a snapshot at the given height and snapshotID
func (s *Snapshotter) CreateSnapshot(ctx context.Context, height uint64, snapshotID string) (*Snapshot, error) {
	// create snapshot directory
	snapshotDir := SnapshotHeightDir(s.snapshotDir, height)
	chunkDir := SnapshotChunkDir(s.snapshotDir, height, DefaultSnapshotFormat)
	err := os.MkdirAll(chunkDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot chunk dir: %w", err)
	}

	// Stage1: Dump the database at the given height and snapshot ID
	err = s.dbSnapshot(ctx, height, DefaultSnapshotFormat, snapshotID)
	if err != nil {
		deleteSnapshotDir(snapshotDir)
		return nil, fmt.Errorf("failed to create snapshot at height %d: %w", height, err)
	}

	// Stage2: Sanitize the dump
	hash, err := s.sanitizeDump(height, DefaultSnapshotFormat)
	if err != nil {
		deleteSnapshotDir(snapshotDir)
		return nil, fmt.Errorf("failed to sanitize snapshot at height %d: %w", height, err)
	}

	// Stage3: Compress the dump
	err = s.compressDump(height, DefaultSnapshotFormat)
	if err != nil {
		deleteSnapshotDir(snapshotDir)
		return nil, fmt.Errorf("failed to compress snapshot at height %d: %w", height, err)
	}

	// Stage4: Split the dump into chunks
	snapshot, err := s.splitDumpIntoChunks(height, DefaultSnapshotFormat, hash)
	if err != nil {
		deleteSnapshotDir(snapshotDir)
		return nil, fmt.Errorf("failed to split snapshot into chunks at height %d: %w", height, err)
	}

	return snapshot, nil
}

// dbSnapshot is the STAGE1 of the snapshot creation process
// It uses pg_dump to dump the database state at the given height and snapshotID
// The pg dump is stored as "/stage1output.sql" in the snapshot directory
// This is a temporary file and will be removed after the snapshot is created
func (s *Snapshotter) dbSnapshot(ctx context.Context, height uint64, format uint32, snapshotID string) error {
	snapshotDir := SnapshotFormatDir(s.snapshotDir, height, format)
	dumpFile := filepath.Join(snapshotDir, stage1output)

	pgDumpCmd := exec.CommandContext(ctx,
		"pg_dump",
		// File format options
		"--dbname", "kwild",
		"--file", dumpFile,
		"--format", "plain",
		// List of schemas to include in the dump
		"--schema", "kwild_voting",
		"--schema", "kwild_chain",
		"--schema", "kwild_accts",
		"--schema", "kwild_internal",
		"--schema", "ds_*",
		// Tables to exclude from the dump
		"-T", "kwild_internal.sentry",
		// other sql dump specific options
		"--no-unlogged-table-data",
		"--no-comments",
		"--create",
		"--no-publications",
		"--no-unlogged-table-data",
		"--no-tablespaces",
		"--no-table-access-method",
		"--no-security-labels",
		"--no-subscriptions",
		"--large-objects",
		// Snapshot ID ensures a consistent snapshot taken at the given block boundary across all nodes
		"--snapshot", snapshotID,
		// Connection options
		"-U", s.dbConfig.DBUser,
		"-h", s.dbConfig.DBHost,
		"-p", s.dbConfig.DBPort,
	)

	s.log.Debug("Executing pg_dump", zap.String("cmd", pgDumpCmd.String()))

	output, err := pgDumpCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to execute pg_dump: %w, output: %s", err, output)
	}

	s.log.Info("pg_dump successful", zap.Uint64("height", height))

	return nil
}

type HashedLine struct {
	Hash   []byte
	offset int64
}

// sanitizeDump is the STAGE2 of the snapshot creation process
// This stage sanitizes the dump file to make it deterministic across all the nodes
// It removes white spaces, comments, SET and SELECT statements
// It sorts the COPY blocks of data based on the hash of the row-data
// The sanitized dump is stored as "/stage2output.sql" in the snapshot directory
// This is a temporary file and will be removed after the snapshot is created
func (s *Snapshotter) sanitizeDump(height uint64, format uint32) ([]byte, error) {
	// check if the stage1output file exists
	snapshotDir := SnapshotFormatDir(s.snapshotDir, height, format)
	dumpFile := filepath.Join(snapshotDir, stage1output)
	dumpInst1, err := os.Open(dumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open dump file: %w", err)
	}
	defer dumpInst1.Close()

	dumpInst2, err := os.Open(dumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open dump file: %w", err)
	}
	defer dumpInst2.Close()

	// sanitized dump file
	sanitizedDumpFile := filepath.Join(snapshotDir, stage2output)
	outputFile, err := os.Create(sanitizedDumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create sanitized dump file: %w", err)
	}
	defer outputFile.Close()

	// Scanner to read the dump file line by line
	scanner := bufio.NewScanner(dumpInst1)
	inCopyBlock := false
	lineHashes := make([]HashedLine, 0)
	offset := int64(0)

	for scanner.Scan() {
		line := scanner.Text()
		numBytes := int64(len(line)) + 1 // +1 for newline character

		if inCopyBlock {
			/*
				COPY schema.table (id, name) FROM stdin;
				2 entry2
				1 entry1
				3 entry3
				\.
			*/
			if line == "\\." { // end of COPY block
				inCopyBlock = false

				// Inline sort the lineHashes array based on the row hash
				sort.Slice(lineHashes, func(i, j int) bool {
					return bytes.Compare(lineHashes[i].Hash, lineHashes[j].Hash) < 0
				})

				/*
					TODO: Consider optimizing the sorting process:
					- #len(lineHashes) number of fseeks per copy block -> not good, saves memory but huge IO overhead
					- Preferably, sort based on the first column or a unique column for data locality
				*/

				// Write the sorted data to the output file based on the offset
				for _, hashedLine := range lineHashes {
					// Seek to the offset of the line in the input file
					_, err := dumpInst2.Seek(hashedLine.offset, 0)
					if err != nil {
						return nil, fmt.Errorf("failed to seek to offset: %w", err)
					}

					lineBytes, err := bufio.NewReader(dumpInst2).ReadBytes('\n')
					if err != nil {
						return nil, fmt.Errorf("failed to read line from input file: %w", err)
					}

					_, err = outputFile.Write(lineBytes)
					if err != nil {
						return nil, fmt.Errorf("failed to write to sanitized dump file: %w", err)
					}
				}

				// Write the end of COPY block to the output file
				_, err = outputFile.WriteString(line + "\n")
				if err != nil {
					return nil, fmt.Errorf("failed to write to sanitized dump file: %w", err)
				}

				// Clear the lineHashes array
				lineHashes = make([]HashedLine, 0)
				offset += numBytes // +1 for newline character
			} else {
				// If we are in a COPY block, we need to sort the data based on the row hash
				hasher := sha256.New()
				hasher.Write([]byte(line))
				lineHashes = append(lineHashes, HashedLine{Hash: hasher.Sum(nil), offset: offset})
				offset += numBytes // +1 for newline character
			}
		} else {
			offset += int64(len(line)) + 1 // +1 for newline character
			if line == "" {
				// skip empty lines
				continue
			} else if strings.HasPrefix(line, "--") {
				// skip comments
				continue
			} else if strings.HasPrefix(line, "SET") || strings.HasPrefix(line, "SELECT") {
				// skip SET and SELECT statements
				continue
			} else {
				if strings.HasPrefix(line, "COPY") && strings.Contains(line, "FROM stdin;") {
					// start of COPY block
					inCopyBlock = true
				}
				// write the line to the output file
				_, err := outputFile.WriteString(line + "\n")
				if err != nil {
					return nil, fmt.Errorf("failed to write to sanitized dump file: %w", err)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan the dump file: %w", err)
	}

	// remove the dump file
	err = os.Remove(dumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to remove dump file: %w", err)
	}

	hash, err := utils.HashFile(sanitizedDumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to hash sanitized dump file: %w", err)
	}

	s.log.Info("Sanitized dump file", zap.Uint64("height", height), zap.String("dumpFile hash", fmt.Sprintf("%x", hash)))

	return hash, nil
}

// CompressDump is the STAGE3 of the snapshot creation process
// This method compresses the sanitized dump file using gzip compression
// Should we do inline compression? or using exec.Command?
func (s *Snapshotter) compressDump(height uint64, format uint32) error {
	// Check if the dump file exists
	snapshotDir := SnapshotFormatDir(s.snapshotDir, height, format)
	dumpFile := filepath.Join(snapshotDir, stage2output)
	inputFile, err := os.Open(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to open dump file: %w", err)
	}
	defer inputFile.Close()

	// dump file stats
	stats, err := os.Stat(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	compressedFile := filepath.Join(snapshotDir, stage3output)
	outputFile, err := os.Create(compressedFile)
	if err != nil {
		return fmt.Errorf("failed to create compressed dump file: %w", err)
	}
	defer outputFile.Close()

	// gzip writer
	// TODO: Should we use gzip.NewWriterLevel() to set the compression level?
	gzipWriter := gzip.NewWriter(outputFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, inputFile)
	if err != nil {
		return fmt.Errorf("failed to copy data to compressed dump file: %w", err)
	}

	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	compressedStats, err := os.Stat(compressedFile)
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	// Remove the sanitized dump file
	err = os.Remove(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to remove dump file: %w", err)
	}

	s.log.Info("Dump file compressed", zap.Uint64("height", height), zap.Uint64("Uncompressed dump size", uint64(stats.Size())), zap.Uint64("Compressed dump size", uint64(compressedStats.Size())))

	return nil
}

// SplitDumpIntoChunks is the STAGE4 of the snapshot creation process
// This method splits the compressed dump file into chunks of fixed size (16MB)
// The chunks are stored in the height/format/chunks directory
// The snapshot header is created and stored in the height/format/header.json file
func (s *Snapshotter) splitDumpIntoChunks(height uint64, format uint32, sqlDumpHash []byte) (*Snapshot, error) {
	// check if the dump file exists
	snapshotDir := SnapshotFormatDir(s.snapshotDir, height, format)
	dumpFile := filepath.Join(snapshotDir, stage3output)
	inputFile, err := os.Open(dumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open dump file: %w", err)
	}
	defer inputFile.Close()

	// split the dump file into chunks

	// split the dump file into chunks
	chunkIndex := uint32(0)
	hashes := make([][]byte, 0)
	hasher := sha256.New()
	fileSize := uint64(0)

	for {
		chunkFileName := SnapshotChunkFile(s.snapshotDir, height, format, chunkIndex)
		chunkFile, err := os.Create(chunkFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk file: %w", err)
		}
		defer chunkFile.Close()

		// write the chunk to the file
		written, err := io.CopyN(chunkFile, inputFile, chunkSize)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to write chunk to file: %w", err)
		}

		// calculate the hash of the chunk
		hasher.Reset()
		hash, err := utils.HashFile(chunkFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to hash chunk file: %w", err)
		}
		hashes = append(hashes, hash)
		fileSize += uint64(written)
		chunkIndex++

		s.log.Info("Chunk created", zap.Uint32("index", chunkIndex), zap.String("chunkFile", chunkFileName), zap.Int64("size", written))

		if err == io.EOF || written < chunkSize {
			break // EOF, Last chunk
		}

	}

	// get file size
	fileInfo, err := os.Stat(dumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}
	if fileSize != uint64(fileInfo.Size()) {
		return nil, fmt.Errorf("file size mismatch: %d != %d", fileSize, fileInfo.Size())
	}

	snapshot := &Snapshot{
		Height:       height,
		Format:       format,
		ChunkCount:   chunkIndex,
		ChunkHashes:  hashes,
		SnapshotHash: sqlDumpHash,
		SnapshotSize: fileSize,
	}
	headerFile := SnapshotHeaderFile(s.snapshotDir, height, format)
	err = snapshot.SaveAs(headerFile)
	if err != nil {
		return nil, fmt.Errorf("failed to save snapshot header: %w", err)
	}

	// remove the compressed dump file
	err = os.Remove(dumpFile)
	if err != nil {
		return nil, fmt.Errorf("failed to remove dump file: %w", err)
	}

	s.log.Info("Chunk files created successfully", zap.Uint64("height", height), zap.Uint32("chunkCount", chunkIndex), zap.Uint64("Total Snapzhot Size", fileSize))

	return snapshot, nil
}
