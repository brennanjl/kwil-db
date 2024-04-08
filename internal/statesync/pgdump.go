package statesync

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
	"time"

	"github.com/kwilteam/kwil-db/internal/utils"
	"go.uber.org/zap"
)

const (
	// chunkSize int64 = 16 * 1024 * 1024 // 16MB
	chunkSize int64 = 1 * 1024 // 5KB
)

// This file deals with dumping the database state and santizing it to make it determinisitc for statesync
// The sanitization process involves removing all the non-deterministic data from the database state, ensuring ordering.
// The whole process occurs in multiple stages:
// STAGE1: Dumping the database state using pg_dump and writing it to a file
// STAGE2: Sanitizing the dump file to make it deterministic
//         - Removing white spaces, comments and SET and SELECT statements
//         - Sorting the COPY blocks of data
// STAGE3: Compressing the sanitized dump file
// STAGE4: Splitting the compressed dump file into chunks of fixed size (16MB)

func (s *SnapshotStore) dbSnapshot(ctx context.Context, height uint64, format uint32, snapshotID string) (string, error) {
	// start time
	startTime := time.Now()

	dir := snapshotFormatDir(s.cfg.SnapshotDir, height, format)
	err := utils.CreateDirIfNeeded(dir)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot dir: %w", err)
	}

	snapshotFile := filepath.Join(dir, "stage1output.sql")
	pgDumpCmd := exec.CommandContext(ctx,
		"pg_dump",
		// File format options
		"--dbname", "kwild",
		"--file", snapshotFile,
		"--format", "plain",
		// "--compress", "gzip:9", // TODO: Which compression level is more efficient?
		// List of schemas to include in the dump
		"--schema", "kwild_voting",
		"--schema", "kwild_chain",
		"--schema", "kwild_accts",
		"--schema", "ds_*",
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
		// Snapshot options
		"--snapshot", snapshotID, // Snapshot ID ensures a consistent snapshot taken at the given block boundary across all nodes
		// Connection options
		"-U", s.cfg.DbConfig.DBUser, // Is this needed?
		"-h", s.cfg.DbConfig.DBHost,
		"-p", s.cfg.DbConfig.DBPort,
	)

	s.log.Debug("Executing pg_dump", zap.String("cmd", pgDumpCmd.String()))

	output, err := pgDumpCmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to execute pg_dump: %w, output: %s", err, output)
	}

	// end time
	endTime := time.Now()
	s.log.Info("Snapshot created", zap.Uint64("height", height), zap.String("snapshotFile", snapshotFile), zap.Duration("duration", endTime.Sub(startTime)))

	return snapshotFile, nil
}

type HashedLine struct {
	Hash   []byte
	offset int64
}

// sanitizeDump sanitizes the dump file to make it deterministic
// TODO: What if sorting is done based on the first column? or maybe based on the primary key?
// Just sorting based on the first column is quite easy but might not work if its not unique
func (s *SnapshotStore) sanitizeDump(height uint64, format uint32, dumpFile string) (string, error) {
	// start time
	startTime := time.Now()

	// check if the dump file exists
	inputFile, err := os.Open(dumpFile)
	if err != nil {
		return "", fmt.Errorf("failed to open dump file: %w", err)
	}
	defer inputFile.Close()
	pgDumpFile, err := os.Open(dumpFile)
	if err != nil {
		return "", fmt.Errorf("failed to open dump file: %w", err)
	}

	// sanitized dump file
	dir := snapshotFormatDir(s.cfg.SnapshotDir, height, format)
	sanitizedDumpFile := filepath.Join(dir, "stage2output.sql")
	outputFile, err := os.Create(sanitizedDumpFile)
	if err != nil {
		return "", fmt.Errorf("failed to create sanitized dump file: %w", err)
	}
	defer outputFile.Close()

	// Scanner to read the dump file line by line
	scanner := bufio.NewScanner(inputFile)
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

				// Inline sort the lineHashes array based on the hash
				sort.Slice(lineHashes, func(i, j int) bool {
					return bytes.Compare(lineHashes[i].Hash, lineHashes[j].Hash) < 0
				})

				/*
					TODO: Issues to fix
					- #len(lineHashes) number of fseeks per copy block -> not good, saves memory but huge IO overhead
					- Scanner buffer size is 64KB, it can throw an error if the line is bigger than that
				*/

				// Write the sorted data to the output file based on the offset
				for _, hashedLine := range lineHashes {
					// Seek to the offset of the line in the input file
					_, err := pgDumpFile.Seek(hashedLine.offset, 0)
					if err != nil {
						return "", fmt.Errorf("failed to seek to offset: %w", err)
					}

					// Read the line from the input file
					lineBytes, err := bufio.NewReader(pgDumpFile).ReadBytes('\n')
					if err != nil {
						return "", fmt.Errorf("failed to read line from input file: %w", err)
					}

					// Write the line to the output file
					_, err = outputFile.Write(lineBytes)
					if err != nil {
						return "", fmt.Errorf("failed to write to sanitized dump file: %w", err)
					}
				}

				// Write the end of COPY block to the output file
				_, err = outputFile.WriteString(line + "\n")
				if err != nil {
					return "", fmt.Errorf("failed to write to sanitized dump file: %w", err)
				}

				// Clear the lineHashes array
				lineHashes = make([]HashedLine, 0)
				offset += numBytes // +1 for newline character

				// Reset the input file offset to the end of the COPY block?
				// _, err = inputFile.Seek(offset, 0)
				// if err != nil {
				// 	return fmt.Errorf("failed to reset the file to the correct offset: %w", err)
				// }

			} else {
				// If we are in a COPY block, we need to sort the data
				hasher := sha256.New()
				hasher.Write([]byte(line))
				lineHashes = append(lineHashes, HashedLine{Hash: hasher.Sum(nil), offset: offset}) // TODO: line vs line offset
				offset += int64(numBytes)                                                          // +1 for newline character
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
					return "", fmt.Errorf("failed to write to sanitized dump file: %w", err)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("failed to scan the dump file: %w", err)
	}

	// remove the dump file
	err = os.Remove(dumpFile)
	if err != nil {
		return "", fmt.Errorf("failed to remove dump file: %w", err)
	}

	// end time
	endTime := time.Now()
	s.log.Info("Dump sanitized", zap.Uint64("height", height), zap.String("dumpFile", dumpFile), zap.Duration("duration", endTime.Sub(startTime)))

	return sanitizedDumpFile, nil
}

// CompressDump compresses the sanitized dump file using gzip compression
// TODO: How do we decide on the compression level to use?
// Should we do inline compression? or using exec.Command?
func (s *SnapshotStore) compressDump(ctx context.Context, height uint64, format uint32, dumpFile string, inMemory bool) (string, error) {
	// time
	startTime := time.Now()

	outputFileName := filepath.Join(snapshotFormatDir(s.cfg.SnapshotDir, height, format), "stage3output.sql.gz")

	// file size:
	stats, err := os.Stat(dumpFile)
	if err != nil {
		return "", fmt.Errorf("failed to get file stats: %w", err)
	}

	if inMemory {
		inputFile, err := os.Open(dumpFile)
		if err != nil {
			return "", fmt.Errorf("failed to open dump file: %w", err)
		}
		defer inputFile.Close()

		outputFile, err := os.Create(outputFileName)
		if err != nil {
			return "", fmt.Errorf("failed to create compressed dump file: %w", err)
		}
		defer outputFile.Close()

		gzipWriter := gzip.NewWriter(outputFile)
		defer gzipWriter.Close()

		_, err = io.Copy(gzipWriter, inputFile)
		if err != nil {
			return "", fmt.Errorf("failed to copy data to compressed dump file: %w", err)
		}

		if err := gzipWriter.Close(); err != nil {
			return "", fmt.Errorf("failed to close gzip writer: %w", err)
		}

	} else {
		compressCmd := exec.CommandContext(ctx, "gzip", "-9", dumpFile, ">", outputFileName)
		s.log.Debug("Executing gzip", zap.String("cmd", compressCmd.String()))
		out, err := compressCmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("failed to execute gzip: %w, output: %s", err, out)
		}
	}

	s.log.Info("Dump compressed", zap.Uint64("height", height), zap.String("dumpFile", dumpFile), zap.String("outputFile", outputFileName))

	compressedStats, err := os.Stat(outputFileName)
	if err != nil {
		return "", fmt.Errorf("failed to get file stats: %w", err)
	}

	// end time
	endTime := time.Now()

	s.log.Info("Dump compressed", zap.Uint64("height", height), zap.Uint64("Plain dump size", uint64(stats.Size())), zap.Uint64("Compressed dump size", uint64(compressedStats.Size())), zap.Duration("duration", endTime.Sub(startTime)))

	return outputFileName, nil
}

// SplitDump splits the compressed dump file into chunks of fixed size (16MB)
func (s *SnapshotStore) splitDump(height uint64, format uint32, dumpFile string) error {
	// start time
	startTime := time.Now()

	// check if the dump file exists
	inputFile, err := os.Open(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to open dump file: %w", err)
	}
	defer inputFile.Close()

	// split the dump file into chunks
	chunkDir := snapshotChunkDir(s.cfg.SnapshotDir, height, format)
	err = utils.CreateDirIfNeeded(chunkDir)
	if err != nil {
		return fmt.Errorf("failed to create chunk dir: %w", err)
	}

	// split the dump file into chunks
	chunkIndex := uint32(0)
	hashes := make([][]byte, 0)
	hasher := sha256.New()
	fileSize := uint64(0)

	for {
		chunkFileName := filepath.Join(chunkDir, fmt.Sprintf("chunk-%d", chunkIndex))
		chunkFile, err := os.Create(chunkFileName)
		if err != nil {
			return fmt.Errorf("failed to create chunk file: %w", err)
		}
		defer chunkFile.Close()

		// write the chunk to the file
		written, err := io.CopyN(chunkFile, inputFile, chunkSize)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to write chunk to file: %w", err)
		}

		// calculate the hash of the chunk
		hasher.Reset()
		hash, err := utils.HashFile(chunkFileName)
		if err != nil {
			return fmt.Errorf("failed to hash chunk file: %w", err)
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
		return fmt.Errorf("failed to get file stats: %w", err)
	}
	if fileSize != uint64(fileInfo.Size()) {
		return fmt.Errorf("file size mismatch: %d != %d", fileSize, fileInfo.Size())
	}

	hash, err := utils.HashFile(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to hash dump file: %w", err)
	}

	header := &SnapshotHeader{
		Height:       height,
		Format:       format,
		ChunkCount:   chunkIndex,
		ChunkHashes:  hashes,
		SnapshotHash: hash,
		SnapshotSize: fileSize,
	}
	headerFile := SnapshotHeaderFile(s.cfg.SnapshotDir, height, format)
	err = header.SaveAs(headerFile)
	if err != nil {
		return fmt.Errorf("failed to save snapshot header: %w", err)
	}

	s.snapshotsMtx.Lock()
	s.snapshots[height] = header
	s.snapshotHeights = append(s.snapshotHeights, height)
	s.snapshotsMtx.Unlock()

	// end time
	endTime := time.Now()
	s.log.Info("Dump split", zap.Uint64("height", height), zap.String("dumpFile", dumpFile), zap.Duration("duration", endTime.Sub(startTime)))

	return nil
}
