package snapshot

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kwilteam/kwil-db/cmd/kwild/config"

	"github.com/spf13/cobra"
)

var (
	createLongExplain = `This command creates a logical SQL dump of the specified database and it's snapshot hash in the directory specified. This command is intended for generating exportable snapshots that are not integrated into the node's snapshot store on the running KwilDB instance. Instead, they are meant to be used for initializing a new network, serving as the genesis state. 
	
	This command interacts directly with the Kwild's PostgreSQL server, bypassing any interactions with the kwild node. It requires user with administrative privileges on the database to ensure that it can access all necessary data and perform required actions such as locking tables etc. without restrictions that are beyond the permissions of a standard database user.`

	createExample = `# Create snapshot and snapshot hash of the database
kwil-admin snapshot create kwil-db kwil_user localhost 5432 /path/to/snapshot/dir

# Snapshot and hash files will be created in the snapshot directory
ls /path/to/snapshot/dir
kwildb-snapshot-hash    kwildb-snapshot.sql.gz`

	createDatabase = `CREATE DATABASE `
)

/*
Use this at the beginning of the sql dump file to drop any active connections on the 'kwild' database.
Not a good idea to use this if the Kwild node is connected to this database.

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'kwild'
  AND pid <> pg_backend_pid();
*/

func createCmd() *cobra.Command {
	var snapshotDir, dbName, dbUser, dbPass, dbHost, dbPort string
	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Creates a snapshot of the database.",
		Long:    createLongExplain,
		Example: createExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			snapshotDir, err := expandPath(snapshotDir)
			if err != nil {
				return fmt.Errorf("failed to expand snapshot directory path: %w", err)
			}

			return pgDump(cmd.Context(), dbName, dbUser, dbPass, dbHost, dbPort, snapshotDir)
		},
	}

	cmd.Flags().StringVar(&snapshotDir, "snapdir", "kwild-snaps", "Directory to store the snapshot and hash files")
	cmd.Flags().StringVar(&dbName, "dbname", "kwild", "Name of the database to snapshot")
	cmd.Flags().StringVar(&dbUser, "user", "postgres", "User with administrative privileges on the database")
	cmd.Flags().StringVar(&dbPass, "pass", "", "Password for the database user")
	cmd.Flags().StringVar(&dbHost, "host", "localhost", "Host of the database")
	cmd.Flags().StringVar(&dbPort, "port", "", "Port of the database")

	return cmd
}

func expandPath(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, path[2:])
	}
	return filepath.Abs(path)
}

func pgDump(ctx context.Context, dbName, dbUser, dbPass, dbHost, dbPort, snapshotDir string) error {
	// Check if the snapshot directory exists, if not create it
	err := os.MkdirAll(snapshotDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	dumpFile := filepath.Join(snapshotDir, "kwildb-snapshot.sql.gz")
	outputFile, err := os.Create(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to create dump file: %w", err)
	}
	defer outputFile.Close()

	gzipWriter := gzip.NewWriter(outputFile)
	defer gzipWriter.Close()

	pgDumpCmd := exec.CommandContext(ctx,
		"pg_dump",
		"--dbname", dbName,
		"--format", "plain",
		// Voting Schema (remove the COPY command from voters table during sanitization)
		"--schema", "kwild_voting", // Include only the processed table

		// Account Schema
		"--schema", "kwild_accts",

		// Internal Schema
		"--schema", "kwild_internal",
		"-T", "kwild_internal.sentry", // Exclude sentry table (no versioning)

		// User Schemas
		"--schema", "ds_*",

		// kwild_chain is not included in this snapshot, as this is used for genesis state

		// Other options
		"--no-unlogged-table-data",
		"--no-comments",
		"--create",
		// "--clean", // drops database first before adding commands to create it
		// "--if-exists",
		"--no-publications",
		"--no-unlogged-table-data",
		"--no-tablespaces",
		"--no-table-access-method",
		"--no-security-labels",
		"--no-subscriptions",
		"--large-objects",
		"--no-owner", // Do not include ownership information, to restore on any user
		"--username", dbUser,
		"--host", dbHost,
		"--port", dbPort,
		"--no-password",
	)

	if dbPass != "" {
		pgDumpCmd.Env = append(os.Environ(), "PGPASSWORD="+dbPass)
	}

	var stderr bytes.Buffer
	pgDumpOutput, err := pgDumpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}
	pgDumpCmd.Stderr = &stderr

	if err := pgDumpCmd.Start(); err != nil {
		return fmt.Errorf("failed to start pg_dump command: %w", err)
	}
	defer pgDumpOutput.Close()

	hasher := sha256.New()
	var inVotersBlock bool
	var validatorCount int64
	genCfg := config.DefaultGenesisConfig()
	genCfg.Alloc = make(map[string]*big.Int)
	multiWriter := io.MultiWriter(gzipWriter, hasher)
	var totalBytes int64

	// Pass the output of pg_dump through scanner to sanitize it
	scanner := bufio.NewScanner(pgDumpOutput)
	for scanner.Scan() {
		line := scanner.Text()

		// Remove whitespaces, set and select statements, process voters table
		if inVotersBlock {
			// Example voter: \\xdae5e91f74b95a9db05fc0f1f8c07f95	\\x9e52ff636caf4988e72e4ac865e6ef83a1e262d1a6376a300f3db8884e1f2253	1

			if line == "\\." { // End of voters block
				inVotersBlock = false
				n, err := multiWriter.Write([]byte(line + "\n"))
				if err != nil {
					return fmt.Errorf("failed to write to gzip writer: %w", err)
				}
				totalBytes += int64(n)
				continue
			}

			strs := strings.Split(line, "\t")
			if len(strs) != 3 {
				return fmt.Errorf("invalid voter line: %s", line)
			}
			voterID, err := hex.DecodeString(strs[1][3:]) // Remove the leading \\x
			if err != nil {
				return fmt.Errorf("failed to decode voter ID: %w", err)
			}

			power, err := strconv.ParseInt(strs[2], 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse power: %w", err)
			}

			genCfg.Validators = append(genCfg.Validators, &config.GenesisValidator{
				PubKey: voterID,
				Power:  power,
				Name:   fmt.Sprintf("validator-%d", validatorCount),
			})
			validatorCount++
		} else {
			if line == "" || strings.TrimSpace(line) == "" { // Skip empty lines
				continue
			} else if strings.HasPrefix(line, "--") { // Skip comments
				continue
			} else if strings.HasPrefix(line, "SET") || strings.HasPrefix(line, "SELECT") || strings.HasPrefix(line[1:], "connect") {
				// Skip SET and SELECT and connect statements
				continue
			} else if strings.HasPrefix(line, createDatabase) {
				// Skip CREATE DATABASE statement
			} else {
				if strings.HasPrefix(line, "COPY kwild_voting.voters") && strings.Contains(line, "FROM stdin;") {
					inVotersBlock = true
				}

				// Write the sanitized line to the gzip writer
				n, err := multiWriter.Write([]byte(line + "\n"))
				if err != nil {
					return fmt.Errorf("failed to write to gzip writer: %w", err)
				}
				totalBytes += int64(n)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan pg_dump output: %w", err)
	}

	// Close the writer when pg_dump completes to signal EOF to sed
	if err := pgDumpCmd.Wait(); err != nil {
		return fmt.Errorf("pg_dump failed with error: %s, err: %w", stderr.String(), err)
	}

	gzipWriter.Flush()
	hash := hasher.Sum(nil)
	genCfg.DataAppHash = hash

	// Write the genesis config to a file
	genesisFile := filepath.Join(snapshotDir, "genesis.json")
	if err := genCfg.SaveAs(genesisFile); err != nil {
		return fmt.Errorf("failed to save genesis config: %w", err)
	}

	fmt.Println("Snapshot created at: ", dumpFile, " Total bytes written: ", totalBytes)
	fmt.Println("Genesis config created at: ", genesisFile, " Genesis hash: ", fmt.Sprintf("%x", hash))
	return nil
}
