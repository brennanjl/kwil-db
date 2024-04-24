package snapshot

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

var (
	cmdLongExplain = `Creates a snapshot of the database. The command is used during network migration to get the state of the KwilDB.
		It interacts directly with the Postgres server without intervening the kwild node.`
)

func createCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <db-name> <db-user> <db-host> <db-port> <snapshot-path>",
		Short: "Creates a snapshot of the database.",
		Long:  cmdLongExplain,
		Args:  cobra.ExactArgs(5),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return pgDump(ctx, args[0], args[1], args[2], args[3], args[4])
		},
	}

	return cmd
}

// pgDump tool directly requests Postgres DB to create logical dumps using pg_dump rather than interacting with the kwild.
// This function downloads the snapshot from Postgres DB, sanitizes it and compresses it.
func pgDump(ctx context.Context, dbName string, dbUser string, dbHost string, dbPort string, dumpFile string) error {

	if !strings.HasSuffix(dumpFile, ".gz") {
		dumpFile += ".gz"
	}

	outputFile, err := os.Create(dumpFile)
	if err != nil {
		return fmt.Errorf("failed to create dump file: %w", err)
	}
	defer outputFile.Close()

	pgDumpCmd := exec.CommandContext(ctx,
		"pg_dump",
		"--dbname", dbName,
		"--format", "plain",
		"--schema", "kwild_voting",
		"--schema", "kwild_chain",
		"--schema", "kwild_accts",
		"--schema", "kwild_internal",
		"--schema", "ds_*",
		"-T", "kwild_internal.sentry",
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
		"-U", dbUser,
		"-h", dbHost,
		"-p", dbPort,
	)

	sedCmd := exec.CommandContext(ctx, "sed", "-e", "/^--/d", "-e", "/^$/d", "-e", "/^SET/d", "-e", "/^SELECT/d")

	// Create a pipe between pg_dump and sed commands
	reader, writer := io.Pipe()
	pgDumpCmd.Stdout = writer
	sedCmd.Stdin = reader
	pgDumpCmd.Stderr = os.Stderr

	sedStdoutPipe, err := sedCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe from sed command: %w", err)
	}

	if err := sedCmd.Start(); err != nil {
		return fmt.Errorf("failed to start sed command: %w", err)
	}

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

	// Compress the sed output
	gzipWriter := gzip.NewWriter(outputFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, sedStdoutPipe)
	if err != nil {
		return fmt.Errorf("failed to write compressed dump file: %w", err)
	}

	if err := gzipWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	if err := sedCmd.Wait(); err != nil {
		return fmt.Errorf("failed to wait for sed command: %w", err)
	}

	fmt.Println("Snapshot created successfully at: ", dumpFile)

	return nil
}
