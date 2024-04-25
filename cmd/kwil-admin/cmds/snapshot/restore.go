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
	restoreLongExplain = `Creates a snapshot of the database. The command is used during network migration to get the state of the KwilDB.
		It interacts directly with the Postgres server without intervening the kwild node.`
)

func restoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore <db-user> <db-host> <db-port> <snapshot-file>",
		Short: "Restore the db state from the snapshot file.",
		Long:  restoreLongExplain,
		Args:  cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return restore(ctx, args[0], args[1], args[2], args[3])
		},
	}

	return cmd
}

func restore(ctx context.Context, dbUser string, dbHost string, dbPort string, snapshotFile string) error {
	// Check if the snapshot file exists, if not return error
	snapfs, err := os.Open(snapshotFile)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer snapfs.Close()

	psqlCmd := exec.CommandContext(ctx,
		"psql",
		"--username", dbUser,
		"--host", dbHost,
		"--port", dbPort,
		"--dbname", "postgres",
	)
	psqlCmd.Stderr = os.Stderr

	stdinPipe, err := psqlCmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdin pipe: %w", err)
	}
	defer stdinPipe.Close()

	if err := psqlCmd.Start(); err != nil {
		return fmt.Errorf("failed to start psql command: %w", err)
	}

	// Check if the snapshot file is compressed, if yes decompress it
	var reader io.Reader
	if strings.HasSuffix(snapshotFile, ".gz") {
		// Decompress the snapshot file
		gzipReader, err := gzip.NewReader(snapfs)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = snapfs
	}

	// Copy data to stdin of the psql command
	if _, err := io.Copy(stdinPipe, reader); err != nil {
		return fmt.Errorf("failed to copy snapshot file to psql: %w", err)
	}
	stdinPipe.Close() // Ensure closure of the pipe

	if err := psqlCmd.Wait(); err != nil {
		return fmt.Errorf("psql command failed: %w", err)
	}

	fmt.Println("DB restored successfully")
	return nil
}
