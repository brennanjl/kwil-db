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

	restoreExample = `# Restore the database from the compressed snapshot file
kwil-admin snapshot restore kwild localhost 5432 /path/to/snapshot.sql.gz
DB restored successfully

# Restore the database from the uncompressed snapshot file
kwil-admin snapshot restore kwild localhost 5432 /path/to/snapshot.sql
DB restored successfully

# Database restore will fail if there are existing connections to the database
kwil-admin snapshot restore kwild localhost 5432 /path/to/snapshot.sql
ERROR:  database "kwild" is being accessed by other users

Ensure that there are no active connections to the database before restoring the snapshot.`
)

func restoreCmd() *cobra.Command {
	var snapshotFile, dbUser, dbPass, dbHost, dbPort string
	cmd := &cobra.Command{
		Use:     "restore <db-user> <db-host> <db-port> <snapshot-file>",
		Short:   "Restore the db state from the snapshot file.",
		Long:    restoreLongExplain,
		Example: restoreExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			return restore(cmd.Context(), dbUser, dbPass, dbHost, dbPort, snapshotFile)
		},
	}
	cmd.Flags().StringVar(&snapshotFile, "snapshot-file", "", "Snapshot file to restore the database from.")
	cmd.Flags().StringVar(&dbUser, "user", "postgres", "User with administrative privileges")
	cmd.Flags().StringVar(&dbPass, "pass", "", "Password for the database user")
	cmd.Flags().StringVar(&dbHost, "host", "localhost", "Host of the database")
	cmd.Flags().StringVar(&dbPort, "port", "", "Port of the database")
	return cmd
}

func restore(ctx context.Context, dbUser string, dbPass string, dbHost string, dbPort string, snapshotFile string) error {
	// Check if the snapshot file exists, if not return error
	if snapshotFile == "" {
		return fmt.Errorf("snapshot file not provided")
	}

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
		"--no-password",
	)
	psqlCmd.Stderr = os.Stderr

	if dbPass != "" {
		psqlCmd.Env = append(os.Environ(), "PGPASSWORD="+dbPass)
	}

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
