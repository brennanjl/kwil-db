package snapshots

import (
	"context"
	"os"
	"testing"

	"github.com/kwilteam/kwil-db/core/log"
	"github.com/stretchr/testify/require"
)

/*
    ListSnapshots

	CreateSnapshot
		- Snapshotter can be mocked -> add a new folder under temp dir,
	RegisterSnapshot
*/

type MockSnapshotter struct {
	snapshotDir string
}

func NewMockSnapshotter(dir string) *MockSnapshotter {
	return &MockSnapshotter{snapshotDir: dir}
}

func (m *MockSnapshotter) CreateSnapshot(ctx context.Context, height uint64, snapshotID string) (*Snapshot, error) {
	data := []byte(snapshotID)

	snapshot := &Snapshot{
		Height:       height,
		Format:       0,
		ChunkCount:   1,
		ChunkHashes:  [][]byte{data},
		SnapshotHash: data,
		SnapshotSize: uint64(len(data)),
	}

	// create the snapshot directory
	chunkDir := SnapshotChunkDir(m.snapshotDir, height, 0)
	err := os.MkdirAll(chunkDir, 0755)
	if err != nil {
		return nil, err
	}

	headerFile := SnapshotHeaderFile(m.snapshotDir, height, 0)
	err = snapshot.SaveAs(headerFile)
	if err != nil {
		return nil, err
	}

	chunkFile := SnapshotChunkFile(m.snapshotDir, height, 0, 0)
	file, err := os.Create(chunkFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

func TestCreateSnapshots(t *testing.T) {
	dir := t.TempDir()
	snapshotter := NewMockSnapshotter(dir)
	logger := log.NewStdOut(log.DebugLevel)

	cfg := &SnapshotConfig{
		RecurringHeight: 1,
		SnapshotDir:     dir,
		MaxSnapshots:    2,
	}
	store, err := NewSnapshotStore(cfg, snapshotter, logger)
	require.NoError(t, err)

	ctx := context.Background()

	height := uint64(1)

	// Check if the snapshot is due
	require.True(t, store.IsSnapshotDue(height))

	// Create a snapshot
	err = store.CreateSnapshot(ctx, height, "snapshot1")
	require.NoError(t, err)

	// List snapshots
	snaps := store.ListSnapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, height, snaps[0].Height)
	require.Equal(t, uint32(1), snaps[0].ChunkCount)

	// Create 2nd snapshot
	height = 2
	err = store.CreateSnapshot(ctx, height, "snapshot2")
	require.NoError(t, err)

	// List snapshots
	snaps = store.ListSnapshots()
	require.Len(t, snaps, 2)

	// Create 3rd snapshot, should purge the snapshot with height 1
	height = 3
	err = store.CreateSnapshot(ctx, height, "snapshot3")
	require.NoError(t, err)

	// List snapshots
	snaps = store.ListSnapshots()
	require.Len(t, snaps, 2)
	for _, snap := range snaps {
		require.NotEqual(t, uint64(1), snap.Height)
	}
}

func TestRegisterSnapshot(t *testing.T) {
	dir := t.TempDir()
	snapshotter := NewMockSnapshotter(dir)
	logger := log.NewStdOut(log.DebugLevel)

	cfg := &SnapshotConfig{
		RecurringHeight: 1,
		SnapshotDir:     dir,
		MaxSnapshots:    2,
	}
	store, err := NewSnapshotStore(cfg, snapshotter, logger)
	require.NoError(t, err)

	ctx := context.Background()

	var snapshot *Snapshot
	// register a snapshot that doesn't exist or nil
	err = store.RegisterSnapshot(snapshot)
	require.NoError(t, err)

	// List snapshots
	snaps := store.ListSnapshots()
	require.Len(t, snaps, 0)

	// Create a snapshot at height 1 through the snapshotter
	height := uint64(1)
	snapshot, err = snapshotter.CreateSnapshot(ctx, height, "snapshot1")
	require.NoError(t, err)

	// Register the snapshot
	err = store.RegisterSnapshot(snapshot)
	require.NoError(t, err)

	// Create another snapshot at height 1
	snapshot2, err := snapshotter.CreateSnapshot(ctx, height, "snapshot1-2")
	require.NoError(t, err)

	// Register the snapshot, as snapshot already exists at the height, its a no-op
	err = store.RegisterSnapshot(snapshot2)
	require.NoError(t, err)

	// List snapshots
	snaps = store.ListSnapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, height, snaps[0].Height)
	require.Equal(t, []byte("snapshot1"), snaps[0].SnapshotHash)

	// Create a snapshot at height 2
	height = 2
	snapshot, err = snapshotter.CreateSnapshot(ctx, height, "snapshot2")
	require.NoError(t, err)

	// Register the snapshot
	err = store.RegisterSnapshot(snapshot)
	require.NoError(t, err)

	// List snapshots
	snaps = store.ListSnapshots()
	require.Len(t, snaps, 2)

	// Create a snapshot at height 3
	height = 3
	snapshot, err = snapshotter.CreateSnapshot(ctx, height, "snapshot3")
	require.NoError(t, err)

	// Register the snapshot
	err = store.RegisterSnapshot(snapshot)
	require.NoError(t, err)

	// List snapshots
	snaps = store.ListSnapshots()
	require.Len(t, snaps, 2)
	for _, snap := range snaps {
		require.NotEqual(t, uint64(1), snap.Height)
	}
}

func TestLoadSnapshotChunk(t *testing.T) {
	dir := t.TempDir()
	snapshotter := NewMockSnapshotter(dir)
	logger := log.NewStdOut(log.DebugLevel)

	cfg := &SnapshotConfig{
		RecurringHeight: 1,
		SnapshotDir:     dir,
		MaxSnapshots:    2,
	}
	store, err := NewSnapshotStore(cfg, snapshotter, logger)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a snapshot at height 1
	height := uint64(1)
	snapshot, err := snapshotter.CreateSnapshot(ctx, height, "snapshot1")
	require.NoError(t, err)

	// Register the snapshot
	err = store.RegisterSnapshot(snapshot)
	require.NoError(t, err)

	// Load the snapshot chunk
	data, err := store.LoadSnapshotChunk(height, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("snapshot1"), data)

	// Load the snapshot chunk that doesn't exist
	data, err = store.LoadSnapshotChunk(height, 0, 1)
	require.Error(t, err)
	require.Nil(t, data)

	// Load the snapshot chunk of unsupported format
	data, err = store.LoadSnapshotChunk(height, 1, 0)
	require.Error(t, err)
	require.Nil(t, data)

	// Load the snapshot chunk that doesn't exist at a given height
	height = 2
	data, err = store.LoadSnapshotChunk(height, 0, 0)
	require.Error(t, err)
	require.Nil(t, data)

}
