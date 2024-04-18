package snapshots

import (
	"encoding/json"
	"os"
)

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

func LoadSnapshot(file string) (*Snapshot, error) {
	bts, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var snapshot Snapshot
	if err := json.Unmarshal(bts, &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}
