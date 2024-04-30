package snapshot

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	ctx := context.Background()
	path := "~/Desktop/kwil/dev/statesync/kwild-snaps/"
	path, err := expandPath(path)
	require.NoError(t, err)

	err = pgDumpRetry(ctx, "kwild", "kwild", "kwild", "localhost", "5434", path)
	require.NoError(t, err)
}
