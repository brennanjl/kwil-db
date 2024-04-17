package statesync

import (
	"fmt"

	abciTypes "github.com/cometbft/cometbft/abci/types"
)

var (
	ErrStateSyncInProgress    = fmt.Errorf("statesync already in progress")
	ErrStateSyncNotInProgress = fmt.Errorf("statesync not in progress")

	ErrAbortSnapshot             = fmt.Errorf("abort snapshot")
	ErrRejectSnapshot            = fmt.Errorf("reject snapshot")
	ErrUnsupportedSnapshotFormat = fmt.Errorf("unsupported snapshot format")
	ErrInvalidSnapshot           = fmt.Errorf("invalid snapshot")

	ErrAbortSnapshotChunk  = fmt.Errorf("abort snapshot chunk")
	ErrRetrySnapshotChunk  = fmt.Errorf("retry snapshot chunk") // retries without refetching the chunk
	ErrRetrySnapshot       = fmt.Errorf("retry snapshot")
	ErrRejectSnapshotChunk = fmt.Errorf("reject snapshot chunk")
)

func ToABCIOfferSnapshotResponse(err error) abciTypes.ResponseOfferSnapshot_Result {
	switch err {
	case nil:
		return abciTypes.ResponseOfferSnapshot_ACCEPT
	case ErrAbortSnapshot:
		return abciTypes.ResponseOfferSnapshot_ABORT
	case ErrRejectSnapshot:
		return abciTypes.ResponseOfferSnapshot_REJECT
	case ErrUnsupportedSnapshotFormat:
		return abciTypes.ResponseOfferSnapshot_REJECT_FORMAT
	default:
		return abciTypes.ResponseOfferSnapshot_UNKNOWN
	}
}

func ToABCIApplySnapshotChunkResponse(err error) abciTypes.ResponseApplySnapshotChunk_Result {
	switch err {
	case nil:
		return abciTypes.ResponseApplySnapshotChunk_ACCEPT
	case ErrAbortSnapshotChunk:
		return abciTypes.ResponseApplySnapshotChunk_ABORT
	case ErrRetrySnapshotChunk:
		return abciTypes.ResponseApplySnapshotChunk_RETRY
	case ErrRejectSnapshotChunk:
		return abciTypes.ResponseApplySnapshotChunk_REJECT_SNAPSHOT
	default:
		return abciTypes.ResponseApplySnapshotChunk_UNKNOWN
	}
}
