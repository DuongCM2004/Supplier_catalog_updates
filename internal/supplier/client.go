// Package supplier defines how the sync engine talks to a supplier feed and provides a
// fixture-backed client with deterministic fault switches for demos and tests.
package supplier

import (
	"context"
	"errors"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
)

var (
	// ErrUnavailable is transient: the caller retries with backoff.
	ErrUnavailable = errors.New("supplier unavailable")
	// ErrNotFound means the update or cursor is unknown; retrying cannot help.
	ErrNotFound = errors.New("supplier: not found")
	// ErrMalformed means the payload could not be decoded; retrying cannot help.
	ErrMalformed = errors.New("supplier: malformed response")
)

// UpdateInfo describes one supplier update before its pages are fetched.
type UpdateInfo struct {
	UpdateID     string `json:"update_id"`
	SupplierID   string `json:"supplier_id"`
	TotalBatches *int   `json:"total_batches,omitempty"` // nil = supplier did not say
}

// Batch is one page of an update. HasMore is a pointer so a missing end-of-feed marker is
// detectable: a page without it can never be taken as the last page.
type Batch struct {
	UpdateID   string                    `json:"update_id"`
	BatchID    string                    `json:"batch_id"`
	NextCursor string                    `json:"next_cursor"`
	HasMore    *bool                     `json:"has_more"`
	Products   []catalog.SupplierProduct `json:"products"`
}

// Client fetches supplier updates. Cursor "" is the first page.
type Client interface {
	GetUpdate(ctx context.Context, updateID string) (UpdateInfo, error)
	FetchBatch(ctx context.Context, updateID, cursor string) (Batch, error)
}
