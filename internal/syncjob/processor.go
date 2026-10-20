package syncjob

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

// validateEnvelope checks everything about a batch that can be judged without the database:
// that it belongs to the job's update, names itself, carries the end-of-feed marker and has
// a cursor that fits that marker. requestedCursor is the cursor the batch was fetched with.
// A nil result means the envelope is sound; otherwise the BLOCKING issue to record.
//
// A batch that points back at the cursor it was fetched with would be fetched again
// forever, so that is rejected here rather than discovered by looping.
func validateEnvelope(job Job, requestedCursor string, b supplier.Batch) *Issue {
	invalid := func(code IssueCode, format string, args ...any) *Issue {
		return &Issue{Code: code, Severity: SeverityBlocking, Message: fmt.Sprintf(format, args...)}
	}
	switch {
	case b.UpdateID != job.UpdateID:
		return invalid(CodeInvalidBatch, "batch belongs to update %.80q, but this job syncs %q", b.UpdateID, job.UpdateID)
	case b.BatchID == "":
		return invalid(CodeInvalidBatch, "batch has no batch_id")
	case b.HasMore == nil:
		return invalid(CodeInvalidBatch, "batch %.80q has no has_more marker, so the end of the feed cannot be told", b.BatchID)
	case *b.HasMore && b.NextCursor == "":
		return invalid(CodeInvalidCursor, "batch %.80q says has_more but gives no next_cursor", b.BatchID)
	case *b.HasMore && b.NextCursor == requestedCursor:
		return invalid(CodeInvalidCursor, "batch %.80q points back at its own cursor %.80q", b.BatchID, requestedCursor)
	case !*b.HasMore && b.NextCursor != "":
		return invalid(CodeInvalidCursor, "batch %.80q ends the feed but still gives next_cursor %.80q", b.BatchID, b.NextCursor)
	}
	return nil
}

// hashedBatch fixes the field order of the hashed JSON, which is what makes BatchHash
// deterministic.
type hashedBatch struct {
	UpdateID   string                    `json:"update_id"`
	BatchID    string                    `json:"batch_id"`
	NextCursor string                    `json:"next_cursor"`
	HasMore    *bool                     `json:"has_more"`
	Products   []catalog.SupplierProduct `json:"products"`
}

// BatchHash identifies the content of a batch: the hex SHA-256 of its envelope and its
// products in canonical form, in the order the supplier listed them. Re-serving a batch
// with different whitespace or key order gives the same hash; changing any field of any
// product gives a different one. The hash is how a batch_id that comes back with other
// content is told apart from a harmless re-delivery.
func BatchHash(b supplier.Batch) string {
	products := make([]catalog.SupplierProduct, len(b.Products)) // [] rather than null for an empty batch
	for i, p := range b.Products {
		products[i] = catalog.Canonicalize(p)
	}
	// Marshalling cannot fail: the value holds only strings, integers and booleans.
	raw, _ := json.Marshal(hashedBatch{
		UpdateID:   b.UpdateID,
		BatchID:    b.BatchID,
		NextCursor: b.NextCursor,
		HasMore:    b.HasMore,
		Products:   products,
	})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
