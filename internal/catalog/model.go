// Package catalog holds the supplier product model, the pure revision rules that decide
// what an incoming product event may do to the stored catalog, and the repository that
// applies those decisions under row locks.
package catalog

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Status is the lifecycle state of a catalog product. WITHDRAWN is terminal: a withdrawn
// product is kept as a tombstone and can never become ACTIVE again.
type Status string

const (
	StatusActive    Status = "ACTIVE"
	StatusWithdrawn Status = "WITHDRAWN"
)

// SupplierProduct is one product event exactly as the supplier sent it.
type SupplierProduct struct {
	SupplierID string  `json:"supplier_id"`
	ProductID  string  `json:"product_id"`
	Name       *string `json:"name,omitempty"`
	PriceMinor *int64  `json:"price_minor,omitempty"`
	Currency   *string `json:"currency,omitempty"`
	Revision   int64   `json:"revision"`
	Status     Status  `json:"status"`
}

// Product is the stored catalog row, tombstones included.
type Product struct {
	SupplierID, ProductID string
	Status                Status
	Revision              int64
	Name                  *string
	PriceMinor            *int64
	Currency              *string
	PayloadHash           string
	CreatedAt, UpdatedAt  time.Time
	WithdrawnAt           *time.Time
}

// Decision is what one incoming event does to the stored row.
type Decision string

const (
	DecisionCreate              Decision = "CREATE"
	DecisionCreateTombstone     Decision = "CREATE_TOMBSTONE"
	DecisionUpdate              Decision = "UPDATE"
	DecisionWithdraw            Decision = "WITHDRAW"
	DecisionAdvanceTombstone    Decision = "ADVANCE_TOMBSTONE"
	DecisionIgnoreStale         Decision = "IGNORE_STALE"
	DecisionIgnoreDuplicate     Decision = "IGNORE_DUPLICATE"
	DecisionVersionConflict     Decision = "VERSION_CONFLICT"     // blocks the whole batch
	DecisionReactivationBlocked Decision = "REACTIVATION_BLOCKED" // blocks the whole batch
)

// Applied is the outcome for one incoming event, in application order.
type Applied struct {
	Event    SupplierProduct // canonicalized
	Decision Decision
	Before   *Product // nil when the row was absent
	After    *Product // nil when nothing was written
}

// BlockingError aborts the whole batch: the caller rolls the transaction back and records
// an issue instead, so a batch is either applied completely or not at all.
type BlockingError struct {
	Code                  string // "VERSION_CONFLICT" | "REACTIVATION_BLOCKED"
	SupplierID, ProductID string
	Revision              int64
	Message               string
}

func (e *BlockingError) Error() string {
	return fmt.Sprintf("%s: %s/%s revision %d: %s", e.Code, e.SupplierID, e.ProductID, e.Revision, e.Message)
}

// ValidationError lists every invalid record of a batch, found before any database work.
type ValidationError struct {
	Problems []string // e.g. "SKU-302: price_minor must be >= 0"
}

func (e *ValidationError) Error() string {
	return "invalid product data: " + strings.Join(e.Problems, "; ")
}

var ErrNotFound = errors.New("product not found")

// ListFilter narrows product listings.
type ListFilter struct {
	SupplierID    string // "" = all suppliers
	Status        string // "ACTIVE" (default) | "WITHDRAWN" | "ALL"
	Limit, Offset int
}

// Rules and repository (rules.go, repository.go):
//
//	func ValidateBatch(events []SupplierProduct, supplierID string) error // *ValidationError or nil
//	func Canonicalize(p SupplierProduct) SupplierProduct
//	func PayloadHash(p SupplierProduct) string // hex sha256 of canonical JSON
//	func Decide(current *Product, in SupplierProduct) Decision // pure; in must be canonical
//	func ApplyBatch(ctx context.Context, tx pgx.Tx, events []SupplierProduct) ([]Applied, error) // *BlockingError possible
//	func ListProducts(ctx context.Context, db storage.DB, f ListFilter) ([]Product, error)
//	func GetProduct(ctx context.Context, db storage.DB, supplierID, productID string) (Product, error) // ErrNotFound
