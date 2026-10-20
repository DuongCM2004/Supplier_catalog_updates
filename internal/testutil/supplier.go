package testutil

import (
	"context"
	"fmt"
	"sync"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

// SupplierID is the supplier every test product belongs to.
const SupplierID = "SUP-001"

// Names recorded in the FakeSupplier call log.
const (
	OpGetUpdate  = "GetUpdate"
	OpFetchBatch = "FetchBatch"
)

// Call is one request seen by a FakeSupplier. Cursor is empty for GetUpdate.
type Call struct {
	Op     string
	Cursor string
}

// FakeSupplier is an in-memory supplier.Client whose failures are switched on by the test, so
// outage, timeout, correction and missing-page scenarios never depend on timing. All methods
// are safe for concurrent use.
type FakeSupplier struct {
	mu             sync.Mutex
	updates        map[string]supplier.UpdateInfo
	pages          map[string]supplier.Batch
	failFetch      map[string]int // remaining failures per cursor; negative = fail until reset to 0
	failUpdate     int
	updateNotFound bool
	notFound       map[string]bool
	blocked        map[string]chan struct{}
	calls          []Call
}

var _ supplier.Client = (*FakeSupplier)(nil)

// NewFakeSupplier returns a supplier with no updates and no pages.
func NewFakeSupplier() *FakeSupplier {
	return &FakeSupplier{
		updates:   map[string]supplier.UpdateInfo{},
		pages:     map[string]supplier.Batch{},
		failFetch: map[string]int{},
		notFound:  map[string]bool{},
		blocked:   map[string]chan struct{}{},
	}
}

// SetUpdate makes GetUpdate answer for info.UpdateID, replacing any earlier info.
func (f *FakeSupplier) SetUpdate(info supplier.UpdateInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates[info.UpdateID] = cloneInfo(info)
}

// AddPage serves b at cursor, replacing the page already there (this is how a test simulates
// the supplier correcting its data) and clearing a NotFound mark on that cursor. Pages are
// keyed by cursor alone, whatever update_id the batch carries, so a test can also serve a
// batch that names the wrong update.
func (f *FakeSupplier) AddPage(cursor string, b supplier.Batch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pages[cursor] = cloneBatch(b)
	delete(f.notFound, cursor)
}

// FailTimes makes the next n FetchBatch calls at cursor return supplier.ErrUnavailable.
// n < 0 fails every call until the test calls FailTimes(cursor, 0).
func (f *FakeSupplier) FailTimes(cursor string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failFetch[cursor] = n
}

// FailUpdateTimes makes the next n GetUpdate calls return supplier.ErrUnavailable.
func (f *FakeSupplier) FailUpdateTimes(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failUpdate = n
}

// UpdateNotFound makes every GetUpdate return supplier.ErrNotFound.
func (f *FakeSupplier) UpdateNotFound() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateNotFound = true
}

// NotFound makes FetchBatch at cursor return supplier.ErrNotFound even if a page exists,
// until AddPage serves that cursor again.
func (f *FakeSupplier) NotFound(cursor string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notFound[cursor] = true
}

// Block makes FetchBatch at cursor wait until its context ends (returning ctx.Err()) or
// Unblock is called. It models a supplier that hangs.
func (f *FakeSupplier) Block(cursor string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.blocked[cursor]; !ok {
		f.blocked[cursor] = make(chan struct{})
	}
}

// Unblock releases every FetchBatch waiting at cursor and stops blocking new calls there.
func (f *FakeSupplier) Unblock(cursor string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if gate, ok := f.blocked[cursor]; ok {
		close(gate)
		delete(f.blocked, cursor)
	}
}

// Calls returns a copy of the request log in arrival order, failed and blocked calls included.
func (f *FakeSupplier) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Call(nil), f.calls...)
}

// FetchCalls counts the FetchBatch requests made at cursor, retries included.
func (f *FakeSupplier) FetchCalls(cursor string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.Op == OpFetchBatch && c.Cursor == cursor {
			n++
		}
	}
	return n
}

// GetUpdate implements supplier.Client.
func (f *FakeSupplier) GetUpdate(ctx context.Context, updateID string) (supplier.UpdateInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, Call{Op: OpGetUpdate})
	if err := ctx.Err(); err != nil {
		return supplier.UpdateInfo{}, err
	}
	if f.failUpdate > 0 {
		f.failUpdate--
		return supplier.UpdateInfo{}, fmt.Errorf("fake supplier: update %q: %w", updateID, supplier.ErrUnavailable)
	}
	info, ok := f.updates[updateID]
	if f.updateNotFound || !ok {
		return supplier.UpdateInfo{}, fmt.Errorf("fake supplier: update %q: %w", updateID, supplier.ErrNotFound)
	}
	return cloneInfo(info), nil
}

// FetchBatch implements supplier.Client. Faults apply in this order: cancelled context,
// Block, FailTimes, NotFound, missing page.
func (f *FakeSupplier) FetchBatch(ctx context.Context, updateID, cursor string) (supplier.Batch, error) {
	f.mu.Lock()
	f.calls = append(f.calls, Call{Op: OpFetchBatch, Cursor: cursor})
	gate := f.blocked[cursor]
	f.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return supplier.Batch{}, err
	}
	if gate != nil {
		select {
		case <-ctx.Done():
			return supplier.Batch{}, ctx.Err()
		case <-gate:
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if n := f.failFetch[cursor]; n != 0 {
		if n > 0 {
			f.failFetch[cursor] = n - 1
		}
		return supplier.Batch{}, fmt.Errorf("fake supplier: cursor %q: %w", cursor, supplier.ErrUnavailable)
	}
	b, ok := f.pages[cursor]
	if f.notFound[cursor] || !ok {
		return supplier.Batch{}, fmt.Errorf("fake supplier: update %q cursor %q: %w", updateID, cursor, supplier.ErrNotFound)
	}
	return cloneBatch(b), nil
}

// Info builds the update description a supplier announces. totalBatches <= 0 means the
// supplier does not say how many batches to expect.
func Info(updateID string, totalBatches int) supplier.UpdateInfo {
	info := supplier.UpdateInfo{UpdateID: updateID, SupplierID: SupplierID}
	if totalBatches > 0 {
		info.TotalBatches = &totalBatches
	}
	return info
}

// Page builds one supplier batch with an explicit end-of-feed marker. A test that needs the
// marker missing sets the returned batch's HasMore to nil.
func Page(updateID, batchID, nextCursor string, hasMore bool, products ...catalog.SupplierProduct) supplier.Batch {
	return supplier.Batch{
		UpdateID:   updateID,
		BatchID:    batchID,
		NextCursor: nextCursor,
		HasMore:    &hasMore,
		Products:   append([]catalog.SupplierProduct{}, products...),
	}
}

// Active builds a valid ACTIVE product event priced in USD.
func Active(productID string, revision int64, name string, priceMinor int64) catalog.SupplierProduct {
	currency := "USD"
	return catalog.SupplierProduct{
		SupplierID: SupplierID,
		ProductID:  productID,
		Name:       &name,
		PriceMinor: &priceMinor,
		Currency:   &currency,
		Revision:   revision,
		Status:     catalog.StatusActive,
	}
}

// Withdrawn builds a withdrawal event, which carries only identity, revision and status.
func Withdrawn(productID string, revision int64) catalog.SupplierProduct {
	return catalog.SupplierProduct{
		SupplierID: SupplierID,
		ProductID:  productID,
		Revision:   revision,
		Status:     catalog.StatusWithdrawn,
	}
}

func cloneInfo(info supplier.UpdateInfo) supplier.UpdateInfo {
	info.TotalBatches = clonePtr(info.TotalBatches)
	return info
}

// cloneBatch copies every pointer and slice so neither the test that stored a batch nor the
// code that received one can change what the fake serves next.
func cloneBatch(b supplier.Batch) supplier.Batch {
	b.HasMore = clonePtr(b.HasMore)
	if b.Products != nil {
		products := make([]catalog.SupplierProduct, len(b.Products))
		for i, p := range b.Products {
			p.Name = clonePtr(p.Name)
			p.PriceMinor = clonePtr(p.PriceMinor)
			p.Currency = clonePtr(p.Currency)
			products[i] = p
		}
		b.Products = products
	}
	return b
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
