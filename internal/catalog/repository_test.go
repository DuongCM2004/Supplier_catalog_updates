package catalog_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

const supplier = "SUP-001"

func ptr[T any](v T) *T { return &v }

func active(productID string, revision int64, name string, priceMinor int64) catalog.SupplierProduct {
	return catalog.SupplierProduct{
		SupplierID: supplier, ProductID: productID, Revision: revision, Status: catalog.StatusActive,
		Name: ptr(name), PriceMinor: ptr(priceMinor), Currency: ptr("USD"),
	}
}

func withdrawn(productID string, revision int64) catalog.SupplierProduct {
	return catalog.SupplierProduct{SupplierID: supplier, ProductID: productID, Revision: revision, Status: catalog.StatusWithdrawn}
}

// newPool returns the shared test pool with every table emptied.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	return pool
}

// apply runs ApplyBatch in its own transaction, committing on success and rolling back on
// any error, as the batch processor does.
func apply(pool *pgxpool.Pool, events ...catalog.SupplierProduct) ([]catalog.Applied, error) {
	var out []catalog.Applied
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		var err error
		out, err = catalog.ApplyBatch(context.Background(), tx, events)
		return err
	})
	return out, err
}

func mustApply(t *testing.T, pool *pgxpool.Pool, events ...catalog.SupplierProduct) []catalog.Applied {
	t.Helper()
	out, err := apply(pool, events...)
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	return out
}

func decisions(applied []catalog.Applied) []catalog.Decision {
	var out []catalog.Decision
	for _, a := range applied {
		out = append(out, a.Decision)
	}
	return out
}

// row reads a product with plain SQL, independently of the repository functions under test.
func row(t *testing.T, pool *pgxpool.Pool, productID string) catalog.Product {
	t.Helper()
	var p catalog.Product
	err := pool.QueryRow(context.Background(),
		`SELECT supplier_id, product_id, status, revision, name, price_minor, currency, payload_hash, created_at, updated_at, withdrawn_at
		 FROM products WHERE supplier_id = $1 AND product_id = $2`, supplier, productID).
		Scan(&p.SupplierID, &p.ProductID, &p.Status, &p.Revision, &p.Name, &p.PriceMinor, &p.Currency, &p.PayloadHash, &p.CreatedAt, &p.UpdatedAt, &p.WithdrawnAt)
	if err != nil {
		t.Fatalf("read product %s: %v", productID, err)
	}
	return p
}

func countProducts(t *testing.T, db storage.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM products`).Scan(&n); err != nil {
		t.Fatalf("count products: %v", err)
	}
	return n
}

func wantDecisions(t *testing.T, applied []catalog.Applied, want ...catalog.Decision) {
	t.Helper()
	if got := decisions(applied); !slices.Equal(got, want) {
		t.Fatalf("decisions = %v, want %v", got, want)
	}
}

func TestApplyBatchPersistsEveryWriteDecision(t *testing.T) {
	pool := newPool(t)

	// First batch: a new product, a new withdrawal of a product never seen, and a product
	// that is created now and withdrawn in the next batch.
	first := mustApply(t, pool, active("A", 1, "Widget", 1000), withdrawn("B", 4), active("C", 1, "Gadget", 500))
	wantDecisions(t, first, catalog.DecisionCreate, catalog.DecisionCreateTombstone, catalog.DecisionCreate)
	for _, a := range first {
		if a.Before != nil || a.After == nil {
			t.Fatalf("%s: before=%v after=%v, want nil/non-nil", a.Event.ProductID, a.Before, a.After)
		}
	}
	a1, b1, c1 := row(t, pool, "A"), row(t, pool, "B"), row(t, pool, "C")
	if a1.Status != catalog.StatusActive || a1.Revision != 1 || *a1.Name != "Widget" || *a1.PriceMinor != 1000 || *a1.Currency != "USD" || a1.WithdrawnAt != nil {
		t.Fatalf("created row = %+v", a1)
	}
	if want := catalog.PayloadHash(active("A", 1, "Widget", 1000)); a1.PayloadHash != want {
		t.Fatalf("payload_hash = %s, want %s", a1.PayloadHash, want)
	}

	// Second batch, a separate transaction so updated_at can move: update, withdraw an
	// active product, and advance the tombstone.
	second := mustApply(t, pool, active("A", 2, "Widget v2", 1100), withdrawn("C", 2), withdrawn("B", 6))
	wantDecisions(t, second, catalog.DecisionUpdate, catalog.DecisionAdvanceTombstone, catalog.DecisionWithdraw)

	a2 := row(t, pool, "A")
	if a2.Revision != 2 || *a2.Name != "Widget v2" || *a2.PriceMinor != 1100 || a2.Status != catalog.StatusActive {
		t.Fatalf("updated row = %+v", a2)
	}
	if !a2.UpdatedAt.After(a1.UpdatedAt) || !a2.CreatedAt.Equal(a1.CreatedAt) {
		t.Fatalf("updated_at %v -> %v, created_at %v -> %v", a1.UpdatedAt, a2.UpdatedAt, a1.CreatedAt, a2.CreatedAt)
	}

	c2 := row(t, pool, "C")
	if c2.Status != catalog.StatusWithdrawn || c2.Revision != 2 || c2.WithdrawnAt == nil {
		t.Fatalf("withdrawn row = %+v", c2)
	}
	if c2.Name == nil || *c2.Name != "Gadget" || *c2.PriceMinor != 500 {
		t.Fatalf("withdrawal dropped the last known details: %+v", c2)
	}
	if !c2.UpdatedAt.After(c1.UpdatedAt) {
		t.Fatal("withdrawal did not move updated_at")
	}

	b2 := row(t, pool, "B")
	if b2.Status != catalog.StatusWithdrawn || b2.Revision != 6 || b2.Name != nil {
		t.Fatalf("advanced tombstone = %+v", b2)
	}
	if !b2.WithdrawnAt.Equal(*b1.WithdrawnAt) || !b2.UpdatedAt.After(b1.UpdatedAt) {
		t.Fatalf("tombstone withdrawn_at %v -> %v, updated_at %v -> %v", b1.WithdrawnAt, b2.WithdrawnAt, b1.UpdatedAt, b2.UpdatedAt)
	}
	if b2.PayloadHash != catalog.PayloadHash(withdrawn("B", 6)) {
		t.Fatal("advanced tombstone kept an old payload_hash")
	}

	// Applied carries the row before and after each write.
	byID := map[string]catalog.Applied{}
	for _, a := range second {
		byID[a.Event.ProductID] = a
	}
	if u := byID["A"]; u.Before.Revision != 1 || u.After.Revision != 2 {
		t.Fatalf("update before/after revisions = %d/%d", u.Before.Revision, u.After.Revision)
	}
}

func TestWithdrawingAnUnseenProductStoresATombstoneWithoutDetails(t *testing.T) {
	pool := newPool(t)

	mustApply(t, pool, withdrawn("GHOST", 4))

	p := row(t, pool, "GHOST")
	if p.Status != catalog.StatusWithdrawn || p.Revision != 4 || p.WithdrawnAt == nil {
		t.Fatalf("tombstone = %+v", p)
	}
	if p.Name != nil || p.PriceMinor != nil || p.Currency != nil {
		t.Fatalf("tombstone has details: %+v", p)
	}
}

// The details a withdrawal carries are dropped before the database sees them, so even text
// PostgreSQL cannot store (a NUL byte) does not fail the batch.
func TestWithdrawalDetailsNeverReachTheDatabase(t *testing.T) {
	pool := newPool(t)
	w := withdrawn("GHOST", 4)
	w.Name, w.Currency = ptr("Wid\x00get"), ptr("\xff")

	if err := catalog.ValidateBatch([]catalog.SupplierProduct{w}, supplier); err != nil {
		t.Fatalf("ValidateBatch: %v", err)
	}
	mustApply(t, pool, w)

	if p := row(t, pool, "GHOST"); p.Name != nil || p.Currency != nil {
		t.Fatalf("tombstone kept withdrawal details: %+v", p)
	}
}

func TestOutOfOrderRevisionsInOneBatchEndAtTheHighest(t *testing.T) {
	pool := newPool(t)

	applied := mustApply(t, pool, active("A", 5, "five", 5), active("A", 4, "four", 4))

	wantDecisions(t, applied, catalog.DecisionCreate, catalog.DecisionUpdate)
	if applied[0].Event.Revision != 4 || applied[1].Event.Revision != 5 {
		t.Fatalf("applied revisions %d then %d, want 4 then 5", applied[0].Event.Revision, applied[1].Event.Revision)
	}
	if applied[1].Before == nil || applied[1].Before.Revision != 4 {
		t.Fatalf("second write should see the first in the same batch: %+v", applied[1].Before)
	}
	if p := row(t, pool, "A"); p.Revision != 5 || *p.Name != "five" {
		t.Fatalf("stored = revision %d name %q, want 5 / five", p.Revision, *p.Name)
	}
}

func TestStaleAndDuplicateEventsChangeNothing(t *testing.T) {
	pool := newPool(t)
	mustApply(t, pool, active("A", 3, "Widget", 100), withdrawn("W", 2))
	before := row(t, pool, "A")
	beforeW := row(t, pool, "W")

	applied := mustApply(t, pool,
		active("A", 2, "older", 1),    // stale
		active("A", 3, "Widget", 100), // duplicate
		active("W", 1, "ancient", 1),  // stale against the tombstone
		withdrawn("W", 2),             // duplicate withdrawal
	)

	wantDecisions(t, applied, catalog.DecisionIgnoreStale, catalog.DecisionIgnoreDuplicate, catalog.DecisionIgnoreStale, catalog.DecisionIgnoreDuplicate)
	for _, a := range applied {
		if a.After != nil {
			t.Fatalf("%s %s wrote a row", a.Event.ProductID, a.Decision)
		}
	}
	if got := row(t, pool, "A"); !got.UpdatedAt.Equal(before.UpdatedAt) || got.Revision != 3 || *got.Name != "Widget" {
		t.Fatalf("ignored events changed A: %+v", got)
	}
	if got := row(t, pool, "W"); !got.UpdatedAt.Equal(beforeW.UpdatedAt) || got.Status != catalog.StatusWithdrawn {
		t.Fatalf("ignored events changed W: %+v", got)
	}
}

func TestRepeatedEventInsideOneBatchIsADuplicate(t *testing.T) {
	pool := newPool(t)

	applied := mustApply(t, pool, active("A", 1, "Widget", 100), active("A", 1, "Widget", 100))

	wantDecisions(t, applied, catalog.DecisionCreate, catalog.DecisionIgnoreDuplicate)
	if n := countProducts(t, pool); n != 1 {
		t.Fatalf("products = %d, want 1", n)
	}
}

func TestStoredValuesAreCanonical(t *testing.T) {
	pool := newPool(t)
	messy := active("A", 1, "  Blue Widget ", 100)
	messy.Currency = ptr(" usd")

	mustApply(t, pool, messy)

	p := row(t, pool, "A")
	if *p.Name != "Blue Widget" || *p.Currency != "USD" {
		t.Fatalf("stored name/currency = %q/%q", *p.Name, *p.Currency)
	}
	// The same event again, spelled differently, is recognised as a duplicate.
	again := mustApply(t, pool, active("A", 1, "Blue Widget", 100))
	wantDecisions(t, again, catalog.DecisionIgnoreDuplicate)
}

func TestEmptyBatchChangesNothing(t *testing.T) {
	pool := newPool(t)

	applied := mustApply(t, pool)

	if len(applied) != 0 || countProducts(t, pool) != 0 {
		t.Fatalf("empty batch produced %d results", len(applied))
	}
}

func TestConflictingEventAbortsBatchAndRollbackDiscardsEarlierWrites(t *testing.T) {
	pool := newPool(t)
	mustApply(t, pool, active("B", 2, "Gadget", 200))

	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		_, err := catalog.ApplyBatch(context.Background(), tx, []catalog.SupplierProduct{
			active("A", 1, "Widget", 100), // applies fine, sorted before B
			active("B", 2, "Gadget", 999), // same revision, different price
		})
		if err == nil {
			t.Error("conflicting batch was accepted")
			return nil
		}
		// The transaction is still usable and still holds the first write; rolling it
		// back is the caller's job.
		if n := countProducts(t, tx); n != 2 {
			t.Errorf("inside the open transaction products = %d, want 2", n)
		}
		return err
	})

	var blocking *catalog.BlockingError
	if !errors.As(err, &blocking) {
		t.Fatalf("want *BlockingError, got %v", err)
	}
	if blocking.Code != "VERSION_CONFLICT" || blocking.SupplierID != supplier || blocking.ProductID != "B" || blocking.Revision != 2 {
		t.Fatalf("blocking error = %+v", blocking)
	}
	for _, part := range []string{"stored ACTIVE revision 2", "incoming ACTIVE revision 2"} {
		if !strings.Contains(blocking.Message, part) {
			t.Fatalf("message %q lacks %q", blocking.Message, part)
		}
	}
	if n := countProducts(t, pool); n != 1 {
		t.Fatalf("after rollback products = %d, want only the original B", n)
	}
	if p := row(t, pool, "B"); *p.PriceMinor != 200 {
		t.Fatalf("B price = %d, want 200", *p.PriceMinor)
	}
}

func TestReactivatingAWithdrawnProductIsBlocked(t *testing.T) {
	pool := newPool(t)
	mustApply(t, pool, active("A", 1, "Widget", 100), withdrawn("A", 2))

	_, err := apply(pool, active("A", 3, "Back again", 100))

	var blocking *catalog.BlockingError
	if !errors.As(err, &blocking) {
		t.Fatalf("want *BlockingError, got %v", err)
	}
	if blocking.Code != "REACTIVATION_BLOCKED" || !strings.Contains(blocking.Message, "stored WITHDRAWN revision 2, incoming ACTIVE revision 3") {
		t.Fatalf("blocking error = %+v", blocking)
	}
	if p := row(t, pool, "A"); p.Status != catalog.StatusWithdrawn || p.Revision != 2 {
		t.Fatalf("tombstone changed: %+v", p)
	}
}

func TestSameRevisionWithDifferentContentInOneBatchBlocks(t *testing.T) {
	pool := newPool(t)

	_, err := apply(pool, active("A", 1, "Widget", 100), active("A", 1, "Widget", 101))

	var blocking *catalog.BlockingError
	if !errors.As(err, &blocking) || blocking.Code != "VERSION_CONFLICT" {
		t.Fatalf("want VERSION_CONFLICT, got %v", err)
	}
	if n := countProducts(t, pool); n != 0 {
		t.Fatalf("products after rollback = %d, want 0", n)
	}
}

// A duplicate decision writes nothing, so only the row lock taken up front keeps another
// transaction from changing the product while this batch is deciding.
func TestApplyBatchLocksTheRowsItDecidesOn(t *testing.T) {
	pool := newPool(t)
	mustApply(t, pool, active("A", 1, "Widget", 100))

	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	applied, err := catalog.ApplyBatch(ctx, tx, []catalog.SupplierProduct{active("A", 1, "Widget", 100)})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	wantDecisions(t, applied, catalog.DecisionIgnoreDuplicate)

	other, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin second: %v", err)
	}
	defer other.Rollback(ctx)
	_, err = other.Exec(ctx, `SELECT 1 FROM products WHERE product_id = 'A' FOR UPDATE NOWAIT`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("second transaction locked the row, err = %v", err)
	}
}

func TestDatabaseRejectsRewritingARevision(t *testing.T) {
	pool := newPool(t)
	mustApply(t, pool, active("A", 5, "Widget", 100))

	_, err := pool.Exec(context.Background(), `UPDATE products SET revision = revision WHERE product_id = 'A'`)

	wantCheckViolation(t, err, "revision must increase")
}

func TestDatabaseRejectsReactivatingATombstone(t *testing.T) {
	pool := newPool(t)
	mustApply(t, pool, withdrawn("A", 3))

	_, err := pool.Exec(context.Background(),
		`UPDATE products SET status = 'ACTIVE', withdrawn_at = NULL, revision = 4, name = 'Back', price_minor = 1, currency = 'USD' WHERE product_id = 'A'`)

	wantCheckViolation(t, err, "WITHDRAWN is terminal")
}

func wantCheckViolation(t *testing.T, err error, message string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" || !strings.Contains(pgErr.Message, message) {
		t.Fatalf("want SQLSTATE 23514 containing %q, got %v", message, err)
	}
}

func fromSupplier(id string, e catalog.SupplierProduct) catalog.SupplierProduct {
	e.SupplierID = id
	return e
}

// seedCatalog stores, per supplier, active and withdrawn products:
//
//	SUP-001: A, B, D active; C withdrawn    SUP-002: X active; Y withdrawn
func seedCatalog(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	mustApply(t, pool,
		active("A", 1, "Widget", 100), active("B", 1, "Bolt", 200), withdrawn("C", 2), active("D", 1, "Dial", 300),
		fromSupplier("SUP-002", active("X", 1, "Xylophone", 400)), fromSupplier("SUP-002", withdrawn("Y", 3)))
}

func keys(products []catalog.Product) []string {
	out := []string{}
	for _, p := range products {
		out = append(out, p.SupplierID+"/"+p.ProductID)
	}
	return out
}

func TestListProductsHidesTombstonesUnlessAsked(t *testing.T) {
	pool := newPool(t)
	seedCatalog(t, pool)

	tests := []struct {
		name   string
		status string
		want   []string
	}{
		{"default is active only", "", []string{"SUP-001/A", "SUP-001/B", "SUP-001/D", "SUP-002/X"}},
		{"explicit active", "ACTIVE", []string{"SUP-001/A", "SUP-001/B", "SUP-001/D", "SUP-002/X"}},
		{"withdrawn only", "WITHDRAWN", []string{"SUP-001/C", "SUP-002/Y"}},
		{"all in key order", "ALL", []string{"SUP-001/A", "SUP-001/B", "SUP-001/C", "SUP-001/D", "SUP-002/X", "SUP-002/Y"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := catalog.ListProducts(context.Background(), pool, catalog.ListFilter{Status: tc.status})
			if err != nil {
				t.Fatalf("ListProducts: %v", err)
			}
			if !slices.Equal(keys(got), tc.want) {
				t.Fatalf("products = %v, want %v", keys(got), tc.want)
			}
		})
	}

	if _, err := catalog.ListProducts(context.Background(), pool, catalog.ListFilter{Status: "DELETED"}); err == nil {
		t.Fatal("unknown status filter was accepted")
	}
}

func TestListProductsFiltersBySupplierAndPages(t *testing.T) {
	pool := newPool(t)
	seedCatalog(t, pool)
	ctx := context.Background()

	tests := []struct {
		name   string
		filter catalog.ListFilter
		want   []string
	}{
		{"one supplier", catalog.ListFilter{SupplierID: "SUP-002", Status: "ALL"}, []string{"SUP-002/X", "SUP-002/Y"}},
		{"unknown supplier", catalog.ListFilter{SupplierID: "SUP-404", Status: "ALL"}, []string{}},
		{"first page", catalog.ListFilter{SupplierID: "SUP-001", Status: "ALL", Limit: 2}, []string{"SUP-001/A", "SUP-001/B"}},
		{"second page", catalog.ListFilter{SupplierID: "SUP-001", Status: "ALL", Limit: 2, Offset: 2}, []string{"SUP-001/C", "SUP-001/D"}},
		{"past the end", catalog.ListFilter{SupplierID: "SUP-001", Status: "ALL", Limit: 2, Offset: 4}, []string{}},
		{"zero limit uses the default page size", catalog.ListFilter{Status: "ALL"}, []string{"SUP-001/A", "SUP-001/B", "SUP-001/C", "SUP-001/D", "SUP-002/X", "SUP-002/Y"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := catalog.ListProducts(ctx, pool, tc.filter)
			if err != nil {
				t.Fatalf("ListProducts: %v", err)
			}
			if got == nil || !slices.Equal(keys(got), tc.want) {
				t.Fatalf("products = %v (nil=%t), want %v", keys(got), got == nil, tc.want)
			}
		})
	}
}

func TestListProductsReturnsStoredFields(t *testing.T) {
	pool := newPool(t)
	seedCatalog(t, pool)

	got, err := catalog.ListProducts(context.Background(), pool, catalog.ListFilter{SupplierID: "SUP-001", Status: "ALL", Limit: 3})
	if err != nil {
		t.Fatalf("ListProducts: %v", err)
	}
	a, c := got[0], got[2]
	if a.Status != catalog.StatusActive || a.Revision != 1 || *a.Name != "Widget" || *a.PriceMinor != 100 || *a.Currency != "USD" || a.WithdrawnAt != nil || a.PayloadHash == "" {
		t.Fatalf("active product = %+v", a)
	}
	if c.Status != catalog.StatusWithdrawn || c.Revision != 2 || c.Name != nil || c.WithdrawnAt == nil {
		t.Fatalf("tombstone = %+v", c)
	}
}

func TestGetProductReturnsTombstonesAndReportsMissing(t *testing.T) {
	pool := newPool(t)
	seedCatalog(t, pool)
	ctx := context.Background()

	a, err := catalog.GetProduct(ctx, pool, "SUP-001", "A")
	if err != nil || a.ProductID != "A" || *a.Name != "Widget" {
		t.Fatalf("GetProduct A = %+v, %v", a, err)
	}
	c, err := catalog.GetProduct(ctx, pool, "SUP-001", "C")
	if err != nil || c.Status != catalog.StatusWithdrawn || c.WithdrawnAt == nil {
		t.Fatalf("GetProduct C = %+v, %v", c, err)
	}

	for _, key := range [][2]string{{"SUP-001", "NOPE"}, {"SUP-002", "A"}, {"SUP-404", "A"}} {
		if _, err := catalog.GetProduct(ctx, pool, key[0], key[1]); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("GetProduct %v: want ErrNotFound, got %v", key, err)
		}
	}
}

// The read functions take storage.DB, so they also work inside a transaction and see its
// uncommitted writes.
func TestReadFunctionsWorkInsideATransaction(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := catalog.ApplyBatch(ctx, tx, []catalog.SupplierProduct{active("A", 1, "Widget", 100)}); err != nil {
			return err
		}
		if p, err := catalog.GetProduct(ctx, tx, supplier, "A"); err != nil || p.Revision != 1 {
			t.Errorf("GetProduct in tx = %+v, %v", p, err)
		}
		if list, err := catalog.ListProducts(ctx, tx, catalog.ListFilter{}); err != nil || len(list) != 1 {
			t.Errorf("ListProducts in tx = %v, %v", keys(list), err)
		}
		return errors.New("roll back")
	})
	if err == nil {
		t.Fatal("transaction unexpectedly committed")
	}
	if _, err := catalog.GetProduct(ctx, pool, supplier, "A"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("rolled-back product is visible: %v", err)
	}
}
