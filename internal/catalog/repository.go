package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/storage"
)

// productColumns is the one column list every product query selects or returns, in the
// order scanProduct reads it.
const productColumns = `supplier_id, product_id, status, revision, name, price_minor, currency,
  payload_hash, created_at, updated_at, withdrawn_at`

type productKey struct{ supplierID, productID string }

func keyOf(e SupplierProduct) productKey { return productKey{e.SupplierID, e.ProductID} }

func scanProduct(row pgx.Row) (Product, error) {
	var p Product
	err := row.Scan(&p.SupplierID, &p.ProductID, &p.Status, &p.Revision, &p.Name, &p.PriceMinor, &p.Currency,
		&p.PayloadHash, &p.CreatedAt, &p.UpdatedAt, &p.WithdrawnAt)
	return p, err
}

func rowToProduct(row pgx.CollectableRow) (Product, error) { return scanProduct(row) }

// ApplyBatch applies one supplier batch to the catalog inside the caller's transaction and
// returns what happened to each event, in the order it was applied.
//
// Events are applied in (supplier, product, revision) order, so the outcome does not depend
// on how the supplier ordered them. The affected rows are locked first, in that same key
// order, so concurrent writers cannot deadlock and every decision is made on a row nobody
// else can change. A conflicting event aborts the whole batch with a *BlockingError and
// leaves the transaction open: the caller rolls it back, which discards the writes already
// made for earlier events of this batch.
func ApplyBatch(ctx context.Context, tx pgx.Tx, events []SupplierProduct) ([]Applied, error) {
	evs := canonicalSorted(events)
	cur, err := lockProducts(ctx, tx, keysOf(evs))
	if err != nil {
		return nil, err
	}

	out := make([]Applied, 0, len(evs))
	for _, ev := range evs {
		before := cur[keyOf(ev)] // nil when the product is not stored yet
		d := Decide(before, ev)
		var after *Product
		switch d {
		case DecisionVersionConflict, DecisionReactivationBlocked:
			return nil, &BlockingError{
				Code:       string(d),
				SupplierID: ev.SupplierID,
				ProductID:  ev.ProductID,
				Revision:   ev.Revision,
				Message:    describeBlock(before, ev, d),
			}
		case DecisionCreate, DecisionCreateTombstone:
			// A plain INSERT: if another writer created the row meanwhile this fails loudly
			// instead of overwriting. Only one job writes at a time, so it cannot happen.
			after, err = insertProduct(ctx, tx, ev)
		case DecisionUpdate, DecisionWithdraw, DecisionAdvanceTombstone:
			after, err = updateProduct(ctx, tx, before, ev, d) // the trigger re-checks revision and terminal status
		}
		if err != nil {
			return nil, err
		}
		if after != nil {
			cur[keyOf(ev)] = after // later events of this batch for the same product see this write
		}
		out = append(out, Applied{Event: ev, Decision: d, Before: before, After: after})
	}
	return out, nil
}

func describeBlock(stored *Product, in SupplierProduct, d Decision) string {
	reason := "same revision with different content"
	if d == DecisionReactivationBlocked {
		reason = "a withdrawn product cannot become active again"
	}
	return fmt.Sprintf("stored %s revision %d, incoming %s revision %d: %s",
		stored.Status, stored.Revision, in.Status, in.Revision, reason)
}

// keysOf lists the distinct products of a batch already sorted by key.
func keysOf(sorted []SupplierProduct) []productKey {
	keys := make([]productKey, 0, len(sorted))
	for _, e := range sorted {
		if k := keyOf(e); len(keys) == 0 || keys[len(keys)-1] != k {
			keys = append(keys, k)
		}
	}
	return keys
}

// Rows are locked in the order PostgreSQL returns them, which is key order; products that
// are not stored yet simply do not appear.
const lockProductsSQL = `
SELECT ` + productColumns + `
FROM products p
JOIN unnest($1::text[], $2::text[]) AS k(sid, pid) ON p.supplier_id = k.sid AND p.product_id = k.pid
ORDER BY p.supplier_id, p.product_id
FOR UPDATE OF p`

func lockProducts(ctx context.Context, tx pgx.Tx, keys []productKey) (map[productKey]*Product, error) {
	cur := make(map[productKey]*Product, len(keys))
	if len(keys) == 0 {
		return cur, nil
	}
	suppliers, products := make([]string, len(keys)), make([]string, len(keys))
	for i, k := range keys {
		suppliers[i], products[i] = k.supplierID, k.productID
	}
	rows, err := tx.Query(ctx, lockProductsSQL, suppliers, products)
	if err != nil {
		return nil, fmt.Errorf("catalog: lock products: %w", err)
	}
	locked, err := pgx.CollectRows(rows, rowToProduct)
	if err != nil {
		return nil, fmt.Errorf("catalog: lock products: %w", err)
	}
	for i := range locked {
		cur[productKey{locked[i].SupplierID, locked[i].ProductID}] = &locked[i]
	}
	return cur, nil
}

// withdrawn_at is stamped by the database in the same statement, so a tombstone can never
// be stored without it (the table also checks this).
const insertProductSQL = `
INSERT INTO products (supplier_id, product_id, status, revision, name, price_minor, currency, payload_hash, withdrawn_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $3::text = 'WITHDRAWN' THEN now() END)
RETURNING ` + productColumns

func insertProduct(ctx context.Context, tx pgx.Tx, ev SupplierProduct) (*Product, error) {
	row := tx.QueryRow(ctx, insertProductSQL, ev.SupplierID, ev.ProductID, string(ev.Status), ev.Revision,
		ev.Name, ev.PriceMinor, ev.Currency, PayloadHash(ev))
	p, err := scanProduct(row)
	if err != nil {
		return nil, fmt.Errorf("catalog: insert product %s/%s: %w", ev.SupplierID, ev.ProductID, err)
	}
	return &p, nil
}

const updateActiveSQL = `
UPDATE products
SET revision = $3, name = $4, price_minor = $5, currency = $6, payload_hash = $7, updated_at = now()
WHERE supplier_id = $1 AND product_id = $2
RETURNING ` + productColumns

// A withdrawal keeps the last known name, price and currency so staff can still see what
// was withdrawn.
const withdrawSQL = `
UPDATE products
SET status = 'WITHDRAWN', revision = $3, payload_hash = $4, withdrawn_at = now(), updated_at = now()
WHERE supplier_id = $1 AND product_id = $2
RETURNING ` + productColumns

const advanceTombstoneSQL = `
UPDATE products
SET revision = $3, payload_hash = $4, updated_at = now()
WHERE supplier_id = $1 AND product_id = $2
RETURNING ` + productColumns

// updateProduct writes one UPDATE, WITHDRAW or ADVANCE_TOMBSTONE decision to a locked row.
// Exactly one row must change; the trigger on products independently rejects a revision
// that does not increase and any move away from WITHDRAWN.
func updateProduct(ctx context.Context, tx pgx.Tx, before *Product, ev SupplierProduct, d Decision) (*Product, error) {
	hash := PayloadHash(ev)
	var row pgx.Row
	switch d {
	case DecisionUpdate:
		row = tx.QueryRow(ctx, updateActiveSQL, ev.SupplierID, ev.ProductID, ev.Revision, ev.Name, ev.PriceMinor, ev.Currency, hash)
	case DecisionWithdraw:
		row = tx.QueryRow(ctx, withdrawSQL, ev.SupplierID, ev.ProductID, ev.Revision, hash)
	case DecisionAdvanceTombstone:
		row = tx.QueryRow(ctx, advanceTombstoneSQL, ev.SupplierID, ev.ProductID, ev.Revision, hash)
	default:
		return nil, fmt.Errorf("catalog: decision %s does not update a product", d)
	}
	p, err := scanProduct(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("catalog: %s %s/%s from revision %d: no row updated", d, ev.SupplierID, ev.ProductID, before.Revision)
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: %s %s/%s from revision %d: %w", d, ev.SupplierID, ev.ProductID, before.Revision, err)
	}
	return &p, nil
}

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

const listProductsSQL = `
SELECT ` + productColumns + `
FROM products
WHERE ($1::text = '' OR supplier_id = $1)
  AND ($2::text = 'ALL' OR status = $2)
ORDER BY supplier_id, product_id
LIMIT $3 OFFSET $4`

// ListProducts returns stored products ordered by key. Status "" means ACTIVE, so
// tombstones stay out of the default listing; "WITHDRAWN" and "ALL" include them. A Limit
// outside 1..500 falls back to the default or the cap, so a zero value never silently
// returns nothing and no call is unbounded.
func ListProducts(ctx context.Context, db storage.DB, f ListFilter) ([]Product, error) {
	status := f.Status
	switch status {
	case "":
		status = string(StatusActive)
	case string(StatusActive), string(StatusWithdrawn), "ALL":
	default:
		return nil, fmt.Errorf("catalog: unknown status filter %q", f.Status)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	limit = min(limit, maxListLimit)
	offset := max(f.Offset, 0)

	rows, err := db.Query(ctx, listProductsSQL, f.SupplierID, status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("catalog: list products: %w", err)
	}
	products, err := pgx.CollectRows(rows, rowToProduct)
	if err != nil {
		return nil, fmt.Errorf("catalog: list products: %w", err)
	}
	return products, nil
}

const getProductSQL = `SELECT ` + productColumns + ` FROM products WHERE supplier_id = $1 AND product_id = $2`

// GetProduct returns one stored product, tombstones included, or ErrNotFound.
func GetProduct(ctx context.Context, db storage.DB, supplierID, productID string) (Product, error) {
	p, err := scanProduct(db.QueryRow(ctx, getProductSQL, supplierID, productID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Product{}, ErrNotFound
	}
	if err != nil {
		return Product{}, fmt.Errorf("catalog: get product %s/%s: %w", supplierID, productID, err)
	}
	return p, nil
}
