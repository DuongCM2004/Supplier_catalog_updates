package catalog

import (
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const (
	propertyHistories = 200
	propertySeedEnv   = "CATALOG_PROPERTY_SEED"
)

// Whatever order a valid history arrives in, the revision rules must leave every product
// at its highest-revision event. The history is applied one event per batch and as one
// batch (which canonicalSorted reorders), exactly as ApplyBatch would feed Decide. A
// failure is reproduced with CATALOG_PROPERTY_SEED=<seed from the log>.
func TestShuffledValidHistoriesConvergeToMaxRevision(t *testing.T) {
	seed := rand.Uint64()
	if raw := os.Getenv(propertySeedEnv); raw != "" {
		var err error
		if seed, err = strconv.ParseUint(raw, 10, 64); err != nil {
			t.Fatalf("%s=%q is not an unsigned integer: %v", propertySeedEnv, raw, err)
		}
	}
	t.Logf("seed %d (reproduce with %s=%d)", seed, propertySeedEnv, seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	for i := range propertyHistories {
		history := randomValidHistory(rng)
		if err := ValidateBatch(history, testSupplier); err != nil {
			t.Fatalf("history %d: generator produced invalid records: %v", i, err)
		}
		// The generated order has each product's revisions ascending, which is the order
		// canonicalSorted must hand a single batch to Decide in, however it was shuffled.
		ascending := slices.Clone(history)
		rng.Shuffle(len(history), func(a, b int) { history[a], history[b] = history[b], history[a] })

		singles := make([][]SupplierProduct, len(history))
		for j, e := range history {
			singles[j] = []SupplierProduct{e}
		}
		paths := []struct {
			name    string
			batches [][]SupplierProduct
			applied []SupplierProduct // the order Decide is expected to see the events in
		}{
			{"one event per batch", singles, history},
			{"one batch", [][]SupplierProduct{history}, ascending},
		}
		for _, p := range paths {
			got, err := foldBatches(p.batches)
			if err != nil {
				t.Fatalf("history %d, %s: %v\nhistory: %s", i, p.name, err, historyString(history))
			}
			want := expectedRows(p.applied)
			if len(got) != len(want) {
				t.Fatalf("history %d, %s: %d products stored, want %d\nhistory: %s", i, p.name, len(got), len(want), historyString(history))
			}
			for k, w := range want {
				if g := got[k]; !sameRow(g, w) {
					t.Fatalf("history %d, %s: %s/%s stored %s, want %s\nhistory: %s",
						i, p.name, k.supplierID, k.productID, rowString(g), rowString(w), historyString(history))
				}
			}
		}
	}
}

// randomValidHistory builds the events of 1-4 products. Each product has ACTIVE revisions
// 1..k with random details and, when it has none (a withdrawal of an unseen product) or by
// chance, a WITHDRAWN event at k+1 (sometimes repeated at k+2), so no revision repeats
// with another payload and no ACTIVE event sits above a withdrawal. One event is sometimes
// repeated verbatim, as a supplier resending a record would.
func randomValidHistory(rng *rand.Rand) []SupplierProduct {
	var history []SupplierProduct
	for p := range 1 + rng.IntN(4) {
		id := "SKU-" + strconv.Itoa(p+1)
		active := rng.IntN(6)
		for rev := int64(1); rev <= int64(active); rev++ {
			history = append(history, randomActive(rng, id, rev))
		}
		if active == 0 || rng.IntN(2) == 0 {
			history = append(history, withdrawnEvent(id, int64(active)+1))
			if rng.IntN(4) == 0 { // the supplier withdraws it again at the next revision
				history = append(history, withdrawnEvent(id, int64(active)+2))
			}
		}
	}
	if rng.IntN(4) == 0 {
		history = append(history, history[rng.IntN(len(history))])
	}
	return history
}

// randomActive pads the name and varies the currency's case and spacing, so the fold also
// proves that canonical details are what get stored.
func randomActive(rng *rand.Rand, productID string, revision int64) SupplierProduct {
	pad := strings.Repeat(" ", rng.IntN(3))
	currencies := []string{"usd", "EUR", " vnd "}
	return with(activeEvent(productID, revision), func(e *SupplierProduct) {
		e.Name = ptr(pad + "item-" + strconv.Itoa(rng.IntN(1000)) + pad)
		e.PriceMinor = ptr(rng.Int64N(1_000_000))
		e.Currency = ptr(currencies[rng.IntN(len(currencies))])
	})
}

// foldBatches applies batches to an in-memory catalog the way ApplyBatch does: each batch
// is canonicalized and sorted, then every event is decided against the row its predecessors
// left. A blocking decision is an error, because a valid history never needs one.
func foldBatches(batches [][]SupplierProduct) (map[productKey]*Product, error) {
	rows := map[productKey]*Product{}
	for _, batch := range batches {
		for _, ev := range canonicalSorted(batch) {
			k := keyOf(ev)
			d := Decide(rows[k], ev)
			next, err := applyDecision(rows[k], ev, d)
			if err != nil {
				return nil, fmt.Errorf("%s/%s revision %d: %w", ev.SupplierID, ev.ProductID, ev.Revision, err)
			}
			rows[k] = next
		}
	}
	return rows, nil
}

// applyDecision returns the stored row after a decision, as repository.go writes it: a
// create or update takes the event's details, a withdrawal keeps the row's last known
// details, advancing a tombstone changes only its revision, and ignored events change nothing.
func applyDecision(cur *Product, ev SupplierProduct, d Decision) (*Product, error) {
	switch d {
	case DecisionCreate, DecisionCreateTombstone, DecisionUpdate:
		return &Product{
			SupplierID: ev.SupplierID, ProductID: ev.ProductID, Status: ev.Status, Revision: ev.Revision,
			Name: ev.Name, PriceMinor: ev.PriceMinor, Currency: ev.Currency,
		}, nil
	case DecisionWithdraw, DecisionAdvanceTombstone:
		next := *cur
		next.Status, next.Revision = StatusWithdrawn, ev.Revision
		return &next, nil
	case DecisionIgnoreStale, DecisionIgnoreDuplicate:
		return cur, nil
	default:
		return nil, fmt.Errorf("%s blocks a valid history", d)
	}
}

// expectedRows is what the rules promise once every event of order has been applied: the
// highest-revision event decides status and revision, and an ACTIVE row holds that event's
// details. A tombstone holds the details the row had when the withdrawal arrived (the
// highest ACTIVE revision applied before it) or none if it arrived first, so those details
// depend on the arrival order; every later ACTIVE event is stale and cannot change them.
func expectedRows(order []SupplierProduct) map[productKey]*Product {
	top := map[productKey]SupplierProduct{}
	lastKnown := map[productKey]SupplierProduct{}
	withdrawn := map[productKey]bool{}
	for _, e := range order {
		e, k := Canonicalize(e), keyOf(e)
		if t, ok := top[k]; !ok || e.Revision > t.Revision {
			top[k] = e
		}
		if e.Status == StatusWithdrawn {
			withdrawn[k] = true
		} else if l, ok := lastKnown[k]; !withdrawn[k] && (!ok || e.Revision > l.Revision) {
			lastKnown[k] = e
		}
	}

	rows := make(map[productKey]*Product, len(top))
	for k, e := range top {
		src := e
		if e.Status == StatusWithdrawn {
			src = lastKnown[k] // zero value (no details) when nothing was known
		}
		rows[k] = &Product{
			SupplierID: k.supplierID, ProductID: k.productID, Status: e.Status, Revision: e.Revision,
			Name: src.Name, PriceMinor: src.PriceMinor, Currency: src.Currency,
		}
	}
	return rows
}

func sameRow(got, want *Product) bool {
	return got != nil && got.Status == want.Status && got.Revision == want.Revision &&
		equalPtr(got.Name, want.Name) && equalPtr(got.PriceMinor, want.PriceMinor) && equalPtr(got.Currency, want.Currency)
}

func rowString(p *Product) string {
	if p == nil {
		return "no row"
	}
	return fmt.Sprintf("%s r%d name=%s price=%s currency=%s", p.Status, p.Revision, show(p.Name), show(p.PriceMinor), show(p.Currency))
}

func show[T any](p *T) string {
	if p == nil {
		return "none"
	}
	return fmt.Sprintf("%v", *p)
}

// historyString lists events as product@revision plus A(ctive) or W(ithdrawn), in order.
func historyString(events []SupplierProduct) string {
	parts := make([]string, len(events))
	for i, e := range events {
		parts[i] = fmt.Sprintf("%s@%d%s", e.ProductID, e.Revision, string(e.Status)[:1])
	}
	return strings.Join(parts, " ")
}
