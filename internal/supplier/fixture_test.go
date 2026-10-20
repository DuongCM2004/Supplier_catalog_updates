package supplier

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const threePageFixture = `{
  "update_id": "UPD-1", "supplier_id": "SUP-001", "total_batches": 3,
  "description": "three pages", "requires": [], "expected": {"status": "COMPLETED"},
  "pages": [
    {"cursor": "", "batch": {"update_id": "UPD-1", "batch_id": "BATCH-001", "next_cursor": "cursor-002", "has_more": true,
      "products": [{"supplier_id": "SUP-001", "product_id": "SKU-1", "name": "Mug", "price_minor": 1200, "currency": "USD", "revision": 1, "status": "ACTIVE"}]}},
    {"cursor": "cursor-002", "batch": {"update_id": "UPD-1", "batch_id": "BATCH-002", "next_cursor": "cursor-003", "has_more": true, "products": []}},
    {"cursor": "cursor-003", "batch": {"update_id": "UPD-1", "batch_id": "BATCH-003", "next_cursor": "", "has_more": false, "products": []}}
  ]
}`

func writeFixture(t *testing.T, dir, id, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, id+".json"), []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", id, err)
	}
}

func newClient(t *testing.T, opts FaultOptions) (*FixtureClient, string) {
	t.Helper()
	dir := t.TempDir()
	writeFixture(t, dir, "UPD-1", threePageFixture)
	return NewFixtureClient(dir, opts), dir
}

func TestFetchBatchServesFirstPageForEmptyCursorAndFollowsCursors(t *testing.T) {
	c, _ := newClient(t, FaultOptions{})
	ctx := context.Background()

	first, err := c.FetchBatch(ctx, "UPD-1", "")
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.BatchID != "BATCH-001" || first.NextCursor != "cursor-002" || first.HasMore == nil || !*first.HasMore {
		t.Fatalf("first page = %+v", first)
	}
	if len(first.Products) != 1 || first.Products[0].ProductID != "SKU-1" || *first.Products[0].PriceMinor != 1200 {
		t.Fatalf("first page products = %+v", first.Products)
	}

	last, err := c.FetchBatch(ctx, "UPD-1", "cursor-003")
	if err != nil {
		t.Fatalf("last page: %v", err)
	}
	if last.BatchID != "BATCH-003" || last.HasMore == nil || *last.HasMore {
		t.Fatalf("last page = %+v", last)
	}
}

func TestGetUpdateReportsHeaderAndUnknownTotal(t *testing.T) {
	c, dir := newClient(t, FaultOptions{})
	ctx := context.Background()

	info, err := c.GetUpdate(ctx, "UPD-1")
	if err != nil {
		t.Fatalf("GetUpdate: %v", err)
	}
	if info.UpdateID != "UPD-1" || info.SupplierID != "SUP-001" || info.TotalBatches == nil || *info.TotalBatches != 3 {
		t.Fatalf("info = %+v", info)
	}

	writeFixture(t, dir, "NOTOTAL", `{"update_id":"NOTOTAL","supplier_id":"SUP-001","pages":[]}`)
	info, err = c.GetUpdate(ctx, "NOTOTAL")
	if err != nil {
		t.Fatalf("GetUpdate without total: %v", err)
	}
	if info.TotalBatches != nil {
		t.Fatalf("TotalBatches = %d, want nil so the API reports null", *info.TotalBatches)
	}
}

func TestUnknownCursorOrUpdateIsNotFound(t *testing.T) {
	c, _ := newClient(t, FaultOptions{})
	ctx := context.Background()

	if _, err := c.FetchBatch(ctx, "UPD-1", "cursor-999"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown cursor: err = %v, want ErrNotFound", err)
	}
	if _, err := c.FetchBatch(ctx, "NO-SUCH-UPDATE", ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown update fetch: err = %v, want ErrNotFound", err)
	}
	if _, err := c.GetUpdate(ctx, "NO-SUCH-UPDATE"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown update info: err = %v, want ErrNotFound", err)
	}
}

func TestUpdateIDCannotEscapeFixturesDirectory(t *testing.T) {
	root := t.TempDir()
	fixtures := filepath.Join(root, "fixtures")
	if err := os.Mkdir(fixtures, 0o700); err != nil {
		t.Fatal(err)
	}
	// A perfectly valid fixture just outside the served directory must stay unreachable.
	writeFixture(t, root, "secret", threePageFixture)
	c := NewFixtureClient(fixtures, FaultOptions{})

	ids := []string{"../secret", `..\secret`, "a/b", "", strings.Repeat("a", 65), "x\x00y", "UPD 1"}
	for _, id := range ids {
		if _, err := c.GetUpdate(context.Background(), id); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetUpdate(%q) err = %v, want ErrNotFound", id, err)
		}
		if _, err := c.FetchBatch(context.Background(), id, ""); !errors.Is(err, ErrNotFound) {
			t.Errorf("FetchBatch(%q) err = %v, want ErrNotFound", id, err)
		}
	}
}

func TestOutageCursorIsUnavailableBeforeAnyFileIsRead(t *testing.T) {
	c, _ := newClient(t, FaultOptions{OutageCursors: map[string]bool{"cursor-002": true}})
	ctx := context.Background()

	if _, err := c.FetchBatch(ctx, "UPD-1", ""); err != nil {
		t.Fatalf("healthy cursor: %v", err)
	}
	for i := 0; i < 3; i++ { // the outage does not heal on its own
		if _, err := c.FetchBatch(ctx, "UPD-1", "cursor-002"); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("attempt %d: err = %v, want ErrUnavailable", i, err)
		}
	}
	// No such update exists, yet the outage still wins: the file is never opened.
	if _, err := c.FetchBatch(ctx, "NO-SUCH-UPDATE", "cursor-002"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("outage on missing update: err = %v, want ErrUnavailable", err)
	}
}

func TestDelayEndsWhenContextEnds(t *testing.T) {
	c, _ := newClient(t, FaultOptions{Delay: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.FetchBatch(ctx, "UPD-1", "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("fetch blocked %s despite the cancelled context", elapsed)
	}
}

func TestDelayThenServesBatch(t *testing.T) {
	c, _ := newClient(t, FaultOptions{Delay: 5 * time.Millisecond})
	b, err := c.FetchBatch(context.Background(), "UPD-1", "")
	if err != nil || b.BatchID != "BATCH-001" {
		t.Fatalf("batch = %+v, err = %v", b, err)
	}
}

func TestCancelledContextFailsBeforeReading(t *testing.T) {
	c, _ := newClient(t, FaultOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.FetchBatch(ctx, "UPD-1", ""); !errors.Is(err, context.Canceled) {
		t.Errorf("FetchBatch err = %v, want context.Canceled", err)
	}
	if _, err := c.GetUpdate(ctx, "UPD-1"); !errors.Is(err, context.Canceled) {
		t.Errorf("GetUpdate err = %v, want context.Canceled", err)
	}
}

func TestTruncatedFixtureIsMalformedNotUnavailable(t *testing.T) {
	c, dir := newClient(t, FaultOptions{})
	writeFixture(t, dir, "CUT", threePageFixture[:len(threePageFixture)/2])
	ctx := context.Background()

	if _, err := c.FetchBatch(ctx, "CUT", ""); !errors.Is(err, ErrMalformed) {
		t.Errorf("FetchBatch err = %v, want ErrMalformed", err)
	}
	if _, err := c.GetUpdate(ctx, "CUT"); !errors.Is(err, ErrMalformed) {
		t.Errorf("GetUpdate err = %v, want ErrMalformed", err)
	}
	if _, err := c.FetchBatch(ctx, "CUT", ""); errors.Is(err, ErrUnavailable) {
		t.Errorf("a truncated file must not be classified as retryable: %v", err)
	}
}

func TestFixtureChangesAreSeenOnTheNextCall(t *testing.T) {
	c, dir := newClient(t, FaultOptions{})
	ctx := context.Background()

	if _, err := c.FetchBatch(ctx, "UPD-1", "cursor-002"); err != nil {
		t.Fatalf("before change: %v", err)
	}
	// The supplier "corrects" its feed: the page disappears. A cache would hide this.
	writeFixture(t, dir, "UPD-1", `{"update_id":"UPD-1","supplier_id":"SUP-001","pages":[]}`)
	if _, err := c.FetchBatch(ctx, "UPD-1", "cursor-002"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after change: err = %v, want ErrNotFound", err)
	}
}

func TestMissingHasMoreMarkerStaysDetectable(t *testing.T) {
	c, dir := newClient(t, FaultOptions{})
	writeFixture(t, dir, "NOMARK", `{"update_id":"NOMARK","supplier_id":"SUP-001","pages":[
	  {"cursor":"","batch":{"update_id":"NOMARK","batch_id":"BATCH-001","next_cursor":"","products":[]}}]}`)
	b, err := c.FetchBatch(context.Background(), "NOMARK", "")
	if err != nil {
		t.Fatal(err)
	}
	if b.HasMore != nil {
		t.Fatalf("HasMore = %v, want nil so the processor can reject the page", *b.HasMore)
	}
}

// Every fixture shipped in the repository must be loadable and internally consistent, or
// the demo and scenario tests would fail for reasons unrelated to the code under test.
func TestEveryShippedFixtureDecodes(t *testing.T) {
	dir := filepath.Join("..", "..", "fixtures")
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skipf("no fixture files in %s yet; nothing to decode", dir)
	}
	c := NewFixtureClient(dir, FaultOptions{})
	for _, file := range files {
		id := strings.TrimSuffix(filepath.Base(file), ".json")
		t.Run(id, func(t *testing.T) {
			f, err := c.loadFixture(id)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if f.UpdateID != id || f.SupplierID == "" {
				t.Fatalf("header update_id=%q supplier_id=%q, want update_id %q and a supplier", f.UpdateID, f.SupplierID, id)
			}
			hasFirstPage := false
			for _, p := range f.Pages {
				hasFirstPage = hasFirstPage || p.Cursor == ""
				if _, err := c.FetchBatch(context.Background(), id, p.Cursor); err != nil {
					t.Errorf("FetchBatch(cursor %q): %v", p.Cursor, err)
				}
			}
			if !hasFirstPage {
				t.Error(`no page with cursor "": the sync could never start`)
			}
		})
	}
}
