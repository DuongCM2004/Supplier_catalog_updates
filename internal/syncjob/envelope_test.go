package syncjob

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
)

func envTrue() *bool  { v := true; return &v }
func envFalse() *bool { v := false; return &v }

func envProduct(id string, revision int64) catalog.SupplierProduct {
	name, price, currency := "Widget "+id, int64(1999), "USD"
	return catalog.SupplierProduct{
		SupplierID: "SUP-001", ProductID: id, Name: &name, PriceMinor: &price, Currency: &currency,
		Revision: revision, Status: catalog.StatusActive,
	}
}

// envBatch is a sound mid-feed batch fetched at cursor "cursor-002".
func envBatch() supplier.Batch {
	return supplier.Batch{
		UpdateID: "UPDATE-1", BatchID: "BATCH-2", NextCursor: "cursor-003", HasMore: envTrue(),
		Products: []catalog.SupplierProduct{envProduct("SKU-1", 1), envProduct("SKU-2", 3)},
	}
}

func envProducts(n int) []catalog.SupplierProduct {
	products := make([]catalog.SupplierProduct, n)
	for i := range products {
		products[i] = envProduct(fmt.Sprintf("SKU-%d", i), 1)
	}
	return products
}

func TestValidateEnvelopeRules(t *testing.T) {
	const requested = "cursor-002"
	job := Job{ID: "job-1", SupplierID: "SUP-001", UpdateID: "UPDATE-1"}

	tests := []struct {
		name     string
		mutate   func(b *supplier.Batch)
		wantCode IssueCode // "" = envelope is sound
	}{
		{"sound mid-feed batch", func(b *supplier.Batch) {}, ""},
		{"sound final batch", func(b *supplier.Batch) { b.HasMore, b.NextCursor = envFalse(), "" }, ""},
		{"empty final batch", func(b *supplier.Batch) { b.HasMore, b.NextCursor, b.Products = envFalse(), "", nil }, ""},
		{"batch of another update", func(b *supplier.Batch) { b.UpdateID = "UPDATE-2" }, CodeInvalidBatch},
		{"missing batch id", func(b *supplier.Batch) { b.BatchID = "" }, CodeInvalidBatch},
		{"missing has_more marker", func(b *supplier.Batch) { b.HasMore = nil }, CodeInvalidBatch},
		{"has_more without next cursor", func(b *supplier.Batch) { b.NextCursor = "" }, CodeInvalidCursor},
		{"next cursor equal to the cursor just fetched", func(b *supplier.Batch) { b.NextCursor = requested }, CodeInvalidCursor},
		{"end of feed that still names a next cursor", func(b *supplier.Batch) { b.HasMore = envFalse() }, CodeInvalidCursor},
		{"NUL byte in update id", func(b *supplier.Batch) { b.UpdateID = "UPDATE-1\x00" }, CodeInvalidBatch},
		{"NUL byte in batch id", func(b *supplier.Batch) { b.BatchID = "BATCH\x002" }, CodeInvalidBatch},
		{"invalid UTF-8 in batch id", func(b *supplier.Batch) { b.BatchID = "BATCH-\xff2" }, CodeInvalidBatch},
		{"NUL byte in next cursor", func(b *supplier.Batch) { b.NextCursor = "cursor\x00003" }, CodeInvalidBatch},
		{"invalid UTF-8 in next cursor", func(b *supplier.Batch) { b.NextCursor = "cursor-\xc3" }, CodeInvalidBatch},
		{"as many products as one batch may carry", func(b *supplier.Batch) { b.Products = envProducts(maxBatchProducts) }, ""},
		{"one product more than one batch may carry", func(b *supplier.Batch) { b.Products = envProducts(maxBatchProducts + 1) }, CodeInvalidBatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := envBatch()
			tc.mutate(&b)
			got := validateEnvelope(job, requested, b)
			if tc.wantCode == "" {
				if got != nil {
					t.Fatalf("validateEnvelope = %+v, want nil", *got)
				}
				return
			}
			if got == nil {
				t.Fatalf("validateEnvelope = nil, want %s", tc.wantCode)
			}
			if got.Code != tc.wantCode || got.Severity != SeverityBlocking || got.Message == "" {
				t.Fatalf("validateEnvelope = %+v, want blocking %s with a message", *got, tc.wantCode)
			}
			if !cleanText(got.Message) {
				t.Fatalf("message %q cannot be stored in PostgreSQL, so the issue about this batch could not be recorded", got.Message)
			}
		})
	}
}

func TestValidateEnvelopeAcceptsFirstPageFetchedWithEmptyCursor(t *testing.T) {
	job := Job{UpdateID: "UPDATE-1"}
	b := envBatch()
	if got := validateEnvelope(job, "", b); got != nil {
		t.Fatalf("first page rejected: %+v", *got)
	}
	// "" is a real cursor (the first page): a batch pointing back at it is still a loop.
	b.NextCursor = ""
	if got := validateEnvelope(job, "", b); got == nil || got.Code != CodeInvalidCursor {
		t.Fatalf("has_more with empty next_cursor = %v, want INVALID_CURSOR", got)
	}
}

func TestBatchHashIgnoresJSONWhitespaceAndKeyOrder(t *testing.T) {
	compact := `{"update_id":"UPDATE-1","batch_id":"BATCH-1","next_cursor":"c2","has_more":true,
		"products":[{"supplier_id":"SUP-001","product_id":"SKU-1","name":"Widget","price_minor":500,"currency":"USD","revision":2,"status":"ACTIVE"}]}`
	reordered := `
	{
	  "products": [ { "status": "ACTIVE", "revision": 2, "currency": "USD", "price_minor": 500,
	                  "name": "Widget", "product_id": "SKU-1", "supplier_id": "SUP-001" } ],
	  "has_more": true,   "next_cursor": "c2",
	  "batch_id": "BATCH-1", "update_id": "UPDATE-1"
	}`
	var a, b supplier.Batch
	if err := json.Unmarshal([]byte(compact), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(reordered), &b); err != nil {
		t.Fatal(err)
	}
	if ha, hb := BatchHash(a), BatchHash(b); ha != hb {
		t.Fatalf("same content hashed differently: %s vs %s", ha, hb)
	}
}

func TestBatchHashIsStableAndTreatsCanonicalFormsAlike(t *testing.T) {
	b := envBatch()
	if BatchHash(b) != BatchHash(b) {
		t.Fatal("hash differs between two calls on the same batch")
	}
	if len(BatchHash(b)) != 64 {
		t.Fatalf("hash %q is not a hex SHA-256", BatchHash(b))
	}

	// The catalog stores trimmed names and upper-case currencies, so a batch that differs
	// only in those is the same batch.
	other := envBatch()
	name, currency := "  Widget SKU-1  ", " usd "
	other.Products[0].Name, other.Products[0].Currency = &name, &currency
	if BatchHash(b) != BatchHash(other) {
		t.Fatal("trimming the name and upper-casing the currency changed the batch hash")
	}

	// Details of a withdrawal carry no meaning and are dropped before hashing.
	w1 := supplier.Batch{UpdateID: "U", BatchID: "B", HasMore: envFalse(),
		Products: []catalog.SupplierProduct{{SupplierID: "SUP-001", ProductID: "SKU-1", Revision: 4, Status: catalog.StatusWithdrawn}}}
	w2 := w1
	w2.Products = []catalog.SupplierProduct{w1.Products[0]}
	stale := "old name"
	w2.Products[0].Name = &stale
	if BatchHash(w1) != BatchHash(w2) {
		t.Fatal("details of a withdrawn product changed the batch hash")
	}

	// An absent product list and an empty one are the same batch.
	empty := supplier.Batch{UpdateID: "U", BatchID: "B", HasMore: envFalse()}
	emptyList := empty
	emptyList.Products = []catalog.SupplierProduct{}
	if BatchHash(empty) != BatchHash(emptyList) {
		t.Fatal("nil and empty product lists hash differently")
	}
}

func TestBatchHashChangesWithAnyField(t *testing.T) {
	base := BatchHash(envBatch())
	str := func(s string) *string { return &s }
	num := func(n int64) *int64 { return &n }

	tests := []struct {
		name   string
		mutate func(b *supplier.Batch)
	}{
		{"update_id", func(b *supplier.Batch) { b.UpdateID = "UPDATE-9" }},
		{"batch_id", func(b *supplier.Batch) { b.BatchID = "BATCH-9" }},
		{"next_cursor", func(b *supplier.Batch) { b.NextCursor = "cursor-009" }},
		{"has_more", func(b *supplier.Batch) { b.HasMore = envFalse() }},
		{"missing has_more", func(b *supplier.Batch) { b.HasMore = nil }},
		{"product supplier_id", func(b *supplier.Batch) { b.Products[0].SupplierID = "SUP-002" }},
		{"product_id", func(b *supplier.Batch) { b.Products[0].ProductID = "SKU-9" }},
		{"product name", func(b *supplier.Batch) { b.Products[0].Name = str("Other") }},
		{"product price", func(b *supplier.Batch) { b.Products[0].PriceMinor = num(2000) }},
		{"product currency", func(b *supplier.Batch) { b.Products[0].Currency = str("EUR") }},
		{"product revision", func(b *supplier.Batch) { b.Products[0].Revision = 2 }},
		{"product status", func(b *supplier.Batch) {
			b.Products[0] = catalog.SupplierProduct{SupplierID: "SUP-001", ProductID: "SKU-1", Revision: 1, Status: catalog.StatusWithdrawn}
		}},
		{"product order", func(b *supplier.Batch) { b.Products[0], b.Products[1] = b.Products[1], b.Products[0] }},
		{"extra product", func(b *supplier.Batch) { b.Products = append(b.Products, envProduct("SKU-3", 1)) }},
		{"missing product", func(b *supplier.Batch) { b.Products = b.Products[:1] }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := envBatch()
			tc.mutate(&b)
			if got := BatchHash(b); got == base {
				t.Fatalf("changing %s did not change the batch hash", tc.name)
			}
		})
	}
}
