package catalog

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

const testSupplier = "SUP-001"

// activeEvent is a valid ACTIVE event; tests tweak one field at a time.
func activeEvent(productID string, revision int64) SupplierProduct {
	return SupplierProduct{
		SupplierID: testSupplier,
		ProductID:  productID,
		Name:       ptr("Widget"),
		PriceMinor: ptr(int64(1250)),
		Currency:   ptr("USD"),
		Revision:   revision,
		Status:     StatusActive,
	}
}

// with returns a copy of e after f has adjusted it.
func with(e SupplierProduct, f func(*SupplierProduct)) SupplierProduct {
	f(&e)
	return e
}

func withdrawnEvent(productID string, revision int64) SupplierProduct {
	return SupplierProduct{SupplierID: testSupplier, ProductID: productID, Revision: revision, Status: StatusWithdrawn}
}

func problemsOf(t *testing.T, err error) []string {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %T (%v)", err, err)
	}
	return ve.Problems
}

func TestValidateBatchAcceptsValidRecords(t *testing.T) {
	tests := []struct {
		name   string
		events []SupplierProduct
	}{
		{"empty batch", nil},
		{"active product", []SupplierProduct{activeEvent("SKU-1", 1)}},
		{"free product", []SupplierProduct{with(activeEvent("SKU-1", 1), func(e *SupplierProduct) { e.PriceMinor = ptr(int64(0)) })}},
		{"lower case currency is normalised later", []SupplierProduct{with(activeEvent("SKU-1", 1), func(e *SupplierProduct) { e.Currency = ptr(" usd ") })}},
		{"withdrawal needs only identity, revision and status", []SupplierProduct{withdrawnEvent("SKU-9", 4)}},
		{"withdrawal details are ignored", []SupplierProduct{with(withdrawnEvent("SKU-9", 4), func(e *SupplierProduct) {
			e.Name, e.PriceMinor, e.Currency = ptr("  "), ptr(int64(-5)), ptr("nope")
		})}},
		{"longest allowed product id and name", []SupplierProduct{with(activeEvent(strings.Repeat("p", maxProductIDLen), 1), func(e *SupplierProduct) {
			e.Name = ptr(strings.Repeat("n", maxNameLen))
		})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateBatch(tc.events, testSupplier); err != nil {
				t.Fatalf("ValidateBatch: %v", err)
			}
		})
	}
}

func TestValidateBatchRejectsInvalidRecords(t *testing.T) {
	mod := func(f func(e *SupplierProduct)) SupplierProduct { return with(activeEvent("SKU-1", 1), f) }
	tests := []struct {
		name  string
		event SupplierProduct
		want  string
	}{
		{"missing name", mod(func(e *SupplierProduct) { e.Name = nil }), "SKU-1: name is required"},
		{"blank name", mod(func(e *SupplierProduct) { e.Name = ptr(" \t ") }), "SKU-1: name must not be blank"},
		{"name too long", mod(func(e *SupplierProduct) { e.Name = ptr(strings.Repeat("n", maxNameLen+1)) }), "SKU-1: name must be at most 255 characters"},
		{"negative price", mod(func(e *SupplierProduct) { e.PriceMinor = ptr(int64(-5)) }), "SKU-1: price_minor must be >= 0"},
		{"missing price", mod(func(e *SupplierProduct) { e.PriceMinor = nil }), "SKU-1: price_minor is required"},
		{"missing currency", mod(func(e *SupplierProduct) { e.Currency = nil }), "SKU-1: currency is required"},
		{"two letter currency", mod(func(e *SupplierProduct) { e.Currency = ptr("US") }), "SKU-1: currency must be a 3-letter code such as USD"},
		{"four letter currency", mod(func(e *SupplierProduct) { e.Currency = ptr("USDX") }), "SKU-1: currency must be a 3-letter code such as USD"},
		{"currency with a digit", mod(func(e *SupplierProduct) { e.Currency = ptr("U5D") }), "SKU-1: currency must be a 3-letter code such as USD"},
		{"revision zero", mod(func(e *SupplierProduct) { e.Revision = 0 }), "SKU-1: revision must be >= 1"},
		{"negative revision", mod(func(e *SupplierProduct) { e.Revision = -2 }), "SKU-1: revision must be >= 1"},
		{"unknown status", mod(func(e *SupplierProduct) { e.Status = "PENDING" }), "SKU-1: status must be ACTIVE or WITHDRAWN"},
		{"empty status", mod(func(e *SupplierProduct) { e.Status = "" }), "SKU-1: status must be ACTIVE or WITHDRAWN"},
		{"supplier of another job", mod(func(e *SupplierProduct) { e.SupplierID = "SUP-999" }), `SKU-1: supplier_id must be "SUP-001"`},
		{"empty product id", mod(func(e *SupplierProduct) { e.ProductID = "" }), "record #1: product_id must not be empty"},
		{"blank product id", mod(func(e *SupplierProduct) { e.ProductID = "  " }), "record #1: product_id must not be empty"},
		{"product id too long", mod(func(e *SupplierProduct) { e.ProductID = strings.Repeat("p", maxProductIDLen+1) }), "record #1: product_id must be at most 128 characters"},
		{"NUL byte in name", mod(func(e *SupplierProduct) { e.Name = ptr("Wid\x00get") }), "SKU-1: name must be valid UTF-8 without NUL bytes"},
		{"invalid UTF-8 in name", mod(func(e *SupplierProduct) { e.Name = ptr("Wid\xffget") }), "SKU-1: name must be valid UTF-8 without NUL bytes"},
		{"NUL byte in currency", mod(func(e *SupplierProduct) { e.Currency = ptr("US\x00") }), "SKU-1: currency must be a 3-letter code such as USD"},
		{"NUL byte in product id", mod(func(e *SupplierProduct) { e.ProductID = "SKU\x00-1" }), "record #1: product_id must be valid UTF-8 without NUL bytes"},
		{"invalid UTF-8 in product id", mod(func(e *SupplierProduct) { e.ProductID = "SKU\xc3" }), "record #1: product_id must be valid UTF-8 without NUL bytes"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := problemsOf(t, ValidateBatch([]SupplierProduct{tc.event}, testSupplier))
			if !slices.Equal(got, []string{tc.want}) {
				t.Fatalf("problems = %q, want [%q]", got, tc.want)
			}
		})
	}
}

func TestValidateBatchCollectsEveryProblem(t *testing.T) {
	bad := activeEvent("SKU-302", 0)
	bad.PriceMinor = ptr(int64(-5))
	bad.Currency = ptr("US")
	events := []SupplierProduct{
		activeEvent("SKU-301", 1),
		bad,
		withdrawnEvent("", 2),
		with(activeEvent("SKU-304", 1), func(e *SupplierProduct) { e.Name = nil }),
	}

	got := problemsOf(t, ValidateBatch(events, testSupplier))
	want := []string{
		"SKU-302: revision must be >= 1",
		"SKU-302: price_minor must be >= 0",
		"SKU-302: currency must be a 3-letter code such as USD",
		"record #3: product_id must not be empty",
		"SKU-304: name is required",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("problems:\n got %q\nwant %q", got, want)
	}
}

// Problem messages end up in a TEXT column; a NUL byte or invalid UTF-8 echoed from the
// supplier would make recording the issue itself fail.
func TestValidationMessagesAreAlwaysStorableText(t *testing.T) {
	events := []SupplierProduct{
		{SupplierID: "SUP\x00", ProductID: "A\x00B", Name: ptr("x\xff"), Currency: ptr("\x00"), Revision: 0, Status: "\x00"},
		{SupplierID: testSupplier, ProductID: "ok\xff", Status: StatusActive, Revision: 1},
	}
	for _, p := range problemsOf(t, ValidateBatch(events, testSupplier)) {
		if !isCleanText(p) {
			t.Errorf("problem %q is not storable text", p)
		}
	}
}

func TestCanonicalizeNormalisesAndDropsWithdrawalDetails(t *testing.T) {
	in := activeEvent("SKU-1", 3)
	in.Name = ptr("  Blue Widget \n")
	in.Currency = ptr(" usd ")

	got := Canonicalize(in)
	if *got.Name != "Blue Widget" || *got.Currency != "USD" {
		t.Fatalf("canonical name/currency = %q/%q", *got.Name, *got.Currency)
	}
	if *in.Name != "  Blue Widget \n" || *in.Currency != " usd " {
		t.Fatal("Canonicalize modified its input")
	}

	w := withdrawnEvent("SKU-1", 4)
	w.Name, w.PriceMinor, w.Currency = ptr("Old"), ptr(int64(9)), ptr("usd")
	if c := Canonicalize(w); c.Name != nil || c.PriceMinor != nil || c.Currency != nil {
		t.Fatalf("withdrawal kept details: %+v", c)
	}
}

func TestPayloadHashIgnoresCosmeticDifferences(t *testing.T) {
	base := activeEvent("SKU-1", 2)
	messy := activeEvent("SKU-1", 2)
	messy.Name = ptr("  Widget\t")
	messy.Currency = ptr(" usd")
	if PayloadHash(base) != PayloadHash(messy) {
		t.Error("whitespace and currency case changed the hash")
	}

	w1, w2 := withdrawnEvent("SKU-1", 3), withdrawnEvent("SKU-1", 3)
	w2.Name, w2.PriceMinor, w2.Currency = ptr("Left over"), ptr(int64(77)), ptr("EUR")
	if PayloadHash(w1) != PayloadHash(w2) {
		t.Error("withdrawal details changed the hash")
	}
}

func TestPayloadHashChangesWithEveryMeaningfulField(t *testing.T) {
	base := activeEvent("SKU-1", 2)
	variants := map[string]SupplierProduct{
		"supplier_id": with(base, func(e *SupplierProduct) { e.SupplierID = "SUP-002" }),
		"product_id":  with(base, func(e *SupplierProduct) { e.ProductID = "SKU-2" }),
		"revision":    with(base, func(e *SupplierProduct) { e.Revision = 3 }),
		"status":      withdrawnEvent("SKU-1", 2),
		"name":        with(base, func(e *SupplierProduct) { e.Name = ptr("Gadget") }),
		"price_minor": with(base, func(e *SupplierProduct) { e.PriceMinor = ptr(int64(1251)) }),
		"currency":    with(base, func(e *SupplierProduct) { e.Currency = ptr("EUR") }),
	}
	baseHash := PayloadHash(base)
	seen := map[string]string{baseHash: "base"}
	for field, v := range variants {
		h := PayloadHash(v)
		if prev, dup := seen[h]; dup {
			t.Errorf("changing %s gives the same hash as %s", field, prev)
		}
		seen[h] = field
	}
}

func TestPayloadHashIsStableHexSHA256(t *testing.T) {
	e := activeEvent("SKU-1", 2)
	first := PayloadHash(e)
	if len(first) != 64 || strings.Trim(first, "0123456789abcdef") != "" {
		t.Fatalf("hash %q is not 64 lower-case hex characters", first)
	}
	for range 1000 {
		if got := PayloadHash(e); got != first {
			t.Fatalf("hash changed between calls: %s vs %s", got, first)
		}
	}
}
