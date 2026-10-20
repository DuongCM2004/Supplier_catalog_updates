package catalog

import (
	"errors"
	"slices"
	"strconv"
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

// stored builds a stored product row as the repository would return it.
func stored(status Status, revision int64) *Product {
	p := &Product{SupplierID: testSupplier, ProductID: "SKU-1", Status: status, Revision: revision}
	if status == StatusActive {
		p.Name, p.PriceMinor, p.Currency = ptr("Widget"), ptr(int64(1250)), ptr("USD")
	}
	return p
}

func TestDecideAppliesRevisionRules(t *testing.T) {
	active := func(rev int64) SupplierProduct { return Canonicalize(activeEvent("SKU-1", rev)) }
	withdrawn := func(rev int64) SupplierProduct { return Canonicalize(withdrawnEvent("SKU-1", rev)) }

	tests := []struct {
		name string
		cur  *Product
		in   SupplierProduct
		want Decision
	}{
		{"unseen active product is created", nil, active(1), DecisionCreate},
		{"withdrawal of an unseen product leaves a tombstone", nil, withdrawn(4), DecisionCreateTombstone},

		{"higher active revision updates", stored(StatusActive, 2), active(3), DecisionUpdate},
		{"lower active revision is stale", stored(StatusActive, 3), active(2), DecisionIgnoreStale},
		{"identical active revision is a duplicate", stored(StatusActive, 3), active(3), DecisionIgnoreDuplicate},
		{"same revision with another price conflicts", stored(StatusActive, 3), with(active(3), func(e *SupplierProduct) { e.PriceMinor = ptr(int64(999)) }), DecisionVersionConflict},
		{"same revision with another name conflicts", stored(StatusActive, 3), with(active(3), func(e *SupplierProduct) { e.Name = ptr("Gadget") }), DecisionVersionConflict},
		{"same revision with another currency conflicts", stored(StatusActive, 3), with(active(3), func(e *SupplierProduct) { e.Currency = ptr("EUR") }), DecisionVersionConflict},

		{"higher withdrawal withdraws an active product", stored(StatusActive, 2), withdrawn(3), DecisionWithdraw},
		{"lower withdrawal of an active product is stale", stored(StatusActive, 3), withdrawn(2), DecisionIgnoreStale},
		{"withdrawal at the stored active revision conflicts", stored(StatusActive, 3), withdrawn(3), DecisionVersionConflict},

		{"lower active revision after a withdrawal is stale", stored(StatusWithdrawn, 3), active(2), DecisionIgnoreStale},
		{"higher active revision after a withdrawal is blocked", stored(StatusWithdrawn, 3), active(4), DecisionReactivationBlocked},
		{"active event at the tombstone revision conflicts", stored(StatusWithdrawn, 3), active(3), DecisionVersionConflict},
		{"repeated withdrawal is a duplicate", stored(StatusWithdrawn, 3), withdrawn(3), DecisionIgnoreDuplicate},
		{"repeated withdrawal is a duplicate although the tombstone kept its last details", &Product{Status: StatusWithdrawn, Revision: 3, Name: ptr("Widget"), PriceMinor: ptr(int64(1250)), Currency: ptr("USD")}, withdrawn(3), DecisionIgnoreDuplicate},
		{"higher withdrawal advances the tombstone", stored(StatusWithdrawn, 3), withdrawn(4), DecisionAdvanceTombstone},
		{"lower withdrawal after a withdrawal is stale", stored(StatusWithdrawn, 3), withdrawn(2), DecisionIgnoreStale},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Decide(tc.cur, tc.in); got != tc.want {
				t.Fatalf("Decide = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestCanonicalSortedOrdersByKeyThenRevision(t *testing.T) {
	describe := func(evs []SupplierProduct) []string {
		var out []string
		for _, e := range evs {
			out = append(out, e.SupplierID+"/"+e.ProductID+"@"+strconv.FormatInt(e.Revision, 10))
		}
		return out
	}
	at := func(supplier, product string, rev int64) SupplierProduct {
		return with(activeEvent(product, rev), func(e *SupplierProduct) { e.SupplierID = supplier })
	}

	t.Run("out of order revisions of one product", func(t *testing.T) {
		got := describe(canonicalSorted([]SupplierProduct{activeEvent("A", 5), activeEvent("A", 4)}))
		if want := []string{"SUP-001/A@4", "SUP-001/A@5"}; !slices.Equal(got, want) {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})

	t.Run("suppliers, then products, then revisions", func(t *testing.T) {
		in := []SupplierProduct{at("SUP-002", "A", 1), at("SUP-001", "B", 2), at("SUP-001", "B", 1), at("SUP-001", "A", 9)}
		got := describe(canonicalSorted(in))
		want := []string{"SUP-001/A@9", "SUP-001/B@1", "SUP-001/B@2", "SUP-002/A@1"}
		if !slices.Equal(got, want) {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})

	t.Run("equal keys keep the supplier order", func(t *testing.T) {
		first := with(activeEvent("A", 3), func(e *SupplierProduct) { e.Name = ptr("first") })
		second := with(activeEvent("A", 3), func(e *SupplierProduct) { e.Name = ptr("second") })
		got := canonicalSorted([]SupplierProduct{first, second})
		if *got[0].Name != "first" || *got[1].Name != "second" {
			t.Fatalf("stable order lost: %q, %q", *got[0].Name, *got[1].Name)
		}
	})

	t.Run("canonicalizes without modifying the input", func(t *testing.T) {
		in := []SupplierProduct{with(activeEvent("A", 1), func(e *SupplierProduct) { e.Name = ptr(" Widget "); e.Currency = ptr("usd") })}
		got := canonicalSorted(in)
		if *got[0].Name != "Widget" || *got[0].Currency != "USD" {
			t.Fatalf("not canonical: %q %q", *got[0].Name, *got[0].Currency)
		}
		if *in[0].Name != " Widget " {
			t.Fatal("input was modified")
		}
	})
}
