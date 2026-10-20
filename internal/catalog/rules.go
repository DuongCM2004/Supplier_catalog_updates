package catalog

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxProductIDLen = 128 // characters
	maxNameLen      = 255 // characters, after trimming
)

// ValidateBatch checks every record of a supplier batch before any database work and
// reports all problems at once, so staff can fix a feed in one pass. It returns a
// *ValidationError or nil.
//
// Strings are also checked for invalid UTF-8 and NUL bytes: PostgreSQL rejects both
// (SQLSTATE 22021), and that must surface as invalid product data rather than as a
// database failure after the batch transaction has started. The details of a withdrawal
// are discarded by Canonicalize and never reach the database, so they are not validated.
func ValidateBatch(events []SupplierProduct, supplierID string) error {
	var problems []string
	for i, e := range events {
		problems = append(problems, validateEvent(e, supplierID, i)...)
	}
	if len(problems) == 0 {
		return nil
	}
	return &ValidationError{Problems: problems}
}

func validateEvent(e SupplierProduct, supplierID string, index int) []string {
	label := recordLabel(e, index)
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, label+": "+fmt.Sprintf(format, args...))
	}

	if e.SupplierID != supplierID {
		// The offending value is not echoed: it may be arbitrarily long or not valid text.
		add("supplier_id must be %q", supplierID)
	}
	switch {
	case strings.TrimSpace(e.ProductID) == "":
		add("product_id must not be empty")
	case !isCleanText(e.ProductID):
		add("product_id must be valid UTF-8 without NUL bytes")
	case utf8.RuneCountInString(e.ProductID) > maxProductIDLen:
		add("product_id must be at most %d characters", maxProductIDLen)
	}
	if e.Revision < 1 {
		add("revision must be >= 1")
	}

	switch e.Status {
	case StatusWithdrawn:
		// Identity, revision and status are all a withdrawal needs.
	case StatusActive:
		problems = append(problems, validateActiveDetails(e, label)...)
	default:
		add("status must be ACTIVE or WITHDRAWN")
	}
	return problems
}

func validateActiveDetails(e SupplierProduct, label string) []string {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, label+": "+fmt.Sprintf(format, args...))
	}

	if e.Name == nil {
		add("name is required")
	} else {
		switch name := strings.TrimSpace(*e.Name); {
		case !isCleanText(*e.Name):
			add("name must be valid UTF-8 without NUL bytes")
		case name == "":
			add("name must not be blank")
		case utf8.RuneCountInString(name) > maxNameLen:
			add("name must be at most %d characters", maxNameLen)
		}
	}

	if e.PriceMinor == nil {
		add("price_minor is required")
	} else if *e.PriceMinor < 0 {
		add("price_minor must be >= 0")
	}

	if e.Currency == nil {
		add("currency is required")
	} else if !isCleanText(*e.Currency) || !isCurrencyCode(canonicalCurrency(*e.Currency)) {
		add("currency must be a 3-letter code such as USD")
	}
	return problems
}

// recordLabel names a record in problem messages. A product_id that is empty, too long or
// not clean text is replaced by the record's position, so a message built from it can
// always be stored in PostgreSQL.
func recordLabel(e SupplierProduct, index int) string {
	if strings.TrimSpace(e.ProductID) != "" && isCleanText(e.ProductID) && utf8.RuneCountInString(e.ProductID) <= maxProductIDLen {
		return e.ProductID
	}
	return "record #" + strconv.Itoa(index+1)
}

func isCleanText(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

func isCurrencyCode(s string) bool {
	if len(s) != 3 {
		return false
	}
	for i := range len(s) {
		if s[i] < 'A' || s[i] > 'Z' {
			return false
		}
	}
	return true
}

func canonicalCurrency(s string) string { return strings.ToUpper(strings.TrimSpace(s)) }

// Canonicalize returns the form that is compared, hashed and stored: name trimmed,
// currency trimmed and upper-cased, and for a withdrawal all product details dropped
// (they carry no meaning once a product is withdrawn). The input is not modified.
func Canonicalize(p SupplierProduct) SupplierProduct {
	if p.Status == StatusWithdrawn {
		p.Name, p.PriceMinor, p.Currency = nil, nil, nil
		return p
	}
	if p.Name != nil {
		name := strings.TrimSpace(*p.Name)
		p.Name = &name
	}
	if p.Currency != nil {
		currency := canonicalCurrency(*p.Currency)
		p.Currency = &currency
	}
	return p
}

// hashedProduct fixes the field order of the hashed JSON, which is what makes the hash
// deterministic. Nil details are hashed as null, so "absent" and "empty" differ.
type hashedProduct struct {
	SupplierID string  `json:"supplier_id"`
	ProductID  string  `json:"product_id"`
	Status     Status  `json:"status"`
	Revision   int64   `json:"revision"`
	Name       *string `json:"name"`
	PriceMinor *int64  `json:"price_minor"`
	Currency   *string `json:"currency"`
}

// PayloadHash is the hex SHA-256 of the canonical JSON of an event. Events that differ
// only in whitespace, currency case or the details of a withdrawal hash identically.
// The hash is kept for audit; equal-revision comparison uses the fields themselves.
func PayloadHash(p SupplierProduct) string {
	c := Canonicalize(p)
	raw, err := json.Marshal(hashedProduct{
		SupplierID: c.SupplierID,
		ProductID:  c.ProductID,
		Status:     c.Status,
		Revision:   c.Revision,
		Name:       c.Name,
		PriceMinor: c.PriceMinor,
		Currency:   c.Currency,
	})
	if err != nil {
		// Unreachable: the struct holds only strings and integers.
		panic("catalog: marshal canonical product: " + err.Error())
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Decide says what one canonical incoming event does to the stored row (nil = no row yet).
// It is pure, so every revision rule can be tested without a database:
//
//   - a lower revision is stale and a repeat of the same revision and content is a
//     duplicate; both are ignored
//   - the same revision with different content is a conflict that blocks the batch
//   - a higher revision updates the product, or withdraws it
//   - WITHDRAWN is terminal: a higher-revision ACTIVE event for a tombstone is blocked
//     rather than reactivating it, and a higher-revision withdrawal only advances the
//     tombstone's revision so later stale events are still recognised as stale
func Decide(cur *Product, in SupplierProduct) Decision {
	if cur == nil {
		if in.Status == StatusWithdrawn {
			return DecisionCreateTombstone
		}
		return DecisionCreate
	}
	switch {
	case in.Revision < cur.Revision:
		return DecisionIgnoreStale
	case in.Revision == cur.Revision:
		if samePayload(cur, in) {
			return DecisionIgnoreDuplicate
		}
		return DecisionVersionConflict
	}
	// in.Revision > cur.Revision
	switch {
	case cur.Status == StatusWithdrawn && in.Status == StatusActive:
		return DecisionReactivationBlocked
	case cur.Status == StatusWithdrawn:
		return DecisionAdvanceTombstone
	case in.Status == StatusWithdrawn:
		return DecisionWithdraw
	default:
		return DecisionUpdate
	}
}

// samePayload compares the fields that give an equal-revision event its meaning. Two
// withdrawals are the same whatever details they carried; a withdrawal never equals an
// ACTIVE event.
func samePayload(cur *Product, in SupplierProduct) bool {
	if cur.Status != in.Status {
		return false
	}
	if cur.Status == StatusWithdrawn {
		return true
	}
	return equalPtr(cur.Name, in.Name) && equalPtr(cur.PriceMinor, in.PriceMinor) && equalPtr(cur.Currency, in.Currency)
}

func equalPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// canonicalSorted canonicalizes a batch and orders it by (supplier_id, product_id,
// revision). Applying events in that order makes the outcome independent of the order
// the supplier listed them in ([r5, r4] is applied as r4 then r5) and fixes the order in
// which product rows are locked. The sort is stable so equal events keep the supplier's
// order. The input slice is left untouched.
func canonicalSorted(events []SupplierProduct) []SupplierProduct {
	out := make([]SupplierProduct, len(events))
	for i, e := range events {
		out[i] = Canonicalize(e)
	}
	slices.SortStableFunc(out, func(a, b SupplierProduct) int {
		return cmp.Or(
			strings.Compare(a.SupplierID, b.SupplierID),
			strings.Compare(a.ProductID, b.ProductID),
			cmp.Compare(a.Revision, b.Revision),
		)
	})
	return out
}
