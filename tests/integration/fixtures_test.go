package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/DuongCM2004/resilient-catalog-sync/internal/catalog"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/supplier"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/syncjob"
	"github.com/DuongCM2004/resilient-catalog-sync/internal/testutil"
)

// fixDir is the directory of supplier feeds shipped with the service, relative to this package.
const fixDir = "../../fixtures"

// fixSpec is the part of a fixture file this test reads: which fixtures must have run first
// and the outcome the feed is documented to produce.
type fixSpec struct {
	UpdateID string      `json:"update_id"`
	Requires []string    `json:"requires"`
	Expected fixExpected `json:"expected"`
}

type fixExpected struct {
	Status    string `json:"status"`
	Created   int    `json:"created"`
	Updated   int    `json:"updated"`
	Withdrawn int    `json:"withdrawn"`
	Duplicate int    `json:"duplicate"`
	Stale     int    `json:"stale"`
	Issue     string `json:"issue"` // code of the one blocking issue a NEEDS_ATTENTION or FAILED job carries
}

// fixUnknownUpdate has no file by design: the supplier does not know it.
var fixUnknownUpdate = fixSpec{
	UpdateID: "UPDATE-2026-404",
	Expected: fixExpected{Status: string(syncjob.StatusFailed), Issue: string(syncjob.CodeUnknownUpdate)},
}

// fixLoad reads every fixture in the documented order (ascending update id).
func fixLoad(t *testing.T) []fixSpec {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(fixDir, "UPDATE-*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("find fixtures in %s: %v (found %d)", fixDir, err, len(paths))
	}
	slices.Sort(paths)
	specs := make([]fixSpec, len(paths))
	for i, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read fixture %s: %v", path, err)
		}
		if err := json.Unmarshal(raw, &specs[i]); err != nil {
			t.Fatalf("decode fixture %s: %v", path, err)
		}
		if want := filepath.Base(path); want != specs[i].UpdateID+".json" || specs[i].Expected.Status == "" {
			t.Fatalf("fixture %s: update_id %q and expected status %q must be set and match the file name", want, specs[i].UpdateID, specs[i].Expected.Status)
		}
	}
	return specs
}

func TestFixturesMatchExpectedOutcomes(t *testing.T) {
	specs := fixLoad(t)
	if len(specs) < 9 {
		t.Fatalf("found %d fixtures, want at least the 9 documented ones", len(specs))
	}
	pool := testutil.DB(t)
	testutil.Reset(t, pool)
	// One database for all fixtures, run in order: later ones build on what earlier ones left.
	m := flowManager(t, pool, supplier.NewFixtureClient(fixDir, supplier.FaultOptions{}), flowConfig(), syncjob.Hooks{}, nil)
	env := &flowEnv{t: t, pool: pool, m: m}

	for _, spec := range slices.Concat(specs, []fixSpec{fixUnknownUpdate}) {
		t.Run(spec.UpdateID, func(t *testing.T) { fixCheck(env.with(t), spec) })
	}
	t.Run("catalog_after_all_fixtures", func(t *testing.T) { fixCheckCatalog(env.with(t)) })
}

// fixCheck runs one fixture and compares the job with the fixture's expected block.
func fixCheck(e *flowEnv, spec fixSpec) {
	e.t.Helper()
	for _, required := range spec.Requires {
		var status string
		err := e.pool.QueryRow(context.Background(),
			`SELECT status FROM sync_jobs WHERE supplier_update_id = $1`, required).Scan(&status)
		if err != nil || status != string(syncjob.StatusCompleted) {
			e.t.Fatalf("%s requires %s to have completed first; found status %q, err %v", spec.UpdateID, required, status, err)
		}
	}

	job := e.run(spec.UpdateID)

	want := spec.Expected
	got := fixExpected{
		Status: string(job.Status), Created: job.Created, Updated: job.Updated,
		Withdrawn: job.Withdrawn, Duplicate: job.Duplicate, Stale: job.Stale, Issue: want.Issue,
	}
	if got != want {
		e.t.Errorf("job = %+v, want %+v (last_error %v)", got, want, job.LastError)
	}
	e.verify(job)

	issues := e.issues(job.ID)
	if want.Issue == "" {
		for _, iss := range issues {
			if iss.Status == "OPEN" {
				e.t.Errorf("open issue %s on a job that should have none: %s", iss.Code, iss.Message)
			}
		}
		return
	}
	if len(issues) != 1 || string(issues[0].Code) != want.Issue || issues[0].Severity != syncjob.SeverityBlocking || issues[0].Status != "OPEN" {
		e.t.Errorf("issues = %+v, want exactly one OPEN BLOCKING %s", issues, want.Issue)
	}
}

// fixCheckCatalog looks at what the whole run left in the catalog. It proves the rejected
// feeds changed nothing: SKU-100 (feed 005), SKU-103's tombstone (006), SKU-302..304 (004),
// SKU-503 (007) and the batches after the conflicting one (009).
func fixCheckCatalog(e *flowEnv) {
	e.t.Helper()
	type row struct {
		id       string
		status   catalog.Status
		revision int64
	}
	rows := []row{
		{"SKU-101", catalog.StatusActive, 2}, {"SKU-102", catalog.StatusActive, 2}, {"SKU-103", catalog.StatusWithdrawn, 2},
		{"SKU-104", catalog.StatusActive, 3}, {"SKU-105", catalog.StatusActive, 1}, {"SKU-106", catalog.StatusActive, 1},
		{"SKU-900", catalog.StatusWithdrawn, 4},
		{"SKU-301", catalog.StatusActive, 1},
		{"SKU-501", catalog.StatusActive, 1}, {"SKU-502", catalog.StatusActive, 1},
		{"SKU-601", catalog.StatusActive, 1}, {"SKU-602", catalog.StatusActive, 1},
		{"SKU-701", catalog.StatusActive, 1}, {"SKU-702", catalog.StatusActive, 1},
	}
	for n := 201; n <= 210; n++ {
		rows = append(rows, row{fmt.Sprintf("SKU-%d", n), catalog.StatusActive, 1})
	}
	for _, r := range rows {
		testutil.AssertProduct(e.t, e.pool, testutil.SupplierID, r.id, r.status, r.revision)
	}
	for _, id := range []string{"SKU-100", "SKU-302", "SKU-303", "SKU-304", "SKU-503", "SKU-703", "SKU-704", "SKU-705"} {
		e.assertAbsent(id)
	}
	var total int
	if err := e.pool.QueryRow(context.Background(), `SELECT count(*) FROM products`).Scan(&total); err != nil {
		e.t.Fatalf("count products: %v", err)
	}
	if total != len(rows) {
		e.t.Errorf("products = %d, want exactly the %d listed", total, len(rows))
	}
}
