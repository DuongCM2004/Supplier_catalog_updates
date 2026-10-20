package supplier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// updateIDPattern is the only shape of update id the client turns into a file name. It has
// no path separators, so an id can never leave the fixtures directory.
var updateIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// FaultOptions switch on supplier misbehaviour for demos and tests. The zero value means a
// healthy supplier.
type FaultOptions struct {
	// OutageCursors always fail with ErrUnavailable, for as long as this client lives, so a
	// demo can "repair" the supplier simply by restarting without the setting.
	OutageCursors map[string]bool
	// Delay is slept before every batch fetch, which gives a demo time to kill the process
	// between batches. It ends early when the caller's context ends.
	Delay time.Duration
}

// FixtureClient serves updates from <dir>/<UPDATE_ID>.json files.
type FixtureClient struct {
	dir  string
	opts FaultOptions
}

// NewFixtureClient returns a client reading update files from dir.
func NewFixtureClient(dir string, opts FaultOptions) *FixtureClient {
	return &FixtureClient{dir: dir, opts: opts}
}

// fixtureFile is the part of a fixture file the client serves; the description, requires and
// expected blocks are for humans and tests and are ignored here.
type fixtureFile struct {
	UpdateID     string        `json:"update_id"`
	SupplierID   string        `json:"supplier_id"`
	TotalBatches *int          `json:"total_batches"`
	Pages        []fixturePage `json:"pages"`
}

type fixturePage struct {
	Cursor string `json:"cursor"`
	Batch  Batch  `json:"batch"`
}

var _ Client = (*FixtureClient)(nil)

// GetUpdate describes the update without returning any page.
func (c *FixtureClient) GetUpdate(ctx context.Context, updateID string) (UpdateInfo, error) {
	if err := ctx.Err(); err != nil {
		return UpdateInfo{}, fmt.Errorf("supplier get update %s: %w", updateID, err)
	}
	f, err := c.loadFixture(updateID)
	if err != nil {
		return UpdateInfo{}, err
	}
	return UpdateInfo{UpdateID: f.UpdateID, SupplierID: f.SupplierID, TotalBatches: f.TotalBatches}, nil
}

// FetchBatch returns the page stored under exactly this cursor ("" is the first page). A
// batch is returned whole or not at all.
func (c *FixtureClient) FetchBatch(ctx context.Context, updateID, cursor string) (Batch, error) {
	if err := ctx.Err(); err != nil {
		return Batch{}, fmt.Errorf("supplier fetch %s at %q: %w", updateID, cursor, err)
	}
	// Outage is decided before touching the file: a down supplier answers nothing.
	if c.opts.OutageCursors[cursor] {
		return Batch{}, fmt.Errorf("%w: simulated outage at cursor %q", ErrUnavailable, cursor)
	}
	if c.opts.Delay > 0 {
		timer := time.NewTimer(c.opts.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return Batch{}, fmt.Errorf("supplier fetch %s at %q: %w", updateID, cursor, ctx.Err())
		case <-timer.C:
		}
	}
	f, err := c.loadFixture(updateID)
	if err != nil {
		return Batch{}, err
	}
	for _, p := range f.Pages {
		if p.Cursor == cursor {
			return p.Batch, nil
		}
	}
	return Batch{}, fmt.Errorf("%w: update %s has no page for cursor %q", ErrNotFound, updateID, cursor)
}

// loadFixture reads and decodes the file on every call, without caching, so tests and demos
// can change a fixture between calls to model a supplier correcting its data.
func (c *FixtureClient) loadFixture(updateID string) (fixtureFile, error) {
	if !updateIDPattern.MatchString(updateID) {
		return fixtureFile{}, fmt.Errorf("%w: invalid update id", ErrNotFound)
	}
	raw, err := os.ReadFile(filepath.Join(c.dir, updateID+".json"))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fixtureFile{}, fmt.Errorf("%w: unknown update %s", ErrNotFound, updateID)
	case err != nil:
		// Other read failures say nothing about the data, so they are worth retrying.
		return fixtureFile{}, fmt.Errorf("%w: read fixture %s: %v", ErrUnavailable, updateID, err)
	}
	var f fixtureFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return fixtureFile{}, fmt.Errorf("%w: decode fixture %s: %w", ErrMalformed, updateID, err)
	}
	return f, nil
}
