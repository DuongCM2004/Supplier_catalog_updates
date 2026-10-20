// Package migrations embeds the versioned SQL schema files applied at startup.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
