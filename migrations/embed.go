// Package migrations embeds and applies the golang-migrate SQL migrations of
// voice_service.
package migrations

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// FS holds the embedded SQL migration files.
//
//go:embed *.sql
var FS embed.FS

// Up applies every pending migration to databaseURL. The URL may use the
// postgres://, postgresql://, or pgx5:// scheme.
func Up(databaseURL string) error {
	source, err := iofs.New(FS, ".")
	if err != nil {
		return fmt.Errorf("migrations: open embedded source: %w", err)
	}
	migrator, err := migrate.NewWithSourceInstance("iofs", source, normalizeURL(databaseURL))
	if err != nil {
		return fmt.Errorf("migrations: open database: %w", err)
	}
	defer func() {
		sourceErr, dbErr := migrator.Close()
		if sourceErr != nil && dbErr != nil {
			_ = sourceErr
		}
	}()
	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrations: apply: %w", err)
	}
	return nil
}

// normalizeURL rewrites the PostgreSQL scheme to the one registered by the
// golang-migrate pgx/v5 driver.
func normalizeURL(databaseURL string) string {
	switch {
	case strings.HasPrefix(databaseURL, "postgres://"):
		return "pgx5://" + strings.TrimPrefix(databaseURL, "postgres://")
	case strings.HasPrefix(databaseURL, "postgresql://"):
		return "pgx5://" + strings.TrimPrefix(databaseURL, "postgresql://")
	default:
		return databaseURL
	}
}
