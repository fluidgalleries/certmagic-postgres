package certmagic_postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/caddyserver/certmagic"
	_ "github.com/jackc/pgx/v4/stdlib"
	"time"
)

type Option = func(Storage) (Storage, error)

func WithQueryTimeout(timeout string) Option {
	return func(storage Storage) (Storage, error) {
		queryTimeout, err := time.ParseDuration(timeout)
		if err != nil {
			return storage, fmt.Errorf("invalid query timeout: %w", err)
		}
		storage.queryTimeout = queryTimeout
		return storage, nil
	}
}

func WithLockTimeout(timeout string) Option {
	return func(storage Storage) (Storage, error) {
		lockTimeout, err := time.ParseDuration(timeout)
		if err != nil {
			return storage, fmt.Errorf("invalid lock timeout: %w", err)
		}
		storage.lockTimeout = lockTimeout
		return storage, nil
	}
}

type Storage struct {
	db           *sql.DB
	queryTimeout time.Duration
	lockTimeout  time.Duration
}

func Connect(connectionString string, options ...Option) (Storage, error) {
	// Open database connection
	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		return Storage{}, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Ping database
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		return Storage{}, fmt.Errorf("failed to ping database: %w", err)
	}

	storage := Storage{
		db:           db,
		queryTimeout: time.Second * 3,
		lockTimeout:  time.Minute * 1,
	}

	for _, option := range options {
		storage, err = option(storage)
		if err != nil {
			return Storage{}, err
		}
	}

	return storage, nil
}

func Open(db *sql.DB, options ...Option) (Storage, error) {
	storage := Storage{
		db:           db,
		queryTimeout: time.Second * 3,
		lockTimeout:  time.Minute * 1,
	}

	for _, option := range options {
		var err error
		storage, err = option(storage)
		if err != nil {
			return Storage{}, err
		}
	}

	return storage, nil
}

// Implement CertMagic.Storage Interface
//
// Lock acquires the lock for key, blocking until the lock
// can be obtained or an error is returned. Note that, even
// after acquiring a lock, an idempotent operation may have
// already been performed by another process that acquired
// the lock before - so always check to make sure idempotent
// operations still need to be performed after acquiring the
// lock.
//
// The actual implementation of obtaining of a lock must be
// an atomic operation so that multiple Lock calls at the
// same time always results in only one caller receiving the
// lock at any given time.
//
// To prevent deadlocks, all implementations (where this concern
// is relevant) should put a reasonable expiration on the lock in
// case Unlock is unable to be called due to some sort of network
// failure or system crash. Additionally, implementations should
// honor context cancellation as much as possible (in case the
// caller wishes to give up and free resources before the lock
// can be obtained).
func (s Storage) Lock(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check if a lock on the key exists
	row := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM certmagic_locks WHERE key = $1 AND expires > CURRENT_TIMESTAMP)`, key)
	var isLocked bool
	if err = row.Scan(&isLocked); err != nil {
		return fmt.Errorf("failed scan: %w", err)
	}

	if isLocked {
		return fmt.Errorf("key %s is already locked", key)
	}

	expires := time.Now().Add(s.lockTimeout)
	if _, err := tx.ExecContext(ctx, `INSERT INTO certmagic_locks (key, expires) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET expires = $2`, key, expires); err != nil {
		return fmt.Errorf("failed to lock key: %s: %w", key, err)
	}

	return tx.Commit()
}

// Unlock releases the lock for key. This method must ONLY be
// called after a successful call to Lock, and only after the
// critical section is finished, even if it errored or timed
// out. Unlock cleans up any resources allocated during Lock.
func (s Storage) Unlock(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx, `DELETE FROM certmagic_locks WHERE key = $1`, key)
	return err
}

// Store puts value at key.
func (s Storage) Store(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx, `INSERT INTO certmagic_data (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET VALUE = $2, modified = CURRENT_TIMESTAMP`, key, value)
	if err != nil {
		return fmt.Errorf("failed exec: %w", err)
	}

	return nil
}

// Load retrieves the value at key.
func (s Storage) Load(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	var value []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM certmagic_data WHERE key = $1`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, certmagic.ErrNotExist(fmt.Errorf("key not found: %s", key))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query row: %w", err)
	}

	return value, nil
}

// Delete deletes key. An error should be
// returned only if the key still exists
// when the method returns.
func (s Storage) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	_, err := s.db.ExecContext(ctx, "DELETE FROM certmagic_data WHERE key = $1", key)
	if err != nil {
		return fmt.Errorf("failed exec: %w", err)
	}

	return nil
}

// Exists returns true if the key exists
// and there was no error checking.
func (s Storage) Exists(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	row := s.db.QueryRowContext(ctx, "select exists(select 1 from certmagic_data where key = $1)", key)
	var exists bool
	err := row.Scan(&exists)
	return err == nil && exists
}

// List returns all keys that match prefix.
// If recursive is true, non-terminal keys
// will be enumerated (i.e. "directories"
// should be walked); otherwise, only keys
// prefixed exactly by prefix will be listed.
func (s Storage) List(prefix string, recursive bool) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	if recursive {
		return nil, fmt.Errorf("recursive not supported")
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT key FROM certmagic_data WHERE key LIKE '%s%%'`, prefix))
	if err != nil {
		return nil, fmt.Errorf("failed query: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("failed scan: %w", err)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Stat returns information about key.
func (s Storage) Stat(key string) (certmagic.KeyInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.queryTimeout)
	defer cancel()

	var modified time.Time
	var size int64
	row := s.db.QueryRowContext(ctx, `SELECT LENGTH (value), modified FROM certmagic_data WHERE key = $1`, key)
	err := row.Scan(&size, &modified)
	if err != nil {
		return certmagic.KeyInfo{}, fmt.Errorf("failed scan: %w", err)
	}

	keyInfo := certmagic.KeyInfo{
		Key:        key,
		Modified:   modified,
		Size:       size,
		IsTerminal: true,
	}
	return keyInfo, nil
}

func (s Storage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Interface guards
var (
	_ certmagic.Storage = (*Storage)(nil)
)
