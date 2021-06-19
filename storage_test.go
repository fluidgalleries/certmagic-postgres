package certmagic_postgres_test

import (
	"context"
	"database/sql"
	"github.com/caddyserver/certmagic"
	"github.com/fluidgalleries/certmagic-postgres"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestStorage_Connect(t *testing.T) {
	_, err := certmagic_postgres.Connect(getConnectionString(t))
	assert.Nil(t, err)
}

func TestStorage_Lock(t *testing.T) {
	tt := []struct {
		name              string
		key               string
		existingLockedKey string
		lockExpiry        string
		sleepDuration     time.Duration
		isLockedErr       bool
	}{
		{
			name:              "can lock a key",
			key:               "abcd",
			existingLockedKey: "1234",
			lockExpiry:        "1m",
			sleepDuration:     time.Duration(0),
			isLockedErr:       false,
		},
		{
			name:              "cannot lock a locked key",
			key:               "abcd",
			existingLockedKey: "abcd",
			lockExpiry:        "1m",
			sleepDuration:     time.Duration(0),
			isLockedErr:       true,
		},
		{
			name:              "can lock an expired locked key",
			key:               "abcd",
			existingLockedKey: "abcd",
			lockExpiry:        "50ms",
			sleepDuration:     time.Millisecond * 100,
			isLockedErr:       false,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			db, teardown := setupDB(t)
			defer teardown()

			storage, err := certmagic_postgres.Open(db, certmagic_postgres.WithLockTimeout(tc.lockExpiry))
			if err != nil {
				t.Fatal(err)
			}

			err = storage.Lock(context.Background(), tc.existingLockedKey)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(tc.sleepDuration)

			err = storage.Lock(context.Background(), tc.key)
			isLockedError := err != nil
			assert.Equal(t, tc.isLockedErr, isLockedError)
		})
	}
}

func TestStorage_Unlock(t *testing.T) {
	db, teardown := setupDB(t)
	defer teardown()

	storage, err := certmagic_postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Lock(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Unlock("abc")
	assert.Nil(t, err)
}

func TestStorage_Store(t *testing.T) {
	db, teardown := setupDB(t)
	defer teardown()

	storage, err := certmagic_postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}

	value := []byte("value")
	err = storage.Store("abc", value)
	if err != nil {
		t.Fatal(err)
	}

	valueGot, err := storage.Load("abc")
	assert.Equal(t, value, valueGot)
	assert.Nil(t, err)
}

func TestStorage_Load(t *testing.T) {
	db, teardown := setupDB(t)
	defer teardown()

	storage, err := certmagic_postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}

	value := []byte("value")
	err = storage.Store("abc", value)
	if err != nil {
		t.Fatal(err)
	}

	valueGot, err := storage.Load("abc")
	require.Nil(t, err)
	require.Equal(t, value, valueGot)

	_, err = storage.Load("bad-key")
	_, isErrNotExist := err.(certmagic.ErrNotExist)
	assert.True(t, isErrNotExist)
}

func TestStorage_Delete(t *testing.T) {
	db, teardown := setupDB(t)
	defer teardown()

	storage, err := certmagic_postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Store("abc", []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Delete("abc")
	assert.Nil(t, err)
}

func TestStorage_Exists(t *testing.T) {
	db, teardown := setupDB(t)
	defer teardown()

	storage, err := certmagic_postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Store("abc", []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, true, storage.Exists("abc"))
	assert.Equal(t, false, storage.Exists("xyz"))
}

func TestStorage_List(t *testing.T) {
	db, teardown := setupDB(t)
	defer teardown()

	storage, err := certmagic_postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}

	_ = storage.Store("abc", []byte("value"))
	_ = storage.Store("abcde", []byte("value"))
	_ = storage.Store("abcdefg", []byte("value"))
	_ = storage.Store("xyz", []byte("value"))
	_ = storage.Store("xyz123", []byte("value"))

	keys, err := storage.List("abc", false)
	assert.Nil(t, err)
	assert.Len(t, keys, 3)
	assert.Equal(t, []string{"abc", "abcde", "abcdefg"}, keys)
}

func TestStorage_Stat(t *testing.T) {
	db, teardown := setupDB(t)
	defer teardown()

	storage, err := certmagic_postgres.Open(db)
	if err != nil {
		t.Fatal(err)
	}

	err = storage.Store("abc", []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	keyInfo, err := storage.Stat("abc")
	assert.Nil(t, err)
	assert.Equal(t, "abc", keyInfo.Key)
	assert.Equal(t, int64(5), keyInfo.Size)
	assert.NotZero(t, keyInfo.Modified)
	assert.True(t, keyInfo.IsTerminal)
}

// Set an env var TEST_CONNECTION_STRING to run these tests - e.g. TEST_CONNECTION_STRING=postgres://localhost/norris_sites_test?sslmode=disable

func getConnectionString(t *testing.T) string {
	connectionString := os.Getenv("TEST_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("set TEST_CONNECTION_STRING to run this test")
	}
	return connectionString
}

func setupDB(t *testing.T) (*sql.DB, func()) {
	connectionString := getConnectionString(t)

	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		t.Fatal(err)
	}

	executeSQL(t, db, "./db/20200721125602_baseline.down.sql")
	executeSQL(t, db, "./db/20200721125602_baseline.up.sql")

	teardown := func() {
		executeSQL(t, db, "./db/20200721125602_baseline.down.sql")
	}

	return db, teardown
}

func executeSQL(t *testing.T, db *sql.DB, path string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, string(query))
	if err != nil {
		t.Fatal(err)
	}
}
