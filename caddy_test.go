package certmagic_postgres

import (
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCaddyStorage_UnmarshalCaddyfile(t *testing.T) {
	tt := []struct {
		name             string
		api              string
		connectionString string
		queryTimeout     string
		lockTimeout      string
	}{
		{
			name:             "inline",
			api:              `postgres myConnectionString`,
			connectionString: "myConnectionString",
		},
		{
			name: "block",
			api: `postgres { 
						connection_string myConnectionString
						query_timeout 3s
						lock_timeout 60s
					}`,
			connectionString: "myConnectionString",
			queryTimeout:     "3s",
			lockTimeout:      "60s",
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			dispencer := caddyfile.NewTestDispenser(tc.api)
			caddyStorage := &CaddyStorage{}
			err := caddyStorage.UnmarshalCaddyfile(dispencer)
			if err != nil {
				t.Fatal(err)
			}

			assert.Equal(t, tc.connectionString, caddyStorage.ConnectionString)
			assert.Equal(t, tc.queryTimeout, caddyStorage.QueryTimeout)
			assert.Equal(t, tc.lockTimeout, caddyStorage.LockTimeout)
		})
	}
}
