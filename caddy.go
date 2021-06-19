package certmagic_postgres

import (
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

type CaddyStorage struct {
	ConnectionString string `json:"connection_string"`
	QueryTimeout     string `json:"query_timeout"`
	LockTimeout      string `json:"lock_timeout"`
	storage          Storage
}

func init() {
	caddy.RegisterModule(CaddyStorage{})
}

// CaddyModule returns the Caddy module information.
func (CaddyStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.storage.postgres",
		New: func() caddy.Module { return new(CaddyStorage) },
	}
}

// Provision configures a new Storage instance using config values obtained from Caddy config
func (s *CaddyStorage) Provision(caddy.Context) error {
	var options []Option
	if s.QueryTimeout != "" {
		options = append(options, WithQueryTimeout(s.QueryTimeout))
	}
	if s.LockTimeout != "" {
		options = append(options, WithLockTimeout(s.LockTimeout))
	}

	var err error
	s.storage, err = Connect(s.ConnectionString, options...)
	return err
}

// UnmarshalCaddyfile sets up the Storage from Caddyfile tokens. Syntax:
//
// postgres [<connection_string>] {
//     connection_string <connection_string>
// }
//
// Expansion of placeholders in the API token is left to the JSON config caddy.Provisioner (above).
func (s *CaddyStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if d.NextArg() {
			s.ConnectionString = d.Val()
		}
		if d.NextArg() {
			return d.ArgErr()
		}
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			switch d.Val() {
			case "connection_string":
				if s.ConnectionString != "" {
					return d.Err("ConnectionString already set")
				}
				if !d.AllArgs(&s.ConnectionString) {
					return d.ArgErr()
				}

			case "query_timeout":
				if s.QueryTimeout != "" {
					return d.Err("QueryTimeout already set")
				}
				if !d.AllArgs(&s.QueryTimeout) {
					return d.ArgErr()
				}

			case "lock_timeout":
				if s.LockTimeout != "" {
					return d.Err("LockTimeout already set")
				}
				if !d.AllArgs(&s.LockTimeout) {
					return d.ArgErr()
				}

			default:
				return d.Errf("unrecognized subdirective '%s'", d.Val())
			}
		}
	}
	if s.ConnectionString == "" {
		return d.Err("missing ConnectionString token")
	}
	return nil
}

// CertMagicStorage objects a Storage instance from a CaddyStorage instance
func (s *CaddyStorage) CertMagicStorage() (certmagic.Storage, error) {
	return s.storage, nil
}

func (s *CaddyStorage) Cleanup() error {
	return s.storage.Close()
}

// Interface guards
var (
	_ caddyfile.Unmarshaler  = (*CaddyStorage)(nil)
	_ caddy.StorageConverter = (*CaddyStorage)(nil)
	_ caddy.Provisioner      = (*CaddyStorage)(nil)
	_ caddy.CleanerUpper     = (*CaddyStorage)(nil)
)
