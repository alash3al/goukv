package goukv

import (
	"net/url"
	"strconv"
)

// DSN data source name
type DSN struct {
	url *url.URL
	dsn string
}

// NewDSN initializes a new dsn by parsing the specified dsn
func NewDSN(dsn string) (*DSN, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	return &DSN{
		dsn: dsn,
		url: u,
	}, nil
}

// Scheme returns the dsn scheme
func (dsn DSN) Scheme() string {
	return dsn.url.Scheme
}

// Hostname returns the dsn hostname
func (dsn DSN) Hostname() string {
	return dsn.url.Hostname()
}

// Port returns the dsn port number as string
func (dsn DSN) Port() string {
	return dsn.url.Port()
}

// Path returns the dsn path
func (dsn DSN) Path() string {
	return dsn.url.Path
}

// Username returns the dsn username
func (dsn DSN) Username() string {
	if dsn.url.User == nil {
		return ""
	}

	return dsn.url.User.Username()
}

// Password returns the password
func (dsn DSN) Password() string {
	if dsn.url.User == nil {
		return ""
	}

	pass, _ := dsn.url.User.Password()

	return pass
}

// GetString fetches an option from the options as string
func (dsn DSN) GetString(key string) string {
	return dsn.url.Query().Get(key)
}

// GetInt returns the value of the key as int
func (dsn DSN) GetInt(key string) int {
	i, _ := strconv.Atoi(dsn.GetString(key))
	return i
}

// GetInt returns the value of the key as int64
func (dsn DSN) GetInt64(key string) int64 {
	i, _ := strconv.ParseInt(dsn.GetString(key), 10, 64)
	return i
}

// GetInt returns the value of the key as float64
func (dsn DSN) GetFloat(key string) float64 {
	f, _ := strconv.ParseFloat(dsn.GetString(key), 64)
	return f
}

// GetInt returns the value of the key as bool
func (dsn DSN) GetBool(key string) bool {
	b, _ := strconv.ParseBool(dsn.GetString(key))
	return b
}
