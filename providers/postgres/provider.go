package postgres

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alash3al/goukv"
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

// Provider represents a driver
type Provider struct {
	db    *sqlx.DB
	l     *sync.RWMutex
	table string
}

// Open implements goukv.Open
func (p Provider) Open(dsn *goukv.DSN) (goukv.Provider, error) {
	driverDSN := fmt.Sprintf("postgres://%s:%s@%s:%s%s", dsn.Username(), dsn.Password(), dsn.Hostname(), dsn.Port(), dsn.Path())
	db, err := sqlx.Connect("postgres", driverDSN)
	if err != nil {
		return nil, err
	}

	table := dsn.GetString("table")

	if _, err := db.Exec(`
		CREATE EXTENSION IF NOT EXISTS pg_trgm;

		CREATE TABLE IF NOT EXISTS ` + (table) + ` (
			_id SERIAL PRIMARY KEY,
			_k 	VARCHAR,
			_v  JSONB,
			_x  BIGINT DEFAULT 0
		);

		CREATE UNIQUE INDEX IF NOT EXISTS idx_` + (table) + `_k ON ` + (table) + `(_k);
		CREATE INDEX IF NOT EXISTS idx_gintrgm_` + (table) + `_k ON ` + (table) + ` USING GIN(_k gin_trgm_ops);
	`); err != nil {
		return nil, err
	}

	return &Provider{
		db:    db,
		l:     &sync.RWMutex{},
		table: table,
	}, nil
}

// Put implements goukv.Put
func (p Provider) Put(e *goukv.Entry) error {
	item := Item{
		K: e.Key,
		V: e.Value,
		X: 0,
	}

	if e.TTL > 0 {
		item.X = time.Now().Add(e.TTL).Unix()
	}

	query := `
		INSERT INTO ` + (p.table) + `(_k, _v, _x) VALUES(:_k, :_v, :_x)
		ON CONFLICT (_k) DO UPDATE
			SET _v = :_v,
				_x = :_x
	`
	_, err := p.db.NamedExec(query, item)

	return err
}

func (p Provider) Incr(k []byte, delta float64) (float64, error) {
	p.l.Lock()
	defer p.l.Unlock()

	ttl, err := p.TTL(k)
	if err != goukv.ErrKeyNotFound && err != nil {
		return 0, err
	}

	val, err := p.Get(k)
	if err != goukv.ErrKeyNotFound && err != nil {
		return 0, err
	}

	var valAsFloat float64

	if val == nil {
		valAsFloat = 0
	} else {
		parsedVal, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return 0, err
		}
		valAsFloat = parsedVal
	}

	valAsFloat += delta

	var newttl time.Duration
	if ttl != nil {
		newttl = time.Until(*ttl)
	}

	return valAsFloat, p.Put(&goukv.Entry{
		Key:   k,
		Value: []byte(fmt.Sprintf("%f", valAsFloat)),
		TTL:   newttl,
	})
}

// Get implements goukv.Get
func (p Provider) Get(k []byte) ([]byte, error) {
	var item Item

	err := p.db.Get(&item, `SELECT * FROM `+(p.table)+` WHERE _k = $1`, k)
	if err == sql.ErrNoRows {
		return nil, goukv.ErrKeyNotFound
	}

	if err != nil {
		return nil, err
	}

	if item.Expired() {
		return nil, goukv.ErrKeyExpired
	}

	return item.V, nil
}

// TTL implements goukv.TTL
func (p Provider) TTL(k []byte) (*time.Time, error) {
	var item Item

	err := p.db.Get(&item, `SELECT * FROM `+(p.table)+` WHERE _k = $1`, k)
	if err == sql.ErrNoRows {
		return nil, goukv.ErrKeyNotFound
	}

	if err != nil {
		return nil, err
	}

	if item.X > 0 {
		expiresAt := item.ExpiresAt()
		return &expiresAt, nil
	}

	return nil, nil
}

// Delete implements goukv.Delete
func (p Provider) Delete(k []byte) error {
	_, err := p.db.Exec(`DELETE FROM `+(p.table)+` WHERE _k = $1`, k)
	return err
}

// Batch perform multi put operation, empty value means *delete*
func (p Provider) Batch(entries []*goukv.Entry) error {
	errStrs := []string{}

	for _, entry := range entries {
		if entry.Value == nil {
			if err := p.Delete(entry.Key); err != nil {
				errStrs = append(errStrs, fmt.Sprintf("%s: %s", string(entry.Key), err.Error()))
			}
		} else {
			if err := p.Put(entry); err != nil {
				errStrs = append(errStrs, fmt.Sprintf("%s: %s", string(entry.Key), err.Error()))
			}
		}
	}

	if len(errStrs) > 0 {
		return errors.New(strings.Join(errStrs, ", "))
	}

	return nil
}

// Close implements goukv.Close
func (p Provider) Close() error {
	return p.db.Close()
}

// Scan implements goukv.Scan
func (p Provider) Scan(opts goukv.ScanOpts) error {
	if opts.Scanner == nil {
		return nil
	}

	query := `SELECT * FROM ` + (p.table) + ``
	where := []string{}
	sortOrder := "ASC"
	args := []interface{}{}

	if opts.ReverseScan {
		sortOrder = "DESC"
	}

	if len(opts.Offset) > 0 {
		where = append(where, `_id >= (SELECT _id FROM `+(p.table)+` WHERE _k = $1)`)
		args = append(args, string(opts.Offset))
	}

	if len(opts.Prefix) > 0 {
		where = append(where, `_k LIKE $2`)
		args = append(args, string(opts.Prefix)+"%")
	}

	if len(where) > 0 {
		query += " WHERE (" + strings.Join(where, ") AND (") + ")"
	}

	query += " ORDER BY _id " + sortOrder

	rows, err := p.db.Queryx(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var item Item

		if err := rows.StructScan(&item); err != nil {
			return err
		}

		if item.Expired() {
			continue
		}

		if !opts.IncludeOffset && bytes.Equal(opts.Offset, item.K) {
			continue
		}

		if !opts.Scanner(item.K, item.V) {
			break
		}
	}

	return nil
}
