package postgres

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alash3al/goukv"
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

// Provider represents a driver
type Provider struct {
	db *sqlx.DB
}

// Open implements goukv.Open
func (p Provider) Open(opts map[string]interface{}) (goukv.Provider, error) {
	driverDSN, ok := opts["dsn"].(string)
	if !ok {
		return nil, errors.New("invalid sql DSN specified")
	}

	db, err := sqlx.Connect("postgres", driverDSN)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(`
		CREATE EXTENSION IF NOT EXISTS pg_trgm;

		CREATE TABLE IF NOT EXISTS goukv_store (
			_id SERIAL PRIMARY KEY,
			_k 	VARCHAR,
			_v  TEXT,
			_x  BIGINT DEFAULT 0
		);

		CREATE UNIQUE INDEX IF NOT EXISTS idx_goukvstore_k ON goukv_store(_k);
		CREATE INDEX IF NOT EXISTS idx_gintrgm_goukvstore_k ON goukv_store USING GIN(_k gin_trgm_ops);
	`); err != nil {
		return nil, err
	}

	return &Provider{
		db: db,
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
		INSERT INTO goukv_store(_k, _v, _x) VALUES(:_k, :_v, :_x)
		ON CONFLICT (_k) DO UPDATE
			SET _v = :_v,
				_x = :_x
	`
	_, err := p.db.NamedExec(query, item)

	return err
}

// Get implements goukv.Get
func (p Provider) Get(k []byte) ([]byte, error) {
	var item Item

	err := p.db.Get(&item, `SELECT * FROM goukv_store WHERE _k = $1`, k)
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

	err := p.db.Get(&item, `SELECT * FROM goukv_store WHERE _k = $1`, k)
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
	_, err := p.db.Exec(`DELETE FROM goukv_store WHERE _k = $1`, k)
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

	query := `SELECT * FROM goukv_store`
	where := []string{}
	sortOrder := "ASC"
	args := []interface{}{}

	if opts.ReverseScan {
		sortOrder = "DESC"
	}

	if len(opts.Offset) > 0 {
		where = append(where, `_id >= (SELECT _id FROM goukv_store WHERE _k = $1)`)
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
