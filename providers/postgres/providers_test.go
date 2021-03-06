package postgres

import (
	"testing"
	"time"

	"github.com/alash3al/goukv"
)

func openDBAndDo(fn func(db goukv.Provider)) error {
	dsn, err := goukv.NewDSN("postgres://postgres:@localhost/tst?table=default")
	if err != nil {
		return err
	}
	p := Provider{}

	db, err := p.Open(dsn)
	if err != nil {
		return err
	}

	defer func() {
		db.Close()
	}()

	fn(db)

	return nil
}

func TestPutGet(t *testing.T) {
	err := openDBAndDo(func(db goukv.Provider) {
		entry := goukv.Entry{
			Key:   []byte("k"),
			Value: []byte("v"),
		}
		err := db.Put(&entry)
		if err != nil {
			t.Error(err)
		}
		val, err := db.Get(entry.Key)
		if err != nil {
			t.Error(err)
		}
		if string(val) != string(entry.Value) {
			t.Errorf("expected (%s), found(%s)", string(entry.Value), string(entry.Value))
		}
	})

	if err != nil {
		t.Error(err.Error())
	}
}

func TestPutGetInvalidKey(t *testing.T) {
	err := openDBAndDo(func(db goukv.Provider) {
		entry := goukv.Entry{
			Key:   []byte("kNotFound"),
			Value: []byte("v"),
		}

		_, err := db.Get(entry.Key)
		if err == nil {
			t.Error("expected an error!")
			return
		}
		if err != goukv.ErrKeyNotFound {
			t.Errorf("expected (%s), found(%s)", goukv.ErrKeyNotFound.Error(), err.Error())
		}
	})

	if err != nil {
		t.Error(err.Error())
	}
}

func TestTTL(t *testing.T) {
	err := openDBAndDo(func(db goukv.Provider) {
		entry := goukv.Entry{
			Key:   []byte("k"),
			Value: []byte("v"),
			TTL:   time.Second * 10,
		}
		err := db.Put(&entry)
		if err != nil {
			t.Error(err)
		}
		expiresAt, err := db.TTL(entry.Key)
		if err != nil {
			t.Error(err)
		}
		if !(expiresAt.Before(time.Now().Add(entry.TTL)) || expiresAt.Equal(time.Now().Add(entry.TTL))) {
			t.Errorf("expected to be expires <= (%d), found (%d)", time.Now().Add(entry.TTL).Unix(), expiresAt.Unix())
		}
	})

	if err != nil {
		t.Error(err.Error())
	}
}
