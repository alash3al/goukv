package postgres

import "time"

type Item struct {
	K []byte `db:"_k"`
	V []byte `db:"_v"`
	X int64  `db:"_x"`

	ID int64 `db:"_id"`
}

func (i Item) ExpiresAt() time.Time {
	return time.Unix(i.X, 0)
}

func (i Item) Expired() bool {
	println(i.X)
	if i.X < 1 {
		return false
	}

	expiresAt := i.ExpiresAt()
	now := time.Now()

	return now.After(expiresAt) || now.Equal(expiresAt)
}
