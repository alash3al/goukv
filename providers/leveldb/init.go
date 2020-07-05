package leveldb

import "github.com/alash3al/goukv"

const (
	name = "leveldb"
)

func init() {
	goukv.Register(name, Provider{})
}
