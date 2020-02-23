package leveldb

import "github.com/alash3al/goukv"

const (
	name = "goleveldb"
)

func init() {
	goukv.Register(name, Provider{})
}
