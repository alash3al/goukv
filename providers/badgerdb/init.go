package badgerdb

import (
	"github.com/alash3al/goukv"
)

const (
	name = "badgerdb"
)

func init() {
	goukv.Register(name, Provider{})
}
