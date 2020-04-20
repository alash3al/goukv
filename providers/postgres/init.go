package postgres

import "github.com/alash3al/goukv"

const (
	name = "postgres"
)

func init() {
	goukv.Register(name, Provider{})
}
