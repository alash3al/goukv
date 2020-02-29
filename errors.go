package goukv

import "errors"

// error related variables
var (
	ErrDriverAlreadyExists = errors.New("the specified driver name is already exisrs")
	ErrDriverNotFound      = errors.New("the requested driver isn't found")
	ErrKeyExpired          = errors.New("the specified key is expired")
	ErrKeyNotFound         = errors.New("the specified key couldn't be found")
)
