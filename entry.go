package goukv

import "time"

// Entry represents a key - value pair
type Entry struct {
	Key   []byte
	Value []byte
	TTL   time.Duration
}
