package goukv

import (
	"sync"
	"time"
)

// providers a providers for available drivers
var (
	providersMap  = map[string]Provider{}
	providersLock = &sync.RWMutex{}
)

// Provider an interface describes a storage backend
type Provider interface {
	Open(map[string]interface{}) (Provider, error)
	Put(*Entry) error
	Get([]byte) ([]byte, error)
	TTL([]byte) (*time.Time, error)
	Delete([]byte) error
	Batch([]*Entry) error
	Scan(ScanOpts) error
	Close() error
}

// Register register a new driver
func Register(name string, provider Provider) error {
	providersLock.Lock()
	defer providersLock.Unlock()

	if providersMap[name] != nil {
		return ErrDriverAlreadyExists
	}

	providersMap[name] = provider

	return nil
}

// Get returns a driver from the registery
func Get(providerName string) (Provider, error) {
	providersLock.Lock()
	defer providersLock.Unlock()

	if providersMap[providerName] == nil {
		return nil, ErrDriverNotFound
	}

	return providersMap[providerName], nil
}

// Open initialize the specified provider and returns its instance
func Open(providerName string, opts map[string]interface{}) (Provider, error) {
	providerInterface, err := Get(providerName)
	if err != nil {
		return nil, err
	}

	return providerInterface.Open(opts)
}
