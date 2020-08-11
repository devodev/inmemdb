package inmemdb

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrKeyEmpty is returned when the provided key is an empty string.
	ErrKeyEmpty = errors.New("key is empty")
	// ErrKeyNotFound is returned when the provided key
	// is not found in the database.
	ErrKeyNotFound = errors.New("key not found")
)

// Entry represents a database entry containing
// a value as well as meaningful data
// used in transaction processing.
type Entry struct {
	Value string
}

// NewEntry creates a new database entry
// initialized using the provided value.
func NewEntry(value string) *Entry {
	return &Entry{Value: value}
}

// Database is an in-memory database.
//
// Database provides a key-value store abstraction
// with support for the `read commited` isolation level only.
//
// Database is safe for concurrent use.
type Database struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

// NewDatabase creates a new Database.
func NewDatabase() *Database {
	db := &Database{
		data: make(map[string]*Entry),
	}
	return db
}

// Put sets the provided key to value.
func (d *Database) Put(key string, value string) error {
	if key == "" {
		return ErrKeyEmpty
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.data[key] = NewEntry(value)
	return nil
}

// PutTxn sets the provided key to value
// within an existing transaction using the provided transaction ID.
func (d *Database) PutTxn(key string, value string, txnID string) error {
	return fmt.Errorf("not implemented")
}

// Get returns the value associated with the provided key.
func (d *Database) Get(key string) (string, error) {
	if key == "" {
		return "", ErrKeyEmpty
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	entry, ok := d.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return entry.Value, nil
}

// GetTxn returns the value associated with the provided key
// within an existing transaction using the provided transaction ID.
func (d *Database) GetTxn(key string, txnID string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

// Delete removes the value associated to the key provided.
func (d *Database) Delete(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.data[key]; !ok {
		return ErrKeyNotFound
	}
	delete(d.data, key)
	return nil
}

// DeleteTxn removes the value associated to the key provided
// within an existing transaction using the provided transaction ID.
func (d *Database) DeleteTxn(key string, txnID string) error {
	return fmt.Errorf("not implemented")
}
