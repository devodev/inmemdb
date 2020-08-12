package inmemdb

import (
	"errors"
	"sync"
)

var (
	// ErrKeyEmpty is returned when the provided key is an empty string.
	ErrKeyEmpty = errors.New("key is empty")
	// ErrKeyNotFound is returned when the provided key
	// is not found in the database.
	ErrKeyNotFound = errors.New("key not found")

	// ErrTransactionExists is returned when the provided transaction ID is currently active.
	ErrTransactionExists = errors.New("transaction already exists")
	// ErrTransactionNotFound is returned when the provided transaction ID is not currently active.
	ErrTransactionNotFound = errors.New("transaction not found")
	// ErrTransactionIDEmpty is returned when the provided key is an empty string.
	ErrTransactionIDEmpty = errors.New("transaction ID is empty")
	// ErrTransactionDiscrepancy is returned when a discrepancy is encountered during CommitTransaction.
	ErrTransactionDiscrepancy = errors.New("transaction dicscrepancy")
)

// Entry represents a database entry.
type Entry struct {
	Value string
}

// NewEntry creates a new database entry
// initialized using the provided value.
func NewEntry(value string) *Entry {
	return &Entry{Value: value}
}

// Database is an in-memory key-value store.
//
// Database is safe for concurrent use.
type Database struct {
	tMu                sync.RWMutex
	activeTransactions map[string]*Transaction

	mu   sync.RWMutex
	data map[string]*Entry
}

// NewDatabase creates a new Database.
func NewDatabase() *Database {
	db := &Database{
		activeTransactions: make(map[string]*Transaction),
		data:               make(map[string]*Entry),
	}
	return db
}

type releaseLock func()

func (d *Database) getTransaction(xid string) (*Transaction, releaseLock, error) {
	deferred := func() {}
	if xid == "" {
		return nil, deferred, ErrTransactionIDEmpty
	}
	d.tMu.Lock()
	deferred = d.tMu.Unlock

	transaction, ok := d.activeTransactions[xid]
	if !ok {
		return nil, deferred, ErrTransactionNotFound
	}
	return transaction, deferred, nil
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
func (d *Database) PutTxn(key string, value string, xid string) error {
	transaction, release, err := d.getTransaction(xid)
	defer release()
	if err != nil {
		return err
	}
	return transaction.Put(key, value)
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
func (d *Database) GetTxn(key string, xid string) (string, error) {
	transaction, release, err := d.getTransaction(xid)
	defer release()
	if err != nil {
		return "", err
	}
	return transaction.Get(key)
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
func (d *Database) DeleteTxn(key string, xid string) error {
	transaction, release, err := d.getTransaction(xid)
	defer release()
	if err != nil {
		return err
	}
	return transaction.Delete(key)
}

// CreateTransaction initializes a transaction
// for the provided transaction ID.
func (d *Database) CreateTransaction(xid string) error {
	transaction, release, err := d.getTransaction(xid)
	defer release()
	if err != nil && err != ErrTransactionNotFound {
		return err
	}
	if transaction != nil {
		return ErrTransactionExists
	}
	d.activeTransactions[xid] = NewTransaction(xid, d)
	return nil
}

// RollbackTransaction reverts uncommited changes from the database
// for the provided transaction ID.
func (d *Database) RollbackTransaction(xid string) error {
	defer func() {
		d.mu.Lock()
		delete(d.activeTransactions, xid)
		d.mu.Unlock()
	}()

	_, release, err := d.getTransaction(xid)
	defer release()
	if err != nil {
		return err
	}
	return nil
}

// CommitTransaction applies uncommited changes to the database
// for the provided transaction ID.
func (d *Database) CommitTransaction(xid string) error {
	defer func() {
		d.mu.Lock()
		delete(d.activeTransactions, xid)
		d.mu.Unlock()
	}()

	transaction, release, err := d.getTransaction(xid)
	defer release()
	if err != nil {
		return err
	}

	// loop over key/value from local cache and check for discrepancies
	// against current state of database.
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, uEntry := range transaction.uncommitedData {
		entry, ok := d.data[key]
		if !ok && uEntry.state != stateAdded {
			return ErrTransactionDiscrepancy
		}
		if ok {
			if uEntry.state == stateAdded {
				return ErrTransactionDiscrepancy
			}
			if uEntry.oldValue != &entry.Value {
				return ErrTransactionDiscrepancy
			}
		}
	}
	// no discrepancies, update database.
	for key, uEntry := range transaction.uncommitedData {
		switch uEntry.state {
		case stateAdded, stateUpdated:
			d.data[key] = NewEntry(*uEntry.newValue)
		case stateDeleted:
			delete(d.data, key)
		}
	}
	return nil
}
