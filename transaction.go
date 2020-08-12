package inmemdb

type stateType int

// state enum.
const (
	stateAdded stateType = iota
	stateUpdated
	stateDeleted
)

// UncommitedEntry represents a database entry
// that is not yet commited.
type UncommitedEntry struct {
	oldValue *string
	newValue *string
	state    stateType
}

// Transaction represents a single unit of work
// to be performed against a Database.
//
// Transaction implements the `read commited` isolation level.
type Transaction struct {
	id             string
	db             *Database
	uncommitedData map[string]*UncommitedEntry
}

// NewTransaction initializes a transaction and returns it.
func NewTransaction(id string, db *Database) *Transaction {
	return &Transaction{
		id:             id,
		db:             db,
		uncommitedData: make(map[string]*UncommitedEntry),
	}
}

// Put updates/creates an uncommitedEntry and adds it to its local cache.
func (t *Transaction) Put(key string, value string) error {
	// entry exists in local cache
	if uEntry, ok := t.uncommitedData[key]; ok {
		if uEntry.state == stateDeleted {
			uEntry.state = stateUpdated
		}
		uEntry.newValue = &value
		return nil
	}
	// entry does not exists locally, check if it exists in database.
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if entry, ok := t.db.data[key]; ok {
		// entry exists in database, create an uncommitedEntry as stateUpdated
		// using the value retrieved as oldValue.
		//
		// oldValue will be used on commitTransaction to verify
		// if someone updated the value outside the transaction context.
		uEntry := UncommitedEntry{
			oldValue: &entry.Value,
			newValue: &value,
			state:    stateUpdated,
		}
		t.uncommitedData[key] = &uEntry
		return nil
	}

	// entry does not exists as well in the database, so
	// create an uncommitedEntry as stateAdded.
	uEntry := UncommitedEntry{
		newValue: &value,
		state:    stateAdded,
	}
	t.uncommitedData[key] = &uEntry
	return nil
}

// Delete udpates/creates an uncommitedEntry and adds it to its local cache.
func (t *Transaction) Delete(key string) error {
	// entry exists in local cache
	if uEntry, ok := t.uncommitedData[key]; ok {
		// entry was added and now deleted, just remove entry from uncommitedData
		if uEntry.state == stateAdded {
			delete(t.uncommitedData, key)
			return nil
		}
		uEntry.state = stateDeleted
		uEntry.newValue = nil
		return nil
	}
	// entry does not exists locally, check if it exists in database
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if entry, ok := t.db.data[key]; ok {
		// entry exists in database, create an uncommitedEntry as stateDeleted
		// and sets newValue to nil.
		//
		// oldValue will be used on commitTransaction to verify
		// if someone updated the value outside the transaction context.
		uEntry := UncommitedEntry{
			oldValue: &entry.Value,
			newValue: nil,
			state:    stateDeleted,
		}
		t.uncommitedData[key] = &uEntry
		return nil
	}
	return ErrKeyNotFound
}

// Get fetches a value associated to the provided key
// if it is visible from the transaction context.
func (t *Transaction) Get(key string) (string, error) {
	// entry exists in local cache, check state
	// if not deleted and return it
	if uEntry, ok := t.uncommitedData[key]; ok {
		if uEntry.state == stateDeleted {
			return "", ErrKeyNotFound
		}
		return *uEntry.newValue, nil
	}
	// entry does not exists locally, check if it exists in database
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	if entry, ok := t.db.data[key]; ok {
		// entry exists in database, return it as-is.
		return entry.Value, nil
	}
	return "", ErrKeyNotFound
}
