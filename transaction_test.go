package inmemdb

import "testing"

func TestTransaction(t *testing.T) {
	db := stubDatabase(t)

	t.Run("create transaction with empty ID", func(t *testing.T) {
		err := db.CreateTransaction("")
		if err != ErrTransactionIDEmpty {
			t.Errorf("did not fail on empty transaction ID and received err: %v", err)
		}
	})

	t.Run("create transaction that already exists, then rollback", func(t *testing.T) {
		err := db.CreateTransaction("tx1")
		if err != nil {
			t.Errorf("got error on CreateTransaction but should have got nil: %v", err)
		}
		err = db.CreateTransaction("tx1")
		if err != ErrTransactionExists {
			t.Errorf("did not fail when transaction ID already exists and received err: %v", err)
		}
		err = db.RollbackTransaction("tx1")
		if err != nil {
			t.Errorf("got error on RollbackTransaction but should have got nil: %v", err)
		}
	})

	t.Run("commit transaction with new entry", func(t *testing.T) {
		err := db.CreateTransaction("tx1")
		if err != nil {
			t.Errorf("got error on CreateTransaction but should have got nil: %v", err)
		}
		err = db.PutTxn("key1", "value1", "tx1")
		if err != nil {
			t.Errorf("got error on PutTxn but should have got nil: %v", err)
		}
		err = db.CommitTransaction("tx1")
		if err != nil {
			t.Errorf("got error on CommitTransaction but should have got nil: %v", err)
		}
		value, err := db.Get("key1")
		if err != nil {
			t.Errorf("got error on Get but should have got nil: %v", err)
		}
		if value != "value1" {
			t.Errorf("value retrieved after commit is not what was Put during transaction: %v", err)
		}
	})

	t.Run("commit transaction with deleted entry", func(t *testing.T) {
		err := db.Put("key1", "value1")
		if err != nil {
			t.Errorf("got error on Put but should have got nil: %v", err)
		}
		err = db.CreateTransaction("tx1")
		if err != nil {
			t.Errorf("got error on CreateTransaction but should have got nil: %v", err)
		}
		err = db.DeleteTxn("key1", "tx1")
		if err != nil {
			t.Errorf("got error on DeleteTxn but should have got nil: %v", err)
		}
		err = db.CommitTransaction("tx1")
		if err != nil {
			t.Errorf("got error on CommitTransaction but should have got nil: %v", err)
		}
		_, err = db.Get("key1")
		if err == nil {
			t.Errorf("did not get an error on Get but should have received: %v", ErrKeyNotFound)
		}
		if err != ErrKeyNotFound {
			t.Errorf("should have got error: %v but got: %v", ErrKeyNotFound, err)
		}
	})
}

func TestTransactionRequestSequence(t *testing.T) {
	db := stubDatabase(t)

	t.Run("request sequence from problem statement", func(t *testing.T) {
		db.CreateTransaction("abc")
		db.PutTxn("a", "foo", "abc")

		value, err := db.GetTxn("a", "abc")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "foo" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "foo")
		}
		value, err = db.Get("a")
		if err == nil {
			t.Errorf("got no error but should have got: %v", ErrKeyNotFound)
		}

		db.CreateTransaction("xyz")
		db.PutTxn("a", "bar", "xyz")

		value, err = db.GetTxn("a", "xyz")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "bar" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "bar")
		}

		db.CommitTransaction("xyz")

		value, err = db.Get("a")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "bar" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "bar")
		}

		err = db.CommitTransaction("abc")
		if err == nil {
			t.Errorf("got no error but should have got: %v", ErrTransactionDiscrepancy)
		}
		if err != ErrTransactionDiscrepancy {
			t.Errorf("invalid error received: %v. wanted: %v", err, ErrTransactionDiscrepancy)
		}

		value, err = db.Get("a")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "bar" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "bar")
		}

		db.CreateTransaction("abc")
		db.PutTxn("a", "foo", "abc")

		value, err = db.Get("a")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "bar" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "bar")
		}

		db.RollbackTransaction("abc")

		err = db.PutTxn("a", "foo", "abc")
		if err == nil {
			t.Errorf("got no error but should have got: %v", ErrTransactionNotFound)
		}

		value, err = db.Get("a")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "bar" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "bar")
		}

		db.CreateTransaction("def")
		db.PutTxn("b", "foo", "def")

		value, err = db.GetTxn("a", "def")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "bar" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "bar")
		}

		value, err = db.GetTxn("b", "def")
		if err != nil {
			t.Errorf("got error but should have got nil: %v", err)
		}
		if value != "foo" {
			t.Errorf("invalid value retrieved: %v. wanted: %v", value, "foo")
		}

		db.RollbackTransaction("def")

		value, err = db.Get("b")
		if err == nil {
			t.Errorf("got no error but should have got: %v", ErrKeyNotFound)
		}
	})
}
