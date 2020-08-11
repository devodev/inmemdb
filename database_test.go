package inmemdb

import (
	"fmt"
	"testing"
)

// stubDatabase provides an initilized database
// that is ready to be used in tests
func stubDatabase(t *testing.T) *Database {
	t.Helper()

	db := NewDatabase()
	if db == nil {
		t.Fatal("db is nil, something went terribly wrong..")
	}
	if db.data == nil {
		t.Fatal("db.data is nil, but we would appreciate if it could be initialized instead")
	}
	return db
}

// compareError raises a test error when a discrepancy is found
// between error received vs error wanted.
func compareError(t *testing.T, gotErr, WantErr error) {
	t.Helper()

	if gotErr != nil {
		if WantErr != nil && gotErr != WantErr {
			t.Errorf("got error: %v, but wanted error: %v", gotErr, WantErr)
		}
		if WantErr == nil {
			t.Errorf("got error: %v, but wanted no error", gotErr)
		}
	} else if WantErr != nil {
		t.Errorf("got no error, but wanted error: %v", WantErr)
	}
}

func TestDatabase(t *testing.T) {
	// perform smoke test before running anything else
	stubDatabase(t)
}

func TestDatabaseGet(t *testing.T) {
	// acquire stub
	db := stubDatabase(t)

	// fill with arbitrary data
	data := map[string]string{
		"key1": "value1",
		"key2": "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooonnng-value2",
		"key4": "",
	}
	for key, value := range data {
		db.Put(key, value)
	}

	// define table driven test cases
	cases := []struct {
		Key       string
		Want      string
		WantError error
	}{
		{Key: "", Want: "", WantError: ErrKeyEmpty},
		{Key: "notFound", Want: "", WantError: ErrKeyNotFound},
		{Key: "key1", Want: "value1", WantError: nil},
		{Key: "key2", Want: "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooonnng-value2", WantError: nil},
		{Key: "key4", Want: "", WantError: nil},
	}

	// run cases
	for idx, c := range cases {
		t.Run(fmt.Sprintf("%d. Key[%v]. WantError[%v]", idx, c.Key, c.WantError), func(t *testing.T) {
			result, err := db.Get(c.Key)
			compareError(t, err, c.WantError)

			// error has already been handled in compareError()
			// and we should not attempt to continue
			if c.WantError != nil {
				return
			}

			if result != c.Want {
				t.Errorf("got result: %v, but wanted: %v", result, c.Want)
			}
		})
	}
}

func TestDatabasePut(t *testing.T) {
	// acquire stub
	db := stubDatabase(t)

	// define table driven test cases
	cases := []struct {
		Key       string
		Value     string
		WantError error
	}{
		{Key: "", Value: "", WantError: ErrKeyEmpty},
		{Key: "key1", Value: "value1", WantError: nil},
		{Key: "key2", Value: "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooonnng-value2", WantError: nil},
		{Key: "key4", Value: "", WantError: nil},
	}

	// run cases
	for idx, c := range cases {
		t.Run(fmt.Sprintf("%d. Key[%v]. WantError[%v]", idx, c.Key, c.WantError), func(t *testing.T) {
			// error check on a different line to improve readability in tests
			err := db.Put(c.Key, c.Value)
			compareError(t, err, c.WantError)

			// error has already been handled in compareError()
			// and we should not attempt to continue
			if c.WantError != nil {
				return
			}

			result, err := db.Get(c.Key)
			if err != nil {
				t.Errorf("got error when retrieving inserted value: %v", err)
			}
			if result != c.Value {
				t.Errorf("got result: %v, but wanted: %v", result, c.Value)
			}
		})
	}
}

func TestDatabaseDelete(t *testing.T) {
	// acquire stub
	db := stubDatabase(t)

	// fill with arbitrary data
	data := map[string]string{
		"key1": "value1",
		"key2": "looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooonnng-value2",
		"key4": "",
	}
	for key, value := range data {
		db.Put(key, value)
	}

	// define table driven test cases
	cases := []struct {
		Key       string
		WantError error
	}{
		{Key: "", WantError: ErrKeyEmpty},
		{Key: "notFound", WantError: ErrKeyNotFound},
		{Key: "key1", WantError: nil},
	}

	// run cases
	for idx, c := range cases {
		t.Run(fmt.Sprintf("%d. Key[%v]. WantError[%v]", idx, c.Key, c.WantError), func(t *testing.T) {
			// error checks on a different lines to improve readability in tests
			err := db.Delete(c.Key)
			compareError(t, err, c.WantError)

			// error has already been handled in compareError()
			// and we should not attempt to continue
			if c.WantError != nil {
				return
			}

			_, err = db.Get(c.Key)
			if err != nil {
				if err != ErrKeyNotFound {
					t.Errorf("got error when retrieving deleted value: %v", err)
				}
				return
			}
			t.Errorf("got result after delete")
		})
	}
}
