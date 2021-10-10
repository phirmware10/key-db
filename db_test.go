package keydb

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func createTempFile(t *testing.T, pattern string, initialValue *string) (*os.File, func()) {
	t.Helper()
	f, err := ioutil.TempFile("", pattern)
	if err != nil {
		t.Fatalf("Could not create file: %s", err)
	}

	if initialValue != nil {
		f.Write([]byte(*initialValue))
	}

	removeFile := func() {
		f.Close()
		os.Remove(f.Name())
	}

	return f, removeFile
}

func TestNewDB(t *testing.T) {
	DB_FILE = "logs"
	INDEX_FILE = "index.json"

	dbFile, removeDB := createTempFile(t, DB_FILE, nil)
	indexFile, removeIndex := createTempFile(t, INDEX_FILE, nil)
	defer removeDB()
	defer removeIndex()

	db := NewDB(dbFile, indexFile)
	if db.f != dbFile {
		t.Errorf("database files are not the same: got %v want %v", db.f, dbFile)
	}

	if db.indexf != indexFile {
		t.Errorf("index files are not the same: got %v want %v", db.indexf, indexFile)
	}

	if db.byteSize != 0 {
		t.Errorf("DB file not empty want 0 got %d", db.byteSize)
	}
}

func TestGetIndexes(t *testing.T) {
	testtable := []struct {
		index            string
		expectedLocation int
		expectedMax      int
	}{
		{
			index:            `{"/unix/temp": { "Location": 45, "Max": 67 }}`,
			expectedLocation: 45,
			expectedMax:      67,
		},
		{
			index:            `{"/unix/temp": { "Location": 0, "Max": 5 }}`,
			expectedLocation: 0,
			expectedMax:      5,
		},
	}

	for _, table := range testtable {
		dbFile, closeDBFile := createTempFile(t, "db", nil)
		defer closeDBFile()

		indexF, closeIndexFile := createTempFile(t, "index", &table.index)
		defer closeIndexFile()

		db := NewDB(dbFile, indexF)
		ht := db.getIndexes()

		gotLocation := ht["/unix/temp"].Location
		gotMax := ht["/unix/temp"].Max

		if table.expectedLocation != gotLocation {
			t.Errorf("Expected %d got %d", table.expectedLocation, gotLocation)
		}

		if table.expectedMax != gotMax {
			t.Errorf("Expected %d got %d", table.expectedMax, gotMax)
		}
	}
}

func TestIndex(t *testing.T) {
	testtable := []struct {
		dbdata    string
		indexdata string
		key       string
		expect    string
		end       int
	}{
		{
			dbdata:    `username:phirmware`,
			indexdata: `{"username":{"Location":0,"Max":18}}`,
			key:       "key",
			expect:    `{"key":{"Location":18,"Max":47},"username":{"Location":0,"Max":18}}`,
			end:       29,
		},
		{
			dbdata:    `key:value`,
			indexdata: `{"key":{"Location":0,"Max":9}}`,
			key:       "database",
			expect:    `{"database":{"Location":9,"Max":49},"key":{"Location":0,"Max":9}}`,
			end:       40,
		},
	}

	for _, table := range testtable {
		dbFile, closeDBFile := createTempFile(t, "db", &table.dbdata)
		defer closeDBFile()

		indexF, closeIndexFile := createTempFile(t, "index", &table.indexdata)
		defer closeIndexFile()

		db := NewDB(dbFile, indexF)
		db.index(table.key, table.end)

		data, _ := os.ReadFile(indexF.Name())
		got := string(data)
		if got != table.expect {
			t.Fatalf("Expected %s got %s", table.expect, got)
		}
	}
}

func TestWriteAndRead(t *testing.T) {
	dbFile, closeDBFile := createTempFile(t, "db", nil)
	defer closeDBFile()

	indexF, closeIndexFile := createTempFile(t, "index", nil)
	defer closeIndexFile()

	key := "name"
	value := "phirmware"

	db := NewDB(dbFile, indexF)

	t.Run("should successfully write to file", func(t *testing.T) {
		result := db.Write(key, value)

		if result != true {
			t.Error("Failed writing data to file")
		}
	})

	t.Run("should correctly index the written key", func (t *testing.T) {
		idx := db.getIndexes()
		keyIndex := idx[key]
		wantedLocation := 0
		wantedMax := 14
		
		if keyIndex.Location != wantedLocation {
			t.Errorf("Failed, incorrect index, want %d got %d", wantedLocation, keyIndex.Location)
		}

		if keyIndex.Max != wantedMax {
			t.Errorf("Failed,incorrect index, want %d got %d", wantedMax, keyIndex.Max)
		}
	})

	t.Run("should get the correct key value", func(t *testing.T) {
		got := db.Read(key)

		fmt.Println(strings.Compare(got, value))

		if strings.Compare(got, value) != 0 {
			t.Errorf("Failed getting incorrect value, want:%s,got:%s,", value, got)
		}
	})

	t.Run("should add other values", func (t *testing.T) {
		localkey := "new-key"
		done := db.Write(localkey, "value")
		if done != true {
			t.Errorf("Did not complete write to file, want %v got %v", true, done)
		}

		if db.Read(key) != "phirmware" {
			t.Fatalf("wanted %s got %s", "phirmware", db.Read(key))
		}
		
		if db.Read(localkey) != "value" {
			t.Fatalf("wanted %s got %s", "value", db.Read(localkey))
		}
	})
}
