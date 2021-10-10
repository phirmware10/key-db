package keydb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var (
	DB_FILE    = "./logs"
	INDEX_FILE = "./index.json"
)

type (
	DB struct {
		f        *os.File
		indexf   *os.File
		byteSize int
	}

	HashTable map[string]HashIndexValue

	HashIndexValue struct {
		Location int
		Max      int
	}
)

func init() {
	pid := os.Getpid()
	log.SetPrefix(fmt.Sprintf("%d]: DB: ", pid))
}

func NewDB(f *os.File, indexf *os.File) *DB {
	// f, err := os.OpenFile(DB_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// indexf, err := os.OpenFile(INDEX_FILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	info, err := f.Stat()

	if err != nil {
		log.Fatalf("FATAL: Error getting file information:, %s", err)
	}

	hf := &DB{
		f:        f,
		indexf:   indexf,
		byteSize: int(info.Size()),
	}

	return hf
}

func (hf *DB) Close() error {
	if err := hf.indexf.Close(); err != nil {
		return err
	}
	return hf.f.Close()
}

func (hf *DB) getIndexes() HashTable {
	info := hf.indexf.Name()

	data, err := ioutil.ReadFile(info)
	if err != nil {
		log.Fatalf("Error reading indexes: %s", err)
	}
	indexes := make(HashTable)

	json.Unmarshal(data, &indexes)
	return indexes
}

func (hf *DB) index(key string, end int) {
	indexes := hf.getIndexes()
	hiv := HashIndexValue{
		Location: hf.byteSize,
		Max:      hf.byteSize + end,
	}
	indexes[key] = hiv

	fbyte, err := json.Marshal(indexes)
	if err != nil {
		log.Fatalf("FATAL: could not write to file, %s", err)
	}

	os.WriteFile(hf.indexf.Name(), fbyte, 0666)
}

func (hf *DB) Write(key, value string) bool {
	formattedStr := key + ":" + value
	nb, err := hf.f.Write([]byte(formattedStr))
	if err != nil {
		log.Printf("ERROR: Could not write to file, %s", err)
		return false
	}
	hf.index(key, nb)
	hf.byteSize = hf.byteSize + nb

	return true
}

func (hf *DB) Read(key string) string {
	indexes := hf.getIndexes()
	hiv := indexes[key]

	size := hiv.Max - hiv.Location
	data := make([]byte, size)
	hf.f.ReadAt(data, int64(hiv.Location))

	sdata := string(data)
	splitdata := strings.Split(sdata, ":")
	return splitdata[len(splitdata)-1]
}

func (hf *DB) FlushDB() {
	hf.f.Truncate(0)
	hf.indexf.Truncate(0)
}
