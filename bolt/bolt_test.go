package bolt

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

func TestFind(t *testing.T) {
	var err error

	opts := &bolt.Options{Timeout: 3 * time.Second}
	_db, err = bolt.Open("../db/data/ci.db", 0600, opts)
	defer _db.Close()

	if err != nil {
		panic(err)
		return
	}

	args := &Args{}
	args.Bucket = "triples"

	args.Query = "Ууганбаяр Сүхбаатар"
	args.Select = M{"_id": 1, ":төрөл": 1, ":нэр": 1, ":эцгийн_нэр": 1}
	args.Limit = 50

	var resp []M
	_find(args, &resp)

	for i := 0; i < len(resp); i++ {
		o, _ := json.Marshal(resp[i])
		println(string(o))
	}
}

func TestFindId(t *testing.T) {
	var err error

	opts := &bolt.Options{Timeout: 3 * time.Second}
	_db, err = bolt.Open("../db/data/ci.db", 0600, opts)
	defer _db.Close()

	if err != nil {
		panic(err)
		return
	}

	args := &Args{}
	args.Bucket = "triples"
	args.DataId = "ЛЖ79011218"

	resp := M{}
	_findById(args, &resp)

	println("----------TestFindId-----------")

	o, _ := json.Marshal(resp)
	println(string(o))
}

func TestBrowse(t *testing.T) {
	var err error

	opts := &bolt.Options{Timeout: 3 * time.Second}
	_db, err = bolt.Open("../db/data/ci.db", 0600, opts)
	defer _db.Close()

	if err != nil {
		panic(err)
		return
	}

	var resp []M
	_browse("triples", []string{"ЛЖ79011218"}, &resp)

	println("----------TestBrowse-----------")

	for i := 0; i < len(resp); i++ {
		o, _ := json.Marshal(resp[i])
		println(string(o))
	}
}
