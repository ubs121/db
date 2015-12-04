// Copyright 2013 ubs121
package bolt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"lerp/util"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

type (
	M map[string]interface{}

	DocElem struct {
		Key   string
		Value interface{}
	}

	D []DocElem

	SubGraph struct {
		Nodes []M `json:"nodes"`
		Links []M `json:"links"`
	}
	Triple struct {
		Id        string `bson:"_id", json:"_id"`
		Subject   string `bson:"_source", json:"_source"`
		Predicate string `bson:"төрөл", json:"төрөл"`
		Object    string `bson:"_target", json:"_target"`
		Context   string `,omitempty`
	}

	Args struct {
		Bucket string
		Query  string   ",omitempty"
		Sort   []string ",omitempty"
		Select M        ",omitempty"
		Start  string   ",omitempty"
		Skip   int      ",omitempty"
		Limit  int      ",omitempty"
		Data   M        ",omitempty"
		DataId string   ",omitempty"
	}
)

const (
	localFillPercent = 0.7
	bucketTriples    = "triples"
	userBucket       = "user"
)

var (
	_db *bolt.DB

	DefaultProjection = []string{"_id", "нэр", "төрөл", "эх_үүсвэр", "тэмдэглэл"}
)

/// Өгөгдлийн сан руу холбох
func Open(dbPath string) {
	var err error

	opts := &bolt.Options{Timeout: 3 * time.Second}
	_db, err = bolt.Open(dbPath, 0600, opts)
	if err != nil {
		panic(err)
		return
	}

	// create buckets
	_db.Update(func(tx *bolt.Tx) error {
		var err error
		for _, b := range []string{"triples", "triples_osp", "triples_pos", "user"} {
			_, err = tx.CreateBucketIfNotExists([]byte(b))
			if err != nil {
				return fmt.Errorf("Couldn't create bucket: %s", err)
			}
		}
		return nil
	})
}

// Өгөгдлийн санг хаах
func Close() {
	_db.Close()
}

// row filtering by role
func _filter() {

}

func _count(args *Args) (int, error) {
	return 0, nil
}

func Get(bucket string, id string) (string, error) {
	str := ""
	_db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		str = string(b.Get([]byte(id)))
		return nil
	})
	return str, nil
}

func GetJson(bucket string, id string, val interface{}) error {
	return _db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte(bucket)).Get([]byte(id))
		if v != nil {
			return json.Unmarshal(v, &val)
		} else {
			return errors.New(bucket + ":" + id + " өгөгдөл олдсонгүй !")
		}

		return nil
	})
}

func _find(args *Args, resp *[]M) {
	q := util.SkipWhitespace(args.Query)

	if q[0] == '(' {
		// TODO: find links
		// TODO: (*) ах (Болд)
	} else {
		// TODO: search by tags or fields
		q = util.SkipWhitespace(q[1:])

		if q[0] == ':' {
			// TODO: search by field
		} else {
			_findByTag(args, resp)
		}
	}

	// query дотор tags эсвэл талбараар хайсан нөхцөл орж ирнэ

	// select
	if args.Select != nil {
	} else {
	}

	// skip
	if args.Skip > 0 {
	}

	// limit
	if args.Limit > 0 {
	}

	// sort
	if len(args.Sort) > 0 {
	}

}

func _findById(args *Args, resp *M) error {

	return _db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(args.Bucket))
		c := b.Cursor()

		prefix := []byte(args.DataId + "|")
		key := ""

		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			key = string(k)
			parts := strings.Split(key, "|")
			(*resp)[parts[1]] = string(v)
		}

		if key != "" {
			(*resp)["_id"] = args.DataId
		}

		return nil
	})
}

func _findByTag(args *Args, resp *[]M) error {
	// TODO: wildcard * тооцох
	tags := strings.Split(strings.ToLower(args.Query), " ")

	return _db.View(func(tx *bolt.Tx) error {
		spoBucket := tx.Bucket([]byte(args.Bucket))
		ospBucket := tx.Bucket([]byte(args.Bucket + "_osp"))
		c := ospBucket.Cursor()

		nLimit := 0
		tag0 := []byte(tags[0])
		key := ""
		_id := ""

		for k, v := c.Seek(tag0); bytes.HasPrefix(k, tag0); k, v = c.Next() {
			key = string(k)
			os := strings.Split(key, "|")
			_id = os[1]

			o := M{}
			o["_id"] = _id
			o[string(v)] = os[0]

			i := 1
			allMatch := true
			for i < len(tags) {
				tval := ospBucket.Get([]byte(tags[i] + "|" + _id))
				if tval == nil {
					allMatch = false
					break
				}
				// NOTE: бодит утгыг нь авах уу?
				o[string(tval)] = tags[i]
				i++
			}

			if !allMatch {
				continue
			}

			// args.Select дээр байгаа бусад талбарыг оруулах
			for f, _ := range args.Select {
				rval := spoBucket.Get([]byte(_id + "|" + f))
				if rval != nil {
					o[f] = string(rval)
				}
			}

			// жагсаалтад нэмэх
			*resp = append(*resp, o)

			nLimit++
			if nLimit == args.Limit {
				break
			}
		}

		return nil
	})
}

func _findOne(args *Args, o *M) error {
	return _db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(args.Bucket))
		c := b.Cursor()

		// FIXME: бусад талбараар хайж болно (pso)
		prefix := []byte(args.DataId + "|")
		key := ""

		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			key = string(k)
			(*o)[key[len(prefix):]] = string(v)
		}

		return nil
	})
}

// id-аар обект цуглуулах
func _browse(bucket string, ids []string, resp *[]M) error {
	return _db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()

		for i := 0; i < len(ids); i++ {
			prefix := []byte(ids[i] + "|")

			o := M{}

			key := ""
			for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
				key = string(k)
				o[key[len(prefix):]] = string(v)
			}

			if key != "" {
				o["_id"] = ids[i]
			}

			*resp = append(*resp, o)
		}

		return nil
	})
}

func _processOne(o M) {
	// 'tags' талбарыг хасах
	delete(o, "tags")

	// TODO: холбоосуудыг нэмж оруулах

}

// TODO: хайлтыг сайжруулах, depth тооцох
func Traverse(graph *SubGraph, depth int, startIds ...string) {
}

func PutJson(bucket string, key string, val interface{}) error {
	// TODO: lock
	return _db.Update(func(tx *bolt.Tx) error {
		ts, _ := tx.CreateBucketIfNotExists([]byte(bucket))

		b, err := json.Marshal(val)
		if err != nil {
			return err
		}

		return ts.Put([]byte(key), b)
	})
}

func _deleteAll(bucket string, id string) error {
	return _db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		c := b.Cursor()

		prefix := []byte(id)

		for k, _ := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			b.Delete(k)
		}

		return nil
	})
}

func _delete(bucket string, id string) error {
	return _db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		return b.Delete([]byte(id))
	})
}

/// хавтаснаас өгөгдөл бааз руу ачаална

// Map returns a map out of the ordered element name/value pairs in d.
func (d D) Map() (m M) {
	m = make(M, len(d))
	for _, item := range d {
		m[item.Key] = item.Value
	}
	return m
}

func Contains(list []string, elem string) bool {
	for _, t := range list {
		if t == elem {
			return true
		}
	}
	return false
}

//TODO: id солих үйлдэл нэмэх, бүх холбоосыг засна
