package mongo

import (
	"encoding/json"
	"errors"
	"io"
	"log"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type ()

var (
	mgoSession *mgo.Session
	_db        *mgo.Database
)

// Open connects to a database.
func Open(dbHost string, dbName string) {
	if mgoSession == nil {
		var err error
		if mgoSession, err = mgo.Dial(dbHost); err != nil {
			panic(errors.New(dbHost + " Өгөгдлийн сантай холбогдоход алдаа гарлаа!"))
		}

		mgoSession.SetSafe(&mgo.Safe{})
		// Optional. Switch the session to a monotonic behavior.
		mgoSession.SetMode(mgo.Monotonic, true)
	}

	_db = mgoSession.Clone().DB(dbName)
}

// DB returns a database instance
func DB() *mgo.Database {
	// TODO: create _tags index
	return _db
}

// C returns a collection.
func C(col string) *mgo.Collection {
	return _db.C(col)
}

// Count returns a number of records in a collection
func Count(col string, query bson.M) (int, error) {
	return _db.C(col).Find(query).Count()
}

// FindOne returns a object matching given criteria in a collection
func FindOne(col string, query bson.M, proj []string) (bson.M, error) {
	q := _db.C(col).Find(query)

	// select
	if proj != nil && len(proj) > 0 {
		q.Select(_makeSel(proj...))
	}

	var resp bson.M
	err := q.One(&resp)
	return resp, err
}

// FindID returns a object with given ID in a Collection.
func FindID(col string, id interface{}) (bson.M, error) {
	q := _db.C(col).FindId(id)

	var resp bson.M
	err := q.One(&resp)
	return resp, err
}

// Find returns records matching given criteria in a collection.
func Find(col string, query bson.M, proj []string, sort []string, skip int, limit int) ([]bson.M, error) {
	q := _db.C(col).Find(query)

	// select
	if proj != nil && len(proj) > 0 {
		q.Select(_makeSel(proj...))
	}

	// skip
	if skip > 0 {
		q.Skip(skip)
	}

	// limit
	if limit > 0 {
		q.Limit(limit)
	}

	// sort
	if len(sort) > 0 {
		q.Sort(sort...)
	}

	var resp []bson.M
	err := q.All(&resp)
	return resp, err
}

// obj["updated"] = time.Now().String()[:19]
// obj["updatedBy"] = r.Header.Get("User")
//onSave(vars["collection"], obj, w, r)
// TODO: foreign key талбарууд шинэчилэх
// tags шинэчилэх
// UpdateTags(obj)

// Insert inserts a object into a collection. It throws an error if object already exists in a collection.
func Insert(col string, obj bson.M) error {
	return _db.C(col).Insert(obj)
}

// Save writes a object into a collection.
func Save(col string, obj bson.M) error {
	// TODO: implement save
	return nil
}

// Update updates existing object in a collection.
func Update(col string, obj bson.M) error {
	// TODO: implement update
	return nil
}

// Delete removes object by ID.
func Delete(col string, id string) error {
	return _db.C(col).RemoveId(id)
}

func _makeSel(proj ...string) (r bson.M) {
	r = make(bson.M, len(proj))
	for _, s := range proj {
		r[s] = 1
	}
	return
}

// Aggregate is a wrapper for MongoDB Pipe function.
func Aggregate(col string, pipeline bson.M) ([]bson.M, error) {
	pipe := _db.C(col).Pipe(pipeline)

	var resp []bson.M
	err := pipe.All(&resp)
	return resp, err
}

// MapReduce is a wrapper for MongoDB MapReduce function.
func MapReduce(col string, query bson.M, m string, r string) ([]bson.M, error) {
	q := _db.C(col).Find(query)

	var resp []bson.M
	_, err := q.MapReduce(&mgo.MapReduce{
		Map:    m,
		Reduce: r,
	}, &resp)
	return resp, err
}

// ExecJS calls a stored javascript in a database.
func ExecJS(id string, params interface{}) (error, bson.M) {
	var runResult bson.M

	err := _db.Run(bson.D{
		{"eval", "function(p) { return " + id + "(p); }"},
		{"args", []interface{}{params}},
		{"nolock", true},
	},
		&runResult)

	return err, runResult
}

/// Тайлан боловсруулах
// func Report(w http.ResponseWriter, r *http.Request) {
// 	var err error
// 	args := JsArgs{}
// 	rpc.ReadJson(r, &args)
//
// 	// call stored js
// 	var runResult bson.M
// 	var resp []bson.M
//
// 	err = _db.Run(bson.D{
// 		{"eval", args.Src},
// 		{"args", []bson.M{args.Params}},
// 		{"nolock", true},
// 	},
// 		&runResult)
//
// 	if err == nil {
//
// 		// боловсруулалтын дараах үр дүнг уншиж буцаах
// 		c := _db.C(runResult["retval"].(string))
// 		q := c.Find(bson.M{})
//
// 		if args.Params["Sort"] != nil {
// 			q.Sort([]string{args.Params["Sort"].(string)}...)
// 		}
//
// 		err = q.All(&resp)
// 	}
//
// 	rpc.WriteJson(r, w, resp, err)
// }

// ImportCSV parses comma separated data and inserts into a database.
func ImportCSV(colName string, data []byte) error {
	// var lines = csvString.split('\n');
	// var headerLine = lines[0];
	// var fields = headerLine.split(',');
	//
	// for (var i = 1; i < lines.length; i++) {
	//   var line = lines[i];
	//
	//   // The csvString that comes from the server has an empty line at the end,
	//   // need to ignore it.
	//   if (line.length == 0) {
	//     continue;
	// }

	return nil
}

// ImportJSON parses json formatted data and inserts it into a database.
func ImportJSON(colName string, data []byte) error {
	var jsonArray []bson.M

	if err := json.Unmarshal(data, &jsonArray); err == nil {
		c := _db.C(colName)

		for _, o := range jsonArray {
			//UpdateTags(o)

			if err := c.Insert(o); err != nil {
				log.Printf("%v: %v", o, err)
			}
		}
	} else {
		log.Printf("DATA FORMAT ERROR: %v", err)
		return err
	}

	return nil
}

// TODO: json export
func ExportJson(colName string, w io.Writer) error {
	// TODO: json export implementation

	return nil
}

func Close() {
	mgoSession.Close()
}
