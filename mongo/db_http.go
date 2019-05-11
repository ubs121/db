package mongo

import (
	"net/http"
	"time"

	"github.com/ubs121/rpc"
)

type (
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
	UrlPrefix = "/db"
)

func Register() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/db/schema", ReadSchema)
	mux.HandleFunc("/db/count", Count)
	mux.HandleFunc("/db/find", Find)
	mux.HandleFunc("/db/findId", FindId)
	mux.HandleFunc("/db/findOne", FindOne)
	mux.HandleFunc("/db/findLinks", FindLinks)
	mux.HandleFunc("/db/save", Save)
	mux.HandleFunc("/db/delete", Delete)
	mux.HandleFunc("/db/insert", Insert)
	mux.HandleFunc("/db/update", Update)
	mux.HandleFunc("/db/import", ImportTriple)

	return mux
}

// Бичлэгийн тоо
func Count(w http.ResponseWriter, r *http.Request) {
	args := &Args{}
	rpc.ReadJson(r, args)

	n, err := _count(args)
	rpc.WriteJson(r, w, n, err)
}

func ReadSchema(w http.ResponseWriter, r *http.Request) {
	rpc.WriteJson(r, w, "resp", nil)
}

/// Хайх
func Find(w http.ResponseWriter, r *http.Request) {
	var err error
	args := &Args{}
	rpc.ReadJson(r, args)

	// default bucket
	if args.Bucket == "" {
		args.Bucket = bucketTriples
	}

	// default limit
	if args.Limit == 0 {
		args.Limit = 50
	}

	var resp []M
	_find(args, &resp)

	rpc.WriteJson(r, w, resp, err)
}

/// Нэгийг унших
func FindOne(w http.ResponseWriter, r *http.Request) {
	args := &Args{}
	rpc.ReadJson(r, args)

	var resp M
	err := _findOne(args, &resp)

	rpc.WriteJson(r, w, resp, err)
}

// Id-аар хайх
func FindId(w http.ResponseWriter, r *http.Request) {
	args := &Args{}
	rpc.ReadJson(r, args)

	var resp M
	err := _findById(args, &resp)

	rpc.WriteJson(r, w, resp, err)
}

// обектын холбоосуудыг хайх
func FindLinks(w http.ResponseWriter, r *http.Request) {

	rpc.WriteJson(r, w, "graph", nil)
}

func Browse(w http.ResponseWriter, r *http.Request) {
	rpc.WriteJson(r, w, "OK", nil)
}

/// Id-аар шалгаад байхгүй бол Insert, байвал Update
func Save(w http.ResponseWriter, r *http.Request) {
	_save("save", w, r)
}

/// Шинээр нэмэх
func Insert(w http.ResponseWriter, r *http.Request) {
	_save("insert", w, r)
}

/// Засах
func Update(w http.ResponseWriter, r *http.Request) {
	_save("update", w, r)
}

func _save(op string, w http.ResponseWriter, r *http.Request) {
	var err error
	args := Args{}
	rpc.ReadJson(r, &args)

	args.Data["_updated"] = time.Now().String()[:19]
	args.Data["_updatedBy"] = r.Header.Get("User")

	// tags шинэчилэх
	updateTags(args.Data)

	// TODO: :зураг талбарыг шалгах, хоосон бол 'зураг' холбоосуудыг шалгах, хамгийн сүүлийн зургийг ашиглах

	// FIXME: баазад хуучин хадгалагдсан таагуудтай нэгтгэх хэрэгтэй, дарж хадгалаад байх шиг байна

	/*
		if op == "save" {
			if args.DataId != "" {
				_, err = c.UpsertId(args.DataId, args.Data)
			} else {
				args.Data["_created"] = time.Now().String()[:19]
				args.Data["_createdBy"] = r.Header.Get("User")
				args.DataId = bson.NewObjectId().Hex()

				err = c.Insert(args.Data)
			}
		} else if op == "insert" {
			args.Data["_created"] = time.Now().String()[:19]
			args.Data["_createdBy"] = r.Header.Get("User")
			args.DataId = bson.NewObjectId().Hex()

			err = c.Insert(args.Data)
		} else if op == "update" {
			err = c.UpdateId(args.DataId, args.Data)
		}
	*/
	rpc.WriteJson(r, w, args.DataId, err)
}

// http хүсэлтээр илгээсэн triple файлыг импортлох
func ImportTriple(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	_load_n3(r.Body)

	rpc.WriteJson(r, w, "OK", nil)
}

// Id-аар устгах
func Delete(w http.ResponseWriter, r *http.Request) {
	args := Args{}
	rpc.ReadJson(r, &args)

	err := _deleteAll(bucketTriples, args.DataId)

	rpc.WriteJson(r, w, args.DataId, err)
}
