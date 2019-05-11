package triple

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"github.com/boltdb/bolt"
	"hash"
	"io"
	"log"
	"os"
	"strings"
)

const (
	WRITE_SIZE = 100000
)

var (
	triples []*Triple

	makeHasher func() hash.Hash
	hasherSize int
	bucketTo   string = "triples"
)

// TODO: add context: user, _createdBy, _updatedBy
func Load(bucket string, tripleFile string) {
	log.Printf("Loading file %s...\n", tripleFile)

	f, err := os.Open(tripleFile)
	if err != nil {
		log.Fatalf("%s файлыг нээж чадахгүй байна", tripleFile)
		return
	}

	defer f.Close()

	bucketTo = bucket

	makeHasher = sha1.New
	hasherSize = sha1.Size

	if strings.HasSuffix(f.Name(), ".n3") {
		_load_n3(f)
	} else if strings.HasSuffix(f.Name(), ".n2") {
		_load_n2(f)
	}

}

// гурвалсан форматтай файл импортлох
func _load_n3(rdr io.Reader) {
	br := bufio.NewReader(rdr)

	nTriples := 0
	nBatch := 0
	line := ""
	multiline := false

	var triple *Triple

	triples = make([]*Triple, 0)

	for {
		l, prefix, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Уншихад алдаа гарлаа: %v\n", err.Error())
		}

		str := string(l)
		line += str

		// өмнөх мөрийн үргэлжлэл байна
		if prefix {
			continue
		}

		if multiline {
			if pos := strings.LastIndex(str, "`"); pos >= 0 {
				triple.Object = line[:len(line)-(len(str)-pos)]
				multiline = false
			} else {
				continue
			}
		} else {

			// Skip leading whitespace.
			str = skipWhitespace(str)
			// Check for a comment
			if str != "" && str[0] == '#' {
				continue
			}

			sub, remainder := getId(str)
			if sub == nil {
				continue
			}
			triple = &Triple{}
			triple.Subject = strings.ToUpper(*sub)

			str = skipWhitespace(remainder)
			pred, remainder := getId(str)
			if pred == nil {
				continue
			}
			triple.Predicate = strings.ToLower(*pred)

			str = skipWhitespace(remainder)
			obj, remainder := getValue(str)
			if obj == nil {
				multiline = true
				line = remainder
				continue
			}
			triple.Object = *obj

			// context байна
			str = skipWhitespace(remainder)
			if str != "" {

				if str[0] == '{' {
					// холбоосын нэмэлт атрибут
					triple.Context = str
				} else {
					// triple.Object дээр нэмж залгах
					triple.Object = triple.Object + " " + str
				}

			}
		}

		// нэг triple олдов
		if triple != nil {
			nTriples++
			nBatch++

			triples = append(triples, triple)

			if len(triples) >= WRITE_SIZE {
				_write()

				println(nTriples)

				nBatch = 0
				triples = triples[:0] // clear triples
			}

		}

		// хувьсагчдыг эхний төлөвт бэлтгэх
		line = ""

	}

	// үлдэгдэл байвал шавхаж бичих
	if len(triples) > 0 {
		_write()
		println(nTriples)
	}

	log.Println("--------------------------")
	log.Println(nTriples, " гурвал уншлаа")

}

func _write() error {
	// TODO: lock
	_db.NoSync = true
	defer func() {
		_db.NoSync = false
	}()

	return _db.Update(func(tx *bolt.Tx) error {
		var err error
		var key string
		var val string

		spo, _ := tx.CreateBucketIfNotExists([]byte(bucketTo))
		spo.FillPercent = localFillPercent

		osp, _ := tx.CreateBucketIfNotExists([]byte(bucketTo + "_osp"))
		osp.FillPercent = localFillPercent

		_id := ""

		for i := 0; i < len(triples); i++ {
			// main (spo) bucket
			key = triples[i].Subject + "|" + triples[i].Predicate
			val = triples[i].Object

			spo.Put([]byte(key), []byte(val))

			// tags (osp)
			_id = strings.ToLower(triples[i].Subject) + "|" + triples[i].Subject
			if osp.Get([]byte(_id)) == nil {
				osp.Put([]byte(_id), []byte("_id"))
			}

			tags := extractTags(val)
			for j := 0; j < len(tags); j++ {
				key = tags[j] + "|" + triples[i].Subject
				val = triples[i].Predicate
				// байгаа эсэхийг шалгах
				exists := osp.Get([]byte(key))
				if exists == nil {
					osp.Put([]byte(key), []byte(val))
				}
			}

			// links (pso) зөвхөн холбоос бол оруулах хэрэгтэй

		}

		return err
	})

}

func GetIdForTriple(t *Triple) string {
	//bson.NewObjectId().Hex()

	hasher := makeHasher()
	id := convertStringToByteHash(t.Subject, hasher)
	id += convertStringToByteHash(t.Predicate, hasher)
	id += convertStringToByteHash(t.Object, hasher)
	//id += convertStringToByteHash(t.Label, hasher)

	return id
}

func convertStringToByteHash(s string, hasher hash.Hash) string {
	hasher.Reset()
	key := make([]byte, 0, hasherSize)
	hasher.Write([]byte(s))
	key = hasher.Sum(key)
	return hex.EncodeToString(key)
}

func _load_n2(rdr io.Reader) {
	br := bufio.NewReader(rdr)

	// TODO: lock
	_db.NoSync = true
	defer func() {
		_db.NoSync = false
	}()

	n := 0

	_db.Update(func(tx *bolt.Tx) error {
		ts, _ := tx.CreateBucketIfNotExists([]byte(bucketTo))
		ts.FillPercent = localFillPercent

		line := ""

		for {
			l, prefix, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			line += string(l)

			// өмнөх мөрийн үргэлжлэл байна
			if prefix {
				continue
			}

			n++
			parts := strings.Split(line, "|")

			if len(parts) > 1 {
				ts.Put([]byte(parts[0]), []byte(parts[1]))
			} else {
				log.Fatalln(n, " мөр алдаатай байна")
			}

			// хувьсагчдыг бэлтгэх
			line = ""
		}

		return nil
	})

	log.Println("--------------------------")
	log.Println(n, " мөр уншлаа")
}
