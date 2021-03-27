package gosimstor


import (
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
)

var storage *Storage

func init() {
	s, _ := getStorage()
	storage = s
}

func getStorage() (*Storage, func(s *Storage)) {
	//
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	dir = strings.Join([]string{
		dir,
		"data",
	}, "/")
	storage, err := NewStorage(
		NewFileProvider(
			"bagwords",
			dir,
			3,
			ToStringString,
			ToStringFloat64Vector,
			FromStringToString,
			FromStringToFloat64Vector,
		),
		NewFileProvider(
			"bench",
			dir,
			3,
			ToStringString,
			ToStringFloat64Vector,
			FromStringToString,
			FromStringToFloat64Vector,
		),
	)
	if err != nil {
		log.Println(err)
		if err := Destructor(storage); err != nil {
			log.Println(err)
		}
		log.Fatal(err)
	}
	return storage, func(s *Storage) {
		if err := Destructor(storage); err != nil {
			log.Fatal(err)
		}
		log.Println("OK")
	}
}

func TestReadFlow(t *testing.T) {
	keys, err := storage.GetIDs("bagwords")
	if err != nil {
		t.Fatal(err)
	}
	log.Println("START READ UPDATE................................................")
	for _, key := range keys {
		id, data, err := storage.Read(
			"bagwords",
			key,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", id, " WITH DATA ", data)
		}
	}
	log.Println("FINISH READ UPDATE................................................")
}

func TestInsertReadUpdateFlow(t *testing.T) {
	keys := make([]string, 0)
	log.Println("START WRITE................................................")
	for i := 0; i < 10; i++ {
		inc := strconv.Itoa(i)
		key := "Key" + inc
		data := []float64{
			1.2 * float64(i), 3.4 * float64(i),
		}
		err := storage.Insert(
			"bagwords",
			key,
			data,
		)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	log.Println("FINISH WRITE................................................")
	log.Println("START READ................................................")
	for _, key := range keys {
		id, data, err := storage.Read(
			"bagwords",
			key,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", id, " WITH DATA ", data)
		}
	}
	log.Println("FINISH READ................................................")
	log.Println("START UPDATE................................................")
	for _, key := range keys {
		err := storage.Update(
			"bagwords",
			key,
			[]float64{
				1.2 * float64(-1), 3.4 * float64(-1),
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	log.Println("FINISH UPDATE................................................")
	log.Println("START READ UPDATE................................................")
	for _, key := range keys {
		id, data, err := storage.Read(
			"bagwords",
			key,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", id, " WITH DATA ", data)
		}
	}
	log.Println("FINISH READ UPDATE................................................")
}

func TestRewriteFlow(t *testing.T) {
	var (
		count        = 10
		id           = make([]string, 0)
		data         = make([][]float64, count+1)
		createIdList = func() {
			for i := 0; i < count; i++ {
				inc := strconv.Itoa(i)
				key := "Key" + inc
				id = append(id, key)
			}
		}
		createDataList = func() {
			for i := 0; i < count; i++ {
				vector := make([]float64, count)
				for j := 0; j < count; j++ {
					vector[j] = float64(j * i)
				}
				data[i] = vector
			}
		}
		update = func() {
			for i := 0; i < count; i++ {
				for j := 0; j < count; j++ {
					data[i][j] = data[i][j] * float64(-1)
				}
				data[i] = append(data[i], float64(0))
			}
			vector := make([]float64, count+1)
			for i := 0; i < count+1; i++ {
				vector[i] = float64(0)
			}
			data[count] = vector
			id = append(id, "Key"+strconv.Itoa(count))
		}
		convertDataToSliceInterface = func() []interface{} {
			d := make([]interface{}, 0)
			for i := 0; i < len(data); i++ {
				d = append(d, data[i])
			}
			return d
		}
		convertIdToSliceInterface = func() []interface{} {
			d := make([]interface{}, 0)
			for i := 0; i < len(id); i++ {
				d = append(d, id[i])
			}
			return d
		}
	)
	createIdList()
	createDataList()
	log.Println("START WRITE................................................")
	for i := 0; i < len(id); i++ {
		err := storage.Insert(
			"bagwords",
			id[i],
			data[i],
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	log.Println("FINISH WRITE................................................")
	//
	log.Println("START READ................................................")
	for i := 0; i < len(id); i++ {
		id, data, err := storage.Read(
			"bagwords",
			id[i],
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", id, " WITH DATA ", data)
		}
	}
	log.Println("FINISH READ................................................")
	//
	update()
	//
	log.Println("START REWRITE................................................")
	err := storage.Rewrite(
		"bagwords",
		convertIdToSliceInterface(),
		convertDataToSliceInterface(),
	)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("FINISH REWRITE................................................")
	//
	log.Println("START READ................................................")
	for i := 0; i < len(id); i++ {
		id, data, err := storage.Read(
			"bagwords",
			id[i],
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", id, " WITH DATA ", data)
		}
	}
	log.Println("FINISH READ................................................")
}

func BenchmarkWriting(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		inc := strconv.Itoa(i)
		key := "Key" + inc
		data := []float64{
			1.2 * float64(i), 3.4 * float64(i),
		}
		_ = storage.Insert(
			"bench",
			key,
			data,
		)
		//if err != nil {
		//	log.Println("i = ",i, " err: ", err)
		//}
	}
}

func BenchmarkReading(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		inc := strconv.Itoa(i)
		key := "Key" + inc
		_, _, _ = storage.Read(
			"bench",
			key,
		)
		//if err != nil {
		//	log.Println("i = ",i, " err: ", err)
		//}
	}
}

func BenchmarkUpdate(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		inc := strconv.Itoa(i)
		key := "Key" + inc
		_ = storage.Update(
			"bench",
			key,
			[]float64{
				1.2 * float64(-1), 3.4 * float64(-1),
			},
		)
		//if err != nil {
		//	log.Println("i = ",i, " err: ", err)
		//}
	}
}