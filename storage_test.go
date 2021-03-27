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
	log.Println("START READ BY KEYS................................................")
	for _, key := range keys {
		row, err := storage.Read(
			"bagwords",
			key,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", row.ID, " WITH DATA ", row.Data)
		}
	}
	log.Println("FINISH READ BY KEYS................................................")
}

//func TestReadAllFlow(t *testing.T) {
//	rows, err := storage.ReadAll("bagwords")
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(rows) == 0 {
//		t.Fatal("ROWS SLICE SIZE 0")
//	}
//	log.Println("START READ ALL................................................")
//	for _, row := range rows {
//		log.Println("READ ID ", row.ID, " WITH DATA ", row.Data)
//	}
//	log.Println("FINISH READ ALL................................................")
//}

func TestInsertReadUpdateFlow(t *testing.T) {
	rows := make([]Row, 0)
	log.Println("START WRITE................................................")
	for i := 0; i < 10; i++ {
		row := Row{
			ID: "Key" + strconv.Itoa(i),
			Data: []float64{
				1.2 * float64(i), 3.4 * float64(i),
			},
		}
		err := storage.Insert(
			"bagwords",
			row,
		)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	log.Println("FINISH WRITE................................................")
	log.Println("START READ................................................")
	for _, row := range rows {
		r, err := storage.Read(
			"bagwords",
			row.ID,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", r.ID, " WITH DATA ", r.Data)
		}
	}
	log.Println("FINISH READ................................................")
	log.Println("START UPDATE................................................")
	for _, row := range rows {
		err := storage.Update(
			"bagwords",
			Row{
				ID: row.ID,
				Data: []float64{
					1.2 * float64(-1), 3.4 * float64(-1),
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	log.Println("FINISH UPDATE................................................")
	log.Println("START READ UPDATE................................................")
	for _, row := range rows {
		r, err := storage.Read(
			"bagwords",
			row.ID,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", r.ID, " WITH DATA ", r.Data)
		}
	}
	log.Println("FINISH READ UPDATE................................................")
}

func TestRewriteFlow(t *testing.T) {
	var (
		count      = 10
		rows       = make([]Row, 0)
		createRows = func() {
			for i := 0; i < count; i++ {
				inc := strconv.Itoa(i)
				key := "Key" + inc
				vector := make([]float64, count)
				for j := 0; j < count; j++ {
					vector[j] = float64(j * i)
				}
				rows = append(rows, Row{
					ID:   key,
					Data: vector,
				})
			}
		}
		updateRows = func() {
			for i := 0; i < count; i++ {
				vector := rows[i].Data.([]float64)
				for j := 0; j < count; j++ {
					vector[j] = vector[j] * float64(-1)
				}
				rows[i].Data = append(rows[i].Data.([]float64), float64(0))
			}
			vector := make([]float64, count+1)
			for i := 0; i < count+1; i++ {
				vector[i] = float64(0)
			}
			rows = append(rows, Row{
				ID:   "Key" + strconv.Itoa(count),
				Data: vector,
			})
		}
	)
	createRows()
	log.Println("START WRITE................................................")
	for i := 0; i < len(rows); i++ {
		err := storage.Insert(
			"bagwords",
			rows[i],
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	log.Println("FINISH WRITE................................................")
	//
	log.Println("START READ................................................")
	for i := 0; i < len(rows); i++ {
		row, err := storage.Read(
			"bagwords",
			rows[i].ID,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", row.ID, " WITH DATA ", row.Data)
		}
	}
	log.Println("FINISH READ................................................")
	//
	updateRows()
	//
	log.Println("START REWRITE................................................")
	err := storage.Rewrite(
		"bagwords",
		rows,
	)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("FINISH REWRITE................................................")
	//
	log.Println("START READ................................................")
	for i := 0; i < len(rows); i++ {
		row, err := storage.Read(
			"bagwords",
			rows[i].ID,
		)
		if err != nil {
			t.Fatal(err)
		} else {
			log.Println("READ ID ", row.ID, " WITH DATA ", row.Data)
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
			Row{
				ID:   key,
				Data: data,
			},
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
		_, _ = storage.Read(
			"bench",
			key,
		)
		//if err != nil {
		//	log.Println("i = ",i, " err: ", err)
		//} else {
		//	// log.Println("READ ID ", row.ID, " WITH DATA ", row.Data)
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
			Row{
				ID: key,
				Data: []float64{
					1.2 * float64(-1), 3.4 * float64(-1),
				},
			},
		)
		//if err != nil {
		//	log.Println("i = ",i, " err: ", err)
		//}
	}
}

//func BenchmarkReadAll(b *testing.B) {
//	b.ReportAllocs()
//	for i := 0; i < b.N; i++ {
//		rows, err := storage.ReadAll("bench")
//		if err != nil {
//			log.Println(err)
//		}
//		if len(rows) == 0 {
//			log.Println("SIZE IS 0")
//		}
//	}
//}