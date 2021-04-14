package gosimstor

import (
	"errors"
	concurrentMap "github.com/streamrail/concurrent-map"
	"runtime"
)

type FromStringToType func(data string) (interface{}, error)
type ToString func(data interface{}) (string, error)

type Storage struct {
	fileStorage concurrentMap.ConcurrentMap
}

func NewStorage(constructors ...FileProviderConstructor) (*Storage, error) {
	storage := new(Storage)
	storage.fileStorage = concurrentMap.New()
	for i := 0; i < len(constructors); i++ {
		constructor := constructors[i]
		provider, err := constructor()
		if err != nil {
			return nil, err
		}
		storage.fileStorage.Set(provider.fileStorageName, provider)
	}
	return storage, nil
}

func Destructor(storage *Storage) error {
	for item := range storage.fileStorage.IterBuffered() {
		dataModel := item.Val.(*fileProvider)
		mx := *dataModel.mx
		mx.Lock()
		err := dataModel.file.Close()
		if err != nil {
			return err
		}
		dataModel = nil
		mx.Unlock()
	}
	storage = nil
	runtime.GC()
	return nil
}

func (storage *Storage) Insert(providerKey string, row Row) error {
	var (
		provider *fileProvider
	)
	if inter, exist := storage.fileStorage.Get(providerKey); !exist {
		return errors.New("FILE PROVIDER ISN'T EXIST. ")
	} else {
		provider = inter.(*fileProvider)
	}
	return provider.Insert(row)
}

func (storage *Storage) Read(providerKey string, id interface{}) (Row, error) {
	var (
		row      Row
		provider *fileProvider
	)
	if inter, exist := storage.fileStorage.Get(providerKey); !exist {
		return row, errors.New("FILE PROVIDER ISN'T EXIST. ")
	} else {
		provider = inter.(*fileProvider)
	}
	return provider.Read(id)
}

func (storage *Storage) Update(providerKey string, row Row) error {
	var (
		provider *fileProvider
	)
	if inter, exist := storage.fileStorage.Get(providerKey); !exist {
		return errors.New("FILE PROVIDER ISN'T EXIST. ")
	} else {
		provider = inter.(*fileProvider)
	}
	return provider.Update(row)
}

func (storage *Storage) Rewrite(providerKey string, rows []Row) error {
	var (
		provider *fileProvider
	)
	if inter, exist := storage.fileStorage.Get(providerKey); !exist {
		return errors.New("FILE PROVIDER ISN'T EXIST. ")
	} else {
		provider = inter.(*fileProvider)
	}
	return provider.Rewrite(rows)
}

func (storage *Storage) GetIDs(providerKey string) ([]string, error) {
	var (
		provider *fileProvider
	)
	if inter, exist := storage.fileStorage.Get(providerKey); !exist {
		return nil, errors.New("FILE PROVIDER ISN'T EXIST. ")
	} else {
		provider = inter.(*fileProvider)
	}
	return provider.GetIDs(), nil
}

