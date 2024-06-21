package db

import (
	"fmt"
	"sync"

	"github.com/dunielm02/memdist/api/v1"
)

type DB struct {
	data sync.Map
}

func New() *DB {
	return &DB{
		data: sync.Map{},
	}
}

func (db *DB) Set(req *api.SetRequest) error {
	db.data.Store(req.Key, req.Value)

	return nil
}

type KeyNotFound error

func (db *DB) Get(req *api.GetRequest) (*api.GetResponse, error) {
	v, ok := db.data.Load(req.Key)

	if !ok {
		return &api.GetResponse{}, KeyNotFound(fmt.Errorf("key not found"))
	}

	return &api.GetResponse{Value: v.(string)}, nil
}

func (db *DB) Delete(req *api.DeleteRequest) error {
	db.data.Delete(req.Key)

	return nil
}

func (db *DB) Read() ([]*api.ConsumeResponse, error) {
	var ret []*api.ConsumeResponse
	db.data.Range(func(key, value any) bool {
		ret = append(ret, &api.ConsumeResponse{
			Key:   key.(string),
			Value: value.(string),
		})
		return true
	})

	return ret, nil
}

func (db *DB) Reset() error {
	db.data = sync.Map{}

	return nil
}
