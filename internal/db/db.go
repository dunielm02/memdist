package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/dunielm02/memdist/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var enc = binary.BigEndian

const (
	KeyValueSize = 4
)

type DB struct {
	data sync.Map
}

func NewDB() *DB {
	return &DB{
		data: sync.Map{},
	}
}

func (db *DB) Set(req *api.SetRequest) error {
	if len(req.Key) >= (1 << KeyValueSize) {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("the size of the Key is bigger than: %d", 1<<KeyValueSize))
	}
	if len(req.Value) >= (1 << KeyValueSize) {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("the size of the Value is bigger than: %d", 1<<KeyValueSize))
	}
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

func (db *DB) Read() io.Reader {
	var ret = bytes.NewBuffer([]byte{})
	db.data.Range(func(key, value any) bool {
		record := &api.Record{
			Key:   key.(string),
			Value: value.(string),
		}
		encoded, _ := proto.Marshal(record)
		binary.Write(ret, enc, uint32(len(encoded)))
		ret.Write(encoded)
		return true
	})

	return ret
}

func (db *DB) Reset() error {
	db.data = sync.Map{}

	return nil
}
