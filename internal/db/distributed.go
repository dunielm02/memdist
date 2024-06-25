package db

import (
	"io"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type DistributedDB struct {
	raft *raft.Raft
	db   *DB
}

func NewDistributedLog() (*DistributedDB, error) {
	// raftConfig := raft.DefaultConfig()
	// raft := raft.NewRaft(raftConfig)

	return nil, nil
}

var _ raft.FSM = &fsm{}

type fsm struct {
	db *DB
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{
		reader: f.db.Read(),
	}, nil
}

func (f *fsm) Restore(snapshot io.ReadCloser) error {
	f.db.Reset()
	for {
		s := make([]byte, KeyValueSize)
		_, err := snapshot.Read(s)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		size := enc.Uint32(s)
		data := make([]byte, int(size))
		_, err = snapshot.Read(data)
		if err != nil {
			return err
		}

		var record = &api.Record{}
		proto.Unmarshal(data, record)

		err = f.db.Set(&api.SetRequest{
			Key:   record.Key,
			Value: record.Value,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

var _ raft.FSMSnapshot = &snapshot{}

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
