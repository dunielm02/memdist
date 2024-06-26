package db

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	SetRequestType    byte = 0
	DeleteRequestType byte = 1
)

type Config struct {
	raft.Config
	StreamLayer *StreamLayer
	Bootstrap   bool
}

type DistributedDB struct {
	Config
	raft *raft.Raft
	db   *DB
}

func NewDistributedDB(baseDir string, cfg Config) (*DistributedDB, error) {
	distDB := &DistributedDB{
		db: NewDB(),
	}
	raftConfig := raft.DefaultConfig()
	fsm := &fsm{
		db: distDB.db,
	}

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.db"))
	if err != nil {
		return nil, err
	}
	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "store.db"))
	if err != nil {
		return nil, err
	}

	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(baseDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return nil, err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		cfg.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	distDB.raft, err = raft.NewRaft(raftConfig, fsm, ldb, sdb, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (d *DistributedDB) Join(name string, addrs string) error {
	
}

func (d *DistributedDB) Leave(name string) error {

}

func (d *DistributedDB) Get(req *api.GetRequest) (*api.GetResponse, error) {
	return d.db.Get(req)
}

func (d *DistributedDB) Set(req *api.SetRequest) error {
	_, err := d.apply(SetRequestType, req)
	if err != nil {
		return err
	}
	return nil
}

func (d *DistributedDB) Delete(req *api.DeleteRequest) error {
	_, err := d.apply(DeleteRequestType, req)
	if err != nil {
		return err
	}
	return nil
}

func (d *DistributedDB) apply(requestType byte, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{requestType})
	if err != nil {
		return nil, err
	}
	encoded, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(encoded)
	if err != nil {
		return nil, err
	}

	future := d.raft.Apply(buf.Bytes(), 10*time.Second)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

var _ raft.FSM = &fsm{}

type fsm struct {
	db *DB
}

func (f *fsm) Apply(log *raft.Log) interface{} {
	reqType := log.Data[0]
	switch reqType {
	case SetRequestType:
		return f.applySetRequest(log.Data[1:])
	case DeleteRequestType:
		return f.applyDeleteRequest(log.Data[1:])
	}
	return status.Error(codes.Internal, "Something went wrong applying the request")
}

func (f *fsm) applySetRequest(req []byte) error {
	setReq := &api.SetRequest{}
	err := proto.Unmarshal(req, setReq)
	if err != nil {
		return err
	}
	err = f.db.Set(setReq)

	return err
}

func (f *fsm) applyDeleteRequest(req []byte) error {
	delReq := &api.DeleteRequest{}
	err := proto.Unmarshal(req, delReq)
	if err != nil {
		return err
	}
	err = f.db.Delete(delReq)

	return err
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

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig *tls.Config, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

func (s *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}

	tlsConn := tls.Client(conn, s.peerTLSConfig)

	return tlsConn, nil
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	tlsConn := tls.Server(conn, s.serverTLSConfig)

	return tlsConn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
