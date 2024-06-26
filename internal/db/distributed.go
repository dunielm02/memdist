package db

import (
	"crypto/tls"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

type DistributedDB struct {
	raft *raft.Raft
	db   *DB
}

func NewDistributedDB(baseDir string) (*DistributedDB, error) {
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

	transport := raft.NewNetworkTransport()

	raft, err := raft.NewRaft(raftConfig, fsm, ldb, sdb, snapshotStore, nil)

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

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func newStreamLayer(ln net.Listener, serverTLSConfig *tls.Config, peerTLSConfig *tls.Config) *StreamLayer {
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
