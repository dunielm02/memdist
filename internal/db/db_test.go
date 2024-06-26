package db_test

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/dunielm02/memdist/internal/db"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var enc = binary.BigEndian

func TestDb(t *testing.T) {
	data := db.NewDB()

	testCases := map[string]string{
		"foo":  "bar",
		"john": "doe",
	}

	for k, v := range testCases {
		data.Set(&api.SetRequest{
			Key:   k,
			Value: v,
		})
	}

	for k, v := range testCases {
		res, err := data.Get(&api.GetRequest{Key: k})
		require.NoError(t, err)
		require.Equal(t, res.Value, v)
	}

	read := data.Read()
	for {
		s := make([]byte, db.KeyValueSize)
		n, err := read.Read(s)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, n, db.KeyValueSize)

		size := enc.Uint32(s)
		data := make([]byte, int(size))
		n, err = read.Read(data)
		require.NoError(t, err)
		require.Equal(t, uint32(n), size)

		var record = &api.Record{}
		proto.Unmarshal(data, record)

		require.Equal(t, testCases[record.Key], record.Value)
	}

	err := data.Delete(&api.DeleteRequest{Key: "foo"})
	require.NoError(t, err)

	_, err = data.Get(&api.GetRequest{Key: "foo"})
	require.Error(t, err)

	err = data.Reset()
	require.NoError(t, err)

	_, err = data.Get(&api.GetRequest{Key: "john"})
	require.Error(t, err)
}
