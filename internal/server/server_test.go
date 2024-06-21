package server_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/dunielm02/memdist/internal/db"
	"github.com/dunielm02/memdist/internal/server"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer(t *testing.T) {
	client := setup(t)

	testCases := map[string]string{
		"foo":  "bar",
		"john": "doe",
	}

	for k, v := range testCases {
		_, err := client.Set(context.Background(), &api.SetRequest{
			Key:   k,
			Value: v,
		})
		require.NoError(t, err)
	}

	for k, v := range testCases {
		res, err := client.Get(context.Background(), &api.GetRequest{
			Key: k,
		})
		require.NoError(t, err)
		require.Equal(t, res.Value, v)
	}

	data, err := client.ConsumeStream(context.Background(), &api.ConsumeRequest{})
	require.NoError(t, err)

	var cont = 0
	for i := 0; i < cont; i++ {
		_, err := data.Recv()
		require.NoError(t, err)
	}

	client.Delete(context.Background(), &api.DeleteRequest{
		Key: "foo",
	})

	_, err = client.Get(context.Background(), &api.GetRequest{
		Key: "foo",
	})
	require.Error(t, err)
}

func setup(t *testing.T) api.DatabaseClient {
	t.Helper()
	srv, err := server.New(server.Config{
		Data: db.New(),
	})
	require.NoError(t, err)

	port := dynaport.Get(1)[0]
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	require.NoError(t, err)

	go func() {
		err := srv.Serve(ln)
		if err != nil {
			panic(err)
		}
	}()

	opts := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.NewClient(ln.Addr().String(), opts)
	require.NoError(t, err)

	client := api.NewDatabaseClient(conn)

	return client
}
