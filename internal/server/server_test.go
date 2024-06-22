package server_test

import (
	"context"
	"net"
	"testing"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/dunielm02/memdist/internal/config"
	"github.com/dunielm02/memdist/internal/db"
	"github.com/dunielm02/memdist/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := config.GetTlsConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		KeyFile:  config.ServerKeyFile,
		CertFile: config.ServerCertFile,
		Server:   true,
		// ServerAddress: ln.Addr().String(),
	})
	require.NoError(t, err)

	serverOpts := []grpc.ServerOption{
		grpc.Creds(credentials.NewTLS(tlsConfig)),
	}

	srv, err := server.New(server.Config{
		Data: db.New(),
	}, serverOpts...)
	require.NoError(t, err)

	go func() {
		err := srv.Serve(ln)
		if err != nil {
			panic(err)
		}
	}()

	tlsConfig, err = config.GetTlsConfig(config.TLSConfig{
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
		CAFile:   config.CAFile,
		Server:   false,
	})
	require.NoError(t, err)
	clientOpts := grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	conn, err := grpc.NewClient(ln.Addr().String(), clientOpts)
	require.NoError(t, err)

	client := api.NewDatabaseClient(conn)

	return client
}
