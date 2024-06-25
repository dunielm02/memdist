package server_test

import (
	"context"
	"net"
	"testing"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/dunielm02/memdist/internal/auth"
	"github.com/dunielm02/memdist/internal/config"
	"github.com/dunielm02/memdist/internal/db"
	"github.com/dunielm02/memdist/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestServer(t *testing.T) {
	rootClient, nobodyClient := setup(t)

	testCases := map[string]string{
		"foo":  "bar",
		"john": "doe",
	}

	for k, v := range testCases {
		_, err := rootClient.Set(context.Background(), &api.SetRequest{
			Key:   k,
			Value: v,
		})
		require.NoError(t, err)
	}

	for k, v := range testCases {
		res, err := rootClient.Get(context.Background(), &api.GetRequest{
			Key: k,
		})
		require.NoError(t, err)
		require.Equal(t, res.Value, v)
	}

	// data, err := rootClient.ConsumeStream(context.Background(), &api.ConsumeRequest{})
	// require.NoError(t, err)

	// var cont = 0
	// for i := 0; i < cont; i++ {
	// 	_, err := data.Recv()
	// 	require.NoError(t, err)
	// }

	rootClient.Delete(context.Background(), &api.DeleteRequest{
		Key: "foo",
	})

	_, err := rootClient.Get(context.Background(), &api.GetRequest{
		Key: "foo",
	})
	require.Error(t, err)

	_, err = nobodyClient.Get(context.Background(), &api.GetRequest{
		Key: "john",
	})
	require.Error(t, err)
}

func setup(t *testing.T) (api.DatabaseClient, api.DatabaseClient) {
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

	authorizer, err := auth.New()
	require.NoError(t, err)

	srv, err := server.New(server.Config{
		Data:       db.New(),
		Authorizer: authorizer,
	}, serverOpts...)
	require.NoError(t, err)

	go func() {
		err := srv.Serve(ln)
		if err != nil {
			panic(err)
		}
	}()

	var newClient = func(cert, key string) api.DatabaseClient {
		tlsConfig, err = config.GetTlsConfig(config.TLSConfig{
			CertFile: cert,
			KeyFile:  key,
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

	return newClient(config.RootCertFile, config.RootKeyFile),
		newClient(config.NobodyCertFile, config.NobodyKeyFile)
}
