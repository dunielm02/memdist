package server_test

import (
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
	_ = setup(t)
}

func setup(t *testing.T) *api.DatabaseClient {
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

	return &client
}
