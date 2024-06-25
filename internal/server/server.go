package server

import (
	"context"

	"github.com/dunielm02/memdist/api/v1"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type KeyValueDb interface {
	Get(*api.GetRequest) (*api.GetResponse, error)
	Set(*api.SetRequest) error
	Delete(*api.DeleteRequest) error
}

const (
	getAction      = "get"
	setAction      = "set"
	deleteAction   = "delete"
	objectWildCard = "*"
)

type Config struct {
	Authorizer Authorizer
	Data       KeyValueDb
}

type Authorizer interface {
	Authorize(sub string, obj string, act string) error
}

var _ api.DatabaseServer = &grpcServer{}

type grpcServer struct {
	api.UnimplementedDatabaseServer
	Config
}

func New(c Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		grpc.ChainUnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(extractAuthData),
		),
		grpc.ChainStreamInterceptor(
			grpc_auth.StreamServerInterceptor(extractAuthData),
		),
	)

	gsrv := grpc.NewServer(opts...)
	srv := newGrpcServer(c)

	api.RegisterDatabaseServer(gsrv, srv)

	return gsrv, nil
}

func newGrpcServer(c Config) *grpcServer {
	return &grpcServer{
		Config: c,
	}
}

func (s *grpcServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildCard, getAction); err != nil {
		return nil, err
	}
	res, err := s.Data.Get(req)

	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	return res, err
}

func (s *grpcServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildCard, setAction); err != nil {
		return nil, err
	}
	err := s.Data.Set(req)

	if err != nil {
		return &api.SetResponse{},
			status.Error(codes.Internal, "something went wrong while setting the value: "+err.Error())
	}

	return &api.SetResponse{}, nil
}

func (s *grpcServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildCard, deleteAction); err != nil {
		return nil, err
	}
	err := s.Data.Delete(req)

	if err != nil {
		return &api.DeleteResponse{},
			status.Error(codes.Internal, "something went wrong while deleting the value: "+err.Error())
	}

	return &api.DeleteResponse{}, nil
}

type username struct{}

func extractAuthData(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, username{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName

	ctx = context.WithValue(ctx, username{}, subject)
	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(username{}).(string)
}
