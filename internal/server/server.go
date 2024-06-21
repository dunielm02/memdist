package server

import (
	"context"

	"github.com/dunielm02/memdist/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KeyValueDb interface {
	Get(*api.GetRequest) (*api.GetResponse, error)
	Set(*api.SetRequest) error
	Delete(*api.DeleteRequest) error
	Read() ([]*api.ConsumeResponse, error)
}

type Config struct {
	Data KeyValueDb
}

var _ api.DatabaseServer = &grpcServer{}

type grpcServer struct {
	api.UnimplementedDatabaseServer
	Config
}

func New(c Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
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
	res, err := s.Data.Get(req)

	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	return res, err
}

func (s *grpcServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	err := s.Data.Set(req)

	if err != nil {
		return &api.SetResponse{},
			status.Error(codes.Internal, "something went wrong while setting the value: "+err.Error())
	}

	return &api.SetResponse{}, nil
}

func (s *grpcServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	err := s.Data.Delete(req)

	if err != nil {
		return &api.DeleteResponse{},
			status.Error(codes.Internal, "something went wrong while deleting the value: "+err.Error())
	}

	return &api.DeleteResponse{}, nil
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, res api.Database_ConsumeStreamServer) error {
	Data, err := s.Data.Read()

	if err != nil {
		return status.Error(codes.Internal, "something went wrong while reading: "+err.Error())
	}

	for _, v := range Data {
		err := res.Send(v)
		if err != nil {
			return status.Error(codes.Internal, "something went wrong while sending on stream: "+err.Error())
		}
	}

	return nil
}
