package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"github.com/dunielm02/memdist/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New() (*Authorizer, error) {
	e, err := casbin.NewEnforcer(config.ACLModelFile, config.ACLPolicyFile)
	if err != nil {
		return nil, err
	}
	return &Authorizer{
		enforcer: e,
	}, nil
}

func (auth *Authorizer) Authorize(sub, obj, act string) error {
	ok, err := auth.enforcer.Enforce(sub, obj, act)
	if err != nil {
		zap.L().Error(
			"error checking authentication",
			zap.String("subject", sub),
			zap.String("object", obj),
			zap.String("action", act),
		)
	}

	if !ok {
		msg := fmt.Sprintf("%s not permitted to %s to %s", sub, act, obj)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}

	return nil
}
