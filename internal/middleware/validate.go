package middleware

import (
	"context"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// Validate returns a Connect interceptor that runs protovalidate on every
// request message and rejects with InvalidArgument on failure.
func Validate() (connect.Interceptor, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			msg, ok := req.Any().(proto.Message)
			if !ok {
				return next(ctx, req)
			}
			if err := v.Validate(msg); err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
			return next(ctx, req)
		}
	}), nil
}
