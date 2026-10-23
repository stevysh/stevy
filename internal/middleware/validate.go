package middleware

import (
	"context"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect/v2"
	"google.golang.org/protobuf/proto"
)

// Validate returns a Connect interceptor that runs protovalidate on every
// request message and rejects with InvalidArgument on failure.
func Validate() (connect.ServerInterceptor, error) {
	v, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			return next(ctx, spec, &validatingStream{ServerStream: stream, validator: v})
		}
	}, nil
}

// validatingStream validates each request message as it is received.
type validatingStream struct {
	connect.ServerStream
	validator protovalidate.Validator
}

func (s *validatingStream) Receive(msg any) error {
	if err := s.ServerStream.Receive(msg); err != nil {
		return err
	}
	m, ok := msg.(proto.Message)
	if !ok {
		return nil
	}
	if err := s.validator.Validate(m); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err.Error()).WithCause(err)
	}
	return nil
}
