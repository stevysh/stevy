package service

import (
	"context"

	"connectrpc.com/connect"

	queuev1 "github.com/stevysh/stevy/gen/stevy/v1"
)

type QueueService struct {
	driver Driver
}

func NewQueue(driver Driver) *QueueService {
	return &QueueService{driver: driver}
}

func (s *QueueService) GetQueue(ctx context.Context, req *connect.Request[queuev1.GetQueueRequest]) (*connect.Response[queuev1.GetQueueResponse], error) {
	info, err := s.driver.QueueInfo(ctx, req.Msg.GetName())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	c := info.CountsByStatus
	return connect.NewResponse(&queuev1.GetQueueResponse{
		Available: c["available"],
		Pending:   c["pending"],
		Scheduled: c["scheduled"],
		Running:   c["running"],
		Retryable: c["retryable"],
		Completed: c["completed"],
		Discarded: c["discarded"],
		Cancelled: c["cancelled"],
		Paused:    info.Paused,
	}), nil
}
