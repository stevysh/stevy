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

func (s *QueueService) ListQueues(ctx context.Context, _ *connect.Request[queuev1.ListQueuesRequest]) (*connect.Response[queuev1.ListQueuesResponse], error) {
	summaries, err := s.driver.ListQueues(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	queues := make([]*queuev1.Queue, 0, len(summaries))
	for _, s := range summaries {
		queues = append(queues, &queuev1.Queue{
			Name:   s.Name,
			Paused: s.Paused,
			Stats:  countsToStats(s.CountsByStatus),
		})
	}
	return connect.NewResponse(&queuev1.ListQueuesResponse{Queues: queues}), nil
}

func (s *QueueService) GetQueue(ctx context.Context, req *connect.Request[queuev1.GetQueueRequest]) (*connect.Response[queuev1.GetQueueResponse], error) {
	info, err := s.driver.QueueInfo(ctx, req.Msg.GetName())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&queuev1.GetQueueResponse{
		Queue: &queuev1.Queue{
			Name:   req.Msg.GetName(),
			Paused: info.Paused,
			Stats:  countsToStats(info.CountsByStatus),
		},
	}), nil
}

func countsToStats(c map[string]int32) *queuev1.QueueStats {
	return &queuev1.QueueStats{
		Pending:   c["pending"],
		Scheduled: c["scheduled"],
		Available: c["available"],
		Running:   c["running"],
		Retryable: c["retryable"],
		Cancelled: c["cancelled"],
		Discarded: c["discarded"],
		Completed: c["completed"],
	}
}

func (s *QueueService) PauseQueue(ctx context.Context, req *connect.Request[queuev1.PauseQueueRequest]) (*connect.Response[queuev1.PauseQueueResponse], error) {
	if err := s.driver.PauseQueue(ctx, req.Msg.GetName()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&queuev1.PauseQueueResponse{}), nil
}

func (s *QueueService) ResumeQueue(ctx context.Context, req *connect.Request[queuev1.ResumeQueueRequest]) (*connect.Response[queuev1.ResumeQueueResponse], error) {
	if err := s.driver.ResumeQueue(ctx, req.Msg.GetName()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&queuev1.ResumeQueueResponse{}), nil
}
