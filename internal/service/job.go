package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobv1 "github.com/stevysh/stevy/gen/stevy/v1"
	"github.com/stevysh/stevy/internal/auth"
)

type JobService struct {
	driver Driver
}

func NewJob(driver Driver) *JobService {
	return &JobService{driver: driver}
}

// ─────────────────────────── ListJobs ───────────────────────────

func (s *JobService) ListJobs(ctx context.Context, req *connect.Request[jobv1.ListJobsRequest]) (*connect.Response[jobv1.ListJobsResponse], error) {
	limit := int(req.Msg.GetLimit())
	offset := int(req.Msg.GetOffset())
	rows, err := s.driver.ListJobs(ctx, req.Msg.GetQueue(), req.Msg.GetStatus(), limit, offset)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	jobs := make([]*jobv1.Job, 0, len(rows))
	for i := range rows {
		jobs = append(jobs, rowToProto(&rows[i]))
	}
	return connect.NewResponse(&jobv1.ListJobsResponse{Jobs: jobs}), nil
}

// ─────────────────────────── CreateJob ───────────────────────────

func (s *JobService) CreateJob(ctx context.Context, req *connect.Request[jobv1.CreateJobRequest]) (*connect.Response[jobv1.CreateJobResponse], error) {
	m := req.Msg

	payloadBytes, _ := json.Marshal(m.GetPayload().AsMap())

	meta := map[string]any{}
	opts := CreateOpts{
		Queue:       m.GetQueue(),
		Kind:        m.GetKind(),
		PayloadJSON: payloadBytes,
	}
	if jo := m.GetOptions(); jo != nil {
		opts.Pending = jo.GetPending()
		if jo.GetDelay() > 0 {
			t := time.Now().Add(time.Duration(jo.GetDelay()) * time.Millisecond)
			opts.ScheduledAt = &t
		}
		if jo.GetAttempts() > 0 {
			opts.MaxAttempts = int(jo.GetAttempts())
		}
		if jo.GetPriority() > 0 {
			opts.Priority = clampPriority(int(jo.GetPriority()))
		}
		if bo := jo.GetBackoff(); bo != nil {
			meta["backoff_type"] = bo.GetType()
			meta["backoff_delay"] = bo.GetDelay()
			meta["backoff_jitter"] = bo.GetJitter()
		}
	}
	metaBytes, _ := json.Marshal(meta)
	opts.Metadata = metaBytes

	created, err := s.driver.CreateJob(ctx, opts)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&jobv1.CreateJobResponse{
		Job: rowToProto(created),
	}), nil
}

// ─────────────────────────── ClaimJob ───────────────────────────

func (s *JobService) ClaimJob(ctx context.Context, req *connect.Request[jobv1.ClaimJobRequest]) (*connect.Response[jobv1.ClaimJobResponse], error) {
	workerID := workerIDFromCtx(ctx)
	row, err := s.driver.ClaimJob(ctx, req.Msg.GetQueue(), workerID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if row == nil {
		return connect.NewResponse(&jobv1.ClaimJobResponse{}), nil
	}
	return connect.NewResponse(&jobv1.ClaimJobResponse{Job: rowToProto(row)}), nil
}

// ─────────────────────────── CompleteJob ───────────────────────────

func (s *JobService) CompleteJob(ctx context.Context, req *connect.Request[jobv1.CompleteJobRequest]) (*connect.Response[jobv1.CompleteJobResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	resultJSON, _ := json.Marshal(req.Msg.GetResult().AsMap())
	if err := s.driver.CompleteJob(ctx, id, resultJSON); err != nil {
		return nil, mapErr(err, "complete", id)
	}
	return connect.NewResponse(&jobv1.CompleteJobResponse{}), nil
}

// ─────────────────────────── FailJob ───────────────────────────

func (s *JobService) FailJob(ctx context.Context, req *connect.Request[jobv1.FailJobRequest]) (*connect.Response[jobv1.FailJobResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	backoffMs, err := s.computeBackoffMs(ctx, id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.driver.FailJob(ctx, id, req.Msg.GetError(), backoffMs); err != nil {
		return nil, mapErr(err, "fail", id)
	}
	return connect.NewResponse(&jobv1.FailJobResponse{}), nil
}

func (s *JobService) computeBackoffMs(ctx context.Context, id string) (int, error) {
	row, err := s.driver.GetJob(ctx, id)
	if err != nil {
		return 0, err
	}
	var meta map[string]any
	_ = json.Unmarshal(row.MetadataJSON, &meta)

	backoffType, _ := meta["backoff_type"].(string)
	jitter, _ := meta["backoff_jitter"].(bool)
	baseDelay := int64(1000)
	if v, ok := meta["backoff_delay"].(float64); ok {
		baseDelay = int64(v)
	}

	var ms int
	switch backoffType {
	case "exponential":
		ms = int(baseDelay * int64(math.Pow(2, float64(row.Attempt-1))))
	default:
		ms = int(baseDelay)
	}
	if jitter {
		ms = ms/2 + rand.Intn(ms/2+1)
	}
	return ms, nil
}

// ─────────────────────────── HeartbeatJob ───────────────────────────

func (s *JobService) HeartbeatJob(ctx context.Context, req *connect.Request[jobv1.HeartbeatJobRequest]) (*connect.Response[jobv1.HeartbeatJobResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.driver.HeartbeatJob(ctx, id); err != nil {
		return nil, mapErr(err, "heartbeat", id)
	}
	return connect.NewResponse(&jobv1.HeartbeatJobResponse{}), nil
}

// ─────────────────────────── CancelJob ───────────────────────────

func (s *JobService) CancelJob(ctx context.Context, req *connect.Request[jobv1.CancelJobRequest]) (*connect.Response[jobv1.CancelJobResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.driver.CancelJob(ctx, id); err != nil {
		return nil, mapErr(err, "cancel", id)
	}
	return connect.NewResponse(&jobv1.CancelJobResponse{}), nil
}

// ─────────────────────────── PromoteJob ───────────────────────────

func (s *JobService) PromoteJob(ctx context.Context, req *connect.Request[jobv1.PromoteJobRequest]) (*connect.Response[jobv1.PromoteJobResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.driver.PromoteJob(ctx, id); err != nil {
		return nil, mapErr(err, "promote", id)
	}
	return connect.NewResponse(&jobv1.PromoteJobResponse{}), nil
}

// ─────────────────────────── GetJob ───────────────────────────

func (s *JobService) GetJob(ctx context.Context, req *connect.Request[jobv1.GetJobRequest]) (*connect.Response[jobv1.GetJobResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	row, err := s.driver.GetJob(ctx, id)
	if err != nil {
		return nil, mapErr(err, "get", id)
	}
	return connect.NewResponse(&jobv1.GetJobResponse{Job: rowToProto(row)}), nil
}

// ─────────────────────────── SetJobProgress ───────────────────────────

func (s *JobService) SetJobProgress(ctx context.Context, req *connect.Request[jobv1.SetJobProgressRequest]) (*connect.Response[jobv1.SetJobProgressResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.driver.SetJobProgress(ctx, id, int(req.Msg.GetProgress())); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&jobv1.SetJobProgressResponse{}), nil
}

// ─────────────────────────── GetJobStatus ───────────────────────────

func (s *JobService) GetJobStatus(ctx context.Context, req *connect.Request[jobv1.GetJobStatusRequest]) (*connect.Response[jobv1.GetJobStatusResponse], error) {
	id, err := parseJobID(req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	row, err := s.driver.GetJobStatus(ctx, id)
	if err != nil {
		return nil, mapErr(err, "get-status", id)
	}

	resp := &jobv1.GetJobStatusResponse{
		Status:   row.Status,
		Progress: int32(row.Progress),
	}
	if errMsg := lastError(row.ErrorsJSON); errMsg != "" {
		resp.Error = &errMsg
	}
	return connect.NewResponse(resp), nil
}

// ─────────────────────────── BatchGetJobStatuses ───────────────────────────

func (s *JobService) BatchGetJobStatuses(ctx context.Context, req *connect.Request[jobv1.BatchGetJobStatusesRequest]) (*connect.Response[jobv1.BatchGetJobStatusesResponse], error) {
	ids := make([]string, 0, len(req.Msg.GetIds()))
	for _, id := range req.Msg.GetIds() {
		if id != "" {
			ids = append(ids, id)
		}
	}
	rows, err := s.driver.BatchGetJobStatuses(ctx, ids)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	entries := make([]*jobv1.BatchGetJobStatusesResponse_JobStatusEntry, 0, len(rows))
	for _, row := range rows {
		entry := &jobv1.BatchGetJobStatusesResponse_JobStatusEntry{
			Id:       row.ID,
			Status:   row.Status,
			Progress: int32(row.Progress),
		}
		if errMsg := lastError(row.ErrorsJSON); errMsg != "" {
			entry.Error = &errMsg
		}
		entries = append(entries, entry)
	}
	return connect.NewResponse(&jobv1.BatchGetJobStatusesResponse{Statuses: entries}), nil
}

// ─────────────────────────── Helpers ───────────────────────────

func rowToProto(row *JobRow) *jobv1.Job {
	var payloadMap map[string]any
	_ = json.Unmarshal(row.PayloadJSON, &payloadMap)
	payload, _ := structpb.NewStruct(payloadMap)

	j := &jobv1.Job{
		Id:        row.ID,
		Queue:     row.Queue,
		Kind:      row.Kind,
		Payload:   payload,
		Status:    row.Status,
		Attempt:   int32(row.Attempt),
		Progress:  int32(row.Progress),
		Errors:    parseAttemptErrors(row.ErrorsJSON),
		CreatedAt: timestamppb.New(row.CreatedAt),
	}
	if len(row.ResultJSON) > 0 {
		var rm map[string]any
		if json.Unmarshal(row.ResultJSON, &rm) == nil {
			j.Result, _ = structpb.NewStruct(rm)
		}
	}
	if row.AttemptedAt != nil {
		j.AttemptedAt = timestamppb.New(*row.AttemptedAt)
	}
	if !row.ScheduledAt.IsZero() {
		j.ScheduledAt = timestamppb.New(row.ScheduledAt)
	}
	if row.FinalizedAt != nil {
		j.FinalizedAt = timestamppb.New(*row.FinalizedAt)
	}
	return j
}

func parseAttemptErrors(errorsJSON []byte) []*jobv1.AttemptError {
	var arr []struct {
		At      time.Time `json:"at"`
		Attempt int       `json:"attempt"`
		Error   string    `json:"error"`
	}
	if err := json.Unmarshal(errorsJSON, &arr); err != nil || len(arr) == 0 {
		return nil
	}
	out := make([]*jobv1.AttemptError, len(arr))
	for i, e := range arr {
		out[i] = &jobv1.AttemptError{
			At:      timestamppb.New(e.At),
			Attempt: int32(e.Attempt),
			Error:   e.Error,
		}
	}
	return out
}

func clampPriority(p int) int {
	switch {
	case p >= 75:
		return 1
	case p >= 50:
		return 2
	case p >= 25:
		return 3
	default:
		return 4
	}
}

func lastError(errorsJSON []byte) string {
	var arr []map[string]any
	if err := json.Unmarshal(errorsJSON, &arr); err != nil || len(arr) == 0 {
		return ""
	}
	s, _ := arr[len(arr)-1]["error"].(string)
	return s
}

func parseJobID(s string) (string, error) {
	if s == "" {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("job id is required"))
	}
	return s, nil
}

func mapErr(err error, op string, id string) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("job %s not found", id))
	case errors.Is(err, ErrWrongState):
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("job %s cannot %s in current state", id, op))
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func workerIDFromCtx(ctx context.Context) int64 {
	return auth.WorkerIDFromContext(ctx)
}
