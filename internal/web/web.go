package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/stevysh/stevy/internal/auth"
	"github.com/stevysh/stevy/internal/db"
	"github.com/stevysh/stevy/internal/service"
)

//go:embed templates/*.html
var templatesFS embed.FS

// allStatuses is the canonical order for filter pills.
var allStatuses = []string{"pending", "scheduled", "available", "running", "retryable", "cancelled", "discarded", "completed"}

func validStatus(s string) bool {
	for _, v := range allStatuses {
		if v == s {
			return true
		}
	}
	return false
}

type Handler struct {
	db       *db.DB
	sessions *auth.SessionManager
	driver   service.Driver
	tmpl     *template.Template            // standalone pages: login, job
	pages    map[string]*template.Template // dashboard pages composed with layout.html
}

func NewHandler(database *db.DB, sessions *auth.SessionManager, driver service.Driver) *Handler {
	h := &Handler{
		db:       database,
		sessions: sessions,
		driver:   driver,
		tmpl:     template.Must(template.ParseFS(templatesFS, "templates/login.html")),
		pages:    map[string]*template.Template{},
	}
	for _, p := range []string{"jobs", "queues", "workers", "keys", "job"} {
		h.pages[p] = template.Must(template.ParseFS(
			templatesFS,
			"templates/layout.html",
			"templates/"+p+".html",
		))
	}
	return h
}

// layoutData carries fields used by layout.html (header + nav).
// Embed it into per-page data structs.
type layoutData struct {
	User      *db.User
	ActiveTab string
	Title     string
}

type pageData struct {
	layoutData
	Jobs         []jobListRow
	Queues       []queueRow
	Workers      []workerRow
	StatusFilter string                 // active status filter on the jobs page ("" = all)
	StatusCounts map[string]int32       // per-status counts for filter pills
	StatusTotal  int32                  // sum across all statuses
	Statuses     []string               // ordered list of valid statuses for rendering pills
}

type queueRow struct {
	Name      string
	Paused    bool
	Available int32
	Running   int32
	Scheduled int32
	Retryable int32
	Completed int32
	Discarded int32
	Cancelled int32
	Pending   int32
}

type jobListRow struct {
	ID        string
	Queue     string
	Kind      string
	Status    string
	CreatedAt string
}

// Index renders login.html when unauthenticated, jobs page otherwise.
func (h *Handler) Index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	user := h.currentUser(r)
	if user == nil {
		h.renderStandalone(w, "login.html", nil)
		return
	}

	status := r.URL.Query().Get("status")
	if !validStatus(status) {
		status = ""
	}

	data := pageData{
		layoutData:   layoutData{User: user, ActiveTab: "jobs"},
		StatusFilter: status,
		Statuses:     allStatuses,
	}

	if jobs, err := h.driver.ListJobs(r.Context(), "", status, 50, ""); err == nil {
		for _, j := range jobs {
			data.Jobs = append(data.Jobs, jobListRow{
				ID:        j.ID,
				Queue:     j.Queue,
				Kind:      j.Kind,
				Status:    j.Status,
				CreatedAt: j.CreatedAt.Local().Format(time.DateTime),
			})
		}
	}
	if counts, err := h.driver.JobCountsByStatus(r.Context()); err == nil {
		data.StatusCounts = counts
		for _, n := range counts {
			data.StatusTotal += n
		}
	}
	h.renderPage(w, "jobs", data)
}

// QueuesPage renders the queues table.
func (h *Handler) QueuesPage(w http.ResponseWriter, r *http.Request) {
	user := h.requireUser(w, r)
	if user == nil {
		return
	}

	data := pageData{layoutData: layoutData{User: user, ActiveTab: "queues"}}
	if queues, err := h.driver.ListQueues(r.Context()); err == nil {
		for _, q := range queues {
			data.Queues = append(data.Queues, queueRow{
				Name:      q.Name,
				Paused:    q.Paused,
				Available: q.CountsByStatus["available"],
				Running:   q.CountsByStatus["running"],
				Scheduled: q.CountsByStatus["scheduled"],
				Retryable: q.CountsByStatus["retryable"],
				Completed: q.CountsByStatus["completed"],
				Discarded: q.CountsByStatus["discarded"],
				Cancelled: q.CountsByStatus["cancelled"],
				Pending:   q.CountsByStatus["pending"],
			})
		}
	}
	h.renderPage(w, "queues", data)
}

// WorkersPage renders the workers shell; the list is populated by JS.
func (h *Handler) WorkersPage(w http.ResponseWriter, r *http.Request) {
	user := h.requireUser(w, r)
	if user == nil {
		return
	}
	h.renderPage(w, "workers", pageData{layoutData: layoutData{User: user, ActiveTab: "workers"}})
}

// WorkersJSON returns the worker list as JSON for the workers page JS.
func (h *Handler) WorkersJSON(w http.ResponseWriter, r *http.Request) {
	if h.sessions.UserID(r) == 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	workers, err := h.db.ListWorkers(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type row struct {
		ID         int64  `json:"id"`
		Name       string `json:"name"`
		CreatedBy  string `json:"created_by"`
		CreatedAt  string `json:"created_at"`
		LastSeenAt string `json:"last_seen_at"`
		Active     bool   `json:"active"`
	}
	out := make([]row, 0, len(workers))
	for _, ws := range workers {
		r := row{
			ID:        ws.ID,
			Name:      ws.Name,
			CreatedBy: ws.CreatedBy,
			CreatedAt: ws.CreatedAt.Local().Format(time.DateTime),
		}
		if ws.LastSeenAt != nil {
			r.LastSeenAt = ws.LastSeenAt.Local().Format(time.DateTime)
			r.Active = time.Since(*ws.LastSeenAt) < 90*time.Second
		}
		out = append(out, r)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// CreateWorker creates a new worker key and returns the plaintext key once.
func (h *Handler) CreateWorker(w http.ResponseWriter, r *http.Request) {
	userID := h.sessions.UserID(r)
	if userID == 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	workerID, plaintext, err := h.db.CreateWorkerKey(r.Context(), userID, req.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": workerID, "name": req.Name, "key": plaintext})
}

// DeleteWorker removes a worker key and its heartbeat row.
func (h *Handler) DeleteWorker(w http.ResponseWriter, r *http.Request) {
	userID := h.sessions.UserID(r)
	if userID == 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	workerIDInt, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if err := h.db.DeleteWorker(r.Context(), userID, workerIDInt); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// KeysPage renders the API keys management page.
func (h *Handler) KeysPage(w http.ResponseWriter, r *http.Request) {
	user := h.requireUser(w, r)
	if user == nil {
		return
	}
	h.renderPage(w, "keys", pageData{layoutData: layoutData{User: user, ActiveTab: "keys"}})
}

type workerRow struct {
	ID         string
	Label      string
	CreatedBy  string
	CreatedAt  string
	LastSeenAt string
	Active     bool
}

type jobErrorEntry struct {
	At      string
	Attempt int32
	Error   string
}

type jobPageData struct {
	layoutData
	ID          string
	Status      string
	Kind        string
	Queue       string
	Progress    int32
	Attempt     int32
	CreatedAt   string
	AttemptedAt string
	FinalizedAt string
	Result      string
	Payload     string
	Errors      []jobErrorEntry
	Polling     bool
}

// JobPage renders a status page for a single job by id. Requires auth.
func (h *Handler) JobPage(w http.ResponseWriter, r *http.Request) {
	user := h.requireUser(w, r)
	if user == nil {
		return
	}
	id := r.PathValue("id")
	row, err := h.driver.GetJob(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	data := jobPageData{
		layoutData: layoutData{
			User:      user,
			ActiveTab: "jobs",
			Title:     "Job " + row.ID,
		},
		ID:        row.ID,
		Status:    row.Status,
		Kind:      row.Kind,
		Queue:     row.Queue,
		Progress:  int32(row.Progress),
		Attempt:   int32(row.Attempt),
		CreatedAt: row.CreatedAt.Local().Format(time.DateTime),
	}

	if row.AttemptedAt != nil {
		data.AttemptedAt = row.AttemptedAt.Local().Format(time.DateTime)
	}
	if row.FinalizedAt != nil {
		data.FinalizedAt = row.FinalizedAt.Local().Format(time.DateTime)
	}
	if len(row.ResultJSON) > 0 {
		data.Result = prettyJSON(row.ResultJSON)
	}
	if len(row.PayloadJSON) > 0 {
		data.Payload = prettyJSON(row.PayloadJSON)
	}
	var errArr []struct {
		At      time.Time `json:"at"`
		Attempt int       `json:"attempt"`
		Error   string    `json:"error"`
	}
	if json.Unmarshal(row.ErrorsJSON, &errArr) == nil {
		for _, e := range errArr {
			data.Errors = append(data.Errors, jobErrorEntry{
				At:      e.At.Local().Format(time.DateTime),
				Attempt: int32(e.Attempt),
				Error:   e.Error,
			})
		}
	}

	switch data.Status {
	case "available", "pending", "scheduled", "running", "retryable":
		data.Polling = true
	}

	h.renderPage(w, "job", data)
}

// RunScheduler promotes scheduled/retryable jobs whose scheduled_at has passed.
func (h *Handler) RunScheduler(w http.ResponseWriter, r *http.Request) {
	n, err := h.driver.PromoteScheduledJobs(r.Context(), 1000)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"promoted":%d}`, n)
}

// PauseQueue/ResumeQueue toggle a queue's paused state.
func (h *Handler) PauseQueue(w http.ResponseWriter, r *http.Request) {
	if h.sessions.UserID(r) == 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := h.driver.PauseQueue(r.Context(), r.PathValue("name")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/queues", http.StatusSeeOther)
}

func (h *Handler) ResumeQueue(w http.ResponseWriter, r *http.Request) {
	if h.sessions.UserID(r) == 0 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if err := h.driver.ResumeQueue(r.Context(), r.PathValue("name")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/queues", http.StatusSeeOther)
}

// currentUser returns the logged-in user, or nil if not authenticated.
func (h *Handler) currentUser(r *http.Request) *db.User {
	userID := h.sessions.UserID(r)
	if userID == 0 {
		return nil
	}
	user, err := h.db.GetUserByID(r.Context(), userID)
	if err != nil {
		return nil
	}
	return user
}

// requireUser redirects to / if not authenticated. Returns nil in that case.
func (h *Handler) requireUser(w http.ResponseWriter, r *http.Request) *db.User {
	user := h.currentUser(r)
	if user == nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return nil
	}
	return user
}

func (h *Handler) renderPage(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t, ok := h.pages[name]
	if !ok {
		http.Error(w, "unknown page", http.StatusInternalServerError)
		return
	}
	if err := t.ExecuteTemplate(w, "layout.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *Handler) renderStandalone(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func prettyJSON(b []byte) string {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return string(b)
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return string(b)
	}
	return string(out)
}
