package auth

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/stevysh/stevy/internal/db"
)

// APIKeyHandler exposes CRUD endpoints for client API keys. Session-gated.
type APIKeyHandler struct {
	db       *db.DB
	sessions *SessionManager
}

func NewAPIKeyHandler(database *db.DB, sessions *SessionManager) *APIKeyHandler {
	return &APIKeyHandler{db: database, sessions: sessions}
}

func (h *APIKeyHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /keys", h.create)
	mux.HandleFunc("GET /keys/list", h.list)
	mux.HandleFunc("DELETE /keys/{id}", h.delete)
}

type createKeyResponse struct {
	ID        string `json:"id"`
	Label     string `json:"label"`
	Key       string `json:"key"`
	KeyPrefix string `json:"key_prefix"`
}

func (h *APIKeyHandler) create(w http.ResponseWriter, r *http.Request) {
	userID := h.sessions.UserID(r)
	if userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req struct {
		Label string `json:"label"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	req.Label = strings.TrimSpace(req.Label)
	if req.Label == "" {
		http.Error(w, "label required", http.StatusBadRequest)
		return
	}
	plaintext, key, err := h.db.CreateAPIKey(r.Context(), userID, req.Label)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, createKeyResponse{
		ID:        key.ID,
		Label:     key.Label,
		Key:       plaintext,
		KeyPrefix: key.KeyPrefix,
	})
}

func (h *APIKeyHandler) list(w http.ResponseWriter, r *http.Request) {
	userID := h.sessions.UserID(r)
	if userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	keys, err := h.db.ListAPIKeys(r.Context(), userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, keys)
}

func (h *APIKeyHandler) delete(w http.ResponseWriter, r *http.Request) {
	userID := h.sessions.UserID(r)
	if userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if err := h.db.DeleteAPIKey(r.Context(), userID, id); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
