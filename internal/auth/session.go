package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

const (
	sessionCookie = "queue_session"
	sessionMaxAge = 30 * 24 * time.Hour
)

type ctxKey string

const (
	userIDKey   ctxKey = "user_id"
	workerIDKey ctxKey = "worker_id"
)

// SessionManager issues HMAC-signed cookies containing the user ID.
type SessionManager struct {
	secret []byte
}

func NewSessionManager(secret []byte) *SessionManager {
	return &SessionManager{secret: secret}
}

func (s *SessionManager) Set(w http.ResponseWriter, userID string) {
	value := userID + "." + s.sign(userID)

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionMaxAge.Seconds()),
	})
}

func (s *SessionManager) Clear(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1,
	})
}

// UserID returns the authenticated user ID from the request cookie, or "".
func (s *SessionManager) UserID(r *http.Request) string {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return ""
	}
	id, sig, ok := strings.Cut(c.Value, ".")
	if !ok {
		return ""
	}
	if !hmac.Equal([]byte(sig), []byte(s.sign(id))) {
		return ""
	}
	return id
}

func (s *SessionManager) sign(data string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

// Context helpers for API-key middleware → RPC handlers.

func ContextWithUserID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, userIDKey, id)
}

func UserIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

func ContextWithWorkerID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, workerIDKey, id)
}

func WorkerIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(workerIDKey).(string); ok {
		return v
	}
	return ""
}
