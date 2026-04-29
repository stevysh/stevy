package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
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

func (s *SessionManager) Set(w http.ResponseWriter, userID int64) {
	idStr := strconv.FormatInt(userID, 10)
	value := idStr + "." + s.sign(idStr)

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

// UserID returns the authenticated user ID from the request cookie, or 0.
func (s *SessionManager) UserID(r *http.Request) int64 {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return 0
	}
	idStr, sig, ok := strings.Cut(c.Value, ".")
	if !ok {
		return 0
	}
	if !hmac.Equal([]byte(sig), []byte(s.sign(idStr))) {
		return 0
	}
	id, _ := strconv.ParseInt(idStr, 10, 64)
	return id
}

func (s *SessionManager) sign(data string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

// Context helpers for API-key middleware → RPC handlers.

func ContextWithUserID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, userIDKey, id)
}

func UserIDFromContext(ctx context.Context) int64 {
	if v, ok := ctx.Value(userIDKey).(int64); ok {
		return v
	}
	return 0
}

func ContextWithWorkerID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, workerIDKey, id)
}

func WorkerIDFromContext(ctx context.Context) int64 {
	if v, ok := ctx.Value(workerIDKey).(int64); ok {
		return v
	}
	return 0
}
