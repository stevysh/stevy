package auth

import (
	"context"
	"net/http"
	"strings"

	"connectrpc.com/connect/v2"

	"github.com/stevysh/stevy/internal/db"
)

// RequireSession wraps an HTTP handler, returning 302 → / if no session.
func RequireSession(sessions *SessionManager, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sessions.UserID(r) != "" {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/_/api/") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, "/", http.StatusFound)
	})
}

// workerMethods are callable only with the Worker auth scheme.
var workerMethods = map[string]bool{
	"/stevy.v1.JobService/ClaimJob":     true,
	"/stevy.v1.JobService/CompleteJob":  true,
	"/stevy.v1.JobService/FailJob":      true,
	"/stevy.v1.JobService/HeartbeatJob": true,
}

// APIKeyInterceptor enforces API-key auth on Connect RPC calls.
// stv_ prefix → client key → api_keys table → client methods.
// stw_ prefix → worker key → workers table → worker methods.
func APIKeyInterceptor(database *db.DB) connect.ServerInterceptor {
	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			info, _ := connect.CallInfoForServerContext(ctx)
			authHdr := info.RequestHeader().Get("Authorization")
			if !strings.HasPrefix(authHdr, "Bearer ") {
				return connect.NewError(connect.CodeUnauthenticated, "")
			}
			token := strings.TrimPrefix(authHdr, "Bearer ")
			procedure := spec.Procedure

			switch {
			case strings.HasPrefix(token, "stw_"):
				if !workerMethods[procedure] {
					return connect.NewError(connect.CodePermissionDenied, "")
				}
				workerID, err := database.LookupWorkerKey(ctx, token)
				if err != nil {
					return connect.NewError(connect.CodeUnauthenticated, err.Error()).WithCause(err)
				}
				ctx = ContextWithWorkerID(ctx, workerID)

			case strings.HasPrefix(token, "stv_"):
				lookup, err := database.LookupAPIKey(ctx, token)
				if err != nil {
					return connect.NewError(connect.CodeUnauthenticated, err.Error()).WithCause(err)
				}
				ctx = ContextWithUserID(ctx, lookup.UserID)

			default:
				return connect.NewError(connect.CodeUnauthenticated, "")
			}

			return next(ctx, spec, stream)
		}
	}
}
