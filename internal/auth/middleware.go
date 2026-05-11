package auth

import (
	"context"
	"net/http"
	"strings"

	"connectrpc.com/connect"

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
func APIKeyInterceptor(database *db.DB) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			authHdr := req.Header().Get("Authorization")
			if !strings.HasPrefix(authHdr, "Bearer ") {
				return nil, connect.NewError(connect.CodeUnauthenticated, nil)
			}
			token := strings.TrimPrefix(authHdr, "Bearer ")
			procedure := req.Spec().Procedure

			switch {
			case strings.HasPrefix(token, "stw_"):
				if !workerMethods[procedure] {
					return nil, connect.NewError(connect.CodePermissionDenied, nil)
				}
				workerID, err := database.LookupWorkerKey(ctx, token)
				if err != nil {
					return nil, connect.NewError(connect.CodeUnauthenticated, err)
				}
				ctx = ContextWithWorkerID(ctx, workerID)

			case strings.HasPrefix(token, "stv_"):
				lookup, err := database.LookupAPIKey(ctx, token)
				if err != nil {
					return nil, connect.NewError(connect.CodeUnauthenticated, err)
				}
				ctx = ContextWithUserID(ctx, lookup.UserID)

			default:
				return nil, connect.NewError(connect.CodeUnauthenticated, nil)
			}

			return next(ctx, req)
		}
	})
}
