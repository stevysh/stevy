package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/stevysh/stevy/internal/db"
)

type OAuthHandler struct {
	cfg            *oauth2.Config
	sessions       *SessionManager
	db             *db.DB
	allowedDomains map[string]bool
}

type OAuthConfig struct {
	ClientID       string
	ClientSecret   string
	RedirectURL    string
	AllowedDomains []string
}

func NewOAuthHandler(cfg OAuthConfig, sessions *SessionManager, database *db.DB) *OAuthHandler {
	domains := map[string]bool{}
	for _, d := range cfg.AllowedDomains {
		domains[d] = true
	}
	return &OAuthHandler{
		cfg: &oauth2.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			RedirectURL:  cfg.RedirectURL,
			Scopes:       []string{"openid", "email", "profile"},
			Endpoint:     google.Endpoint,
		},
		sessions:       sessions,
		db:             database,
		allowedDomains: domains,
	}
}

func (h *OAuthHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/auth/google/login", h.Login)
	mux.HandleFunc("/auth/google/callback", h.Callback)
	mux.HandleFunc("/auth/logout", h.Logout)
}

func (h *OAuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	state, err := randomState()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "oauth_state",
		Value:    state,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(10 * time.Minute / time.Second),
	})
	url := h.cfg.AuthCodeURL(state, oauth2.AccessTypeOnline)
	http.Redirect(w, r, url, http.StatusFound)
}

func (h *OAuthHandler) Callback(w http.ResponseWriter, r *http.Request) {
	wantState, err := r.Cookie("oauth_state")
	if err != nil || wantState.Value == "" {
		http.Error(w, "missing state cookie", http.StatusBadRequest)
		return
	}
	if r.URL.Query().Get("state") != wantState.Value {
		http.Error(w, "state mismatch", http.StatusBadRequest)
		return
	}

	tok, err := h.cfg.Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		http.Error(w, "exchange failed: "+err.Error(), http.StatusUnauthorized)
		return
	}

	info, err := h.fetchUserInfo(r.Context(), tok)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !h.emailAllowed(info.Email) {
		http.Error(w, "email not allowed", http.StatusForbidden)
		return
	}

	user, err := h.db.UpsertUser(r.Context(), info.ID, info.Email, info.Name)
	if err != nil {
		http.Error(w, "user upsert failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	h.sessions.Set(w, user.ID)
	http.Redirect(w, r, "/", http.StatusFound)
}

func (h *OAuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	h.sessions.Clear(w)
	http.Redirect(w, r, "/", http.StatusFound)
}

func (h *OAuthHandler) emailAllowed(email string) bool {
	if len(h.allowedDomains) == 0 {
		return true // no restriction
	}
	for domain := range h.allowedDomains {
		if len(email) > len(domain)+1 && email[len(email)-len(domain)-1:] == "@"+domain {
			return true
		}
	}
	return false
}

type googleUserInfo struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

func (h *OAuthHandler) fetchUserInfo(ctx context.Context, tok *oauth2.Token) (*googleUserInfo, error) {
	client := h.cfg.Client(ctx, tok)
	resp, err := client.Get("https://www.googleapis.com/oauth2/v2/userinfo")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var info googleUserInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

func randomState() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
