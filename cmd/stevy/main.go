package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/vanguard"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	_ "modernc.org/sqlite"

	"github.com/stevysh/stevy/gen/stevy/v1/stevyv1connect"

	"github.com/stevysh/stevy/internal/auth"
	"github.com/stevysh/stevy/internal/db"
	"github.com/stevysh/stevy/internal/dialect"
	"github.com/stevysh/stevy/internal/middleware"
	"github.com/stevysh/stevy/internal/service"
	"github.com/stevysh/stevy/internal/web"
)

var Version = "dev"

func main() {
	// Load .env if present; existing env vars take precedence.
	_ = godotenv.Load()

	switch firstArg() {
	case "migrate":
		exit(cmdMigrate())
	case "scheduler":
		exit(cmdScheduler())
	case "", "serve":
		exit(cmdServe())
	case "version":
		fmt.Println("stevy", Version)
	case "help", "-h", "--help":
		fmt.Println("usage: stevy [serve|scheduler|migrate|version]")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", firstArg())
		os.Exit(2)
	}
}

// backend bundles everything that varies by dialect.
type backend struct {
	dialect dialect.Dialect
	sqlDB   *sql.DB
	pgPool  *pgxpool.Pool // only set when dialect == Postgres
	driver  service.Driver
	closeFn func()
}

func jobLockDuration() time.Duration {
	if v := os.Getenv("JOB_LOCK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 30 * time.Second
}

func openBackend(ctx context.Context, dsn string) (*backend, error) {
	d, err := dialect.FromDSN(dsn)
	if err != nil {
		return nil, err
	}

	lockDur := jobLockDuration()

	switch d {
	case dialect.Postgres:
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, err
		}
		sqlDB := stdlib.OpenDBFromPool(pool)
		return &backend{
			dialect: d,
			sqlDB:   sqlDB,
			pgPool:  pool,
			driver:  service.NewPGDriver(pool, lockDur),
			closeFn: func() { sqlDB.Close(); pool.Close() },
		}, nil

	case dialect.SQLite:
		path := dialect.StripSQLitePrefix(dsn)
		sqlDB, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)")
		if err != nil {
			return nil, err
		}
		sqlDB.SetMaxOpenConns(1)
		return &backend{
			dialect: d,
			sqlDB:   sqlDB,
			driver:  service.NewSQLiteDriver(sqlDB, lockDur),
			closeFn: func() { sqlDB.Close() },
		}, nil
	}
	return nil, fmt.Errorf("unsupported dialect: %s", d)
}

func cmdMigrate() error {
	ctx := context.Background()
	b, err := openBackend(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer b.closeFn()
	return runMigrations(b)
}

func runMigrations(b *backend) error {
	if err := db.Migrate(b.sqlDB, b.dialect); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	fmt.Println("migrated")
	return nil
}

// cmdScheduler runs a long-lived loop that promotes scheduled/retryable jobs
// whose scheduled_at has passed to 'available'. Deploy as a separate process
// (Cloud Run with min-instances=1, GCE, etc) so it stays warm.
func cmdScheduler() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	b, err := openBackend(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer b.closeFn()

	tick := time.Second
	if v := os.Getenv("SCHEDULER_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			tick = d
		}
	}
	logger.Info("scheduler starting", "interval", tick, "dialect", b.dialect)

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("scheduler stopping")
			return nil
		case <-ticker.C:
			n, err := b.driver.PromoteScheduledJobs(ctx, 1000)
			if err != nil {
				logger.Error("promote", "err", err)
			} else if n > 0 {
				logger.Info("promoted", "count", n)
			}

			expired, err := b.driver.FailExpiredJobs(ctx, 1000)
			if err != nil {
				logger.Error("fail_expired", "err", err)
			} else if expired > 0 {
				logger.Info("failed_expired", "count", expired)
			}
		}
	}
}

func cmdServe() error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	migrate := fs.Bool("migrate", false, "run migrations on startup")
	_ = fs.Parse(serveArgs())

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	b, err := openBackend(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer b.closeFn()

	if *migrate || os.Getenv("MIGRATE") == "true" {
		if err := runMigrations(b); err != nil {
			return err
		}
	}

	database := db.New(b.sqlDB, b.dialect)

	sessions := auth.NewSessionManager([]byte(mustEnv("SESSION_SECRET")))
	oauth := auth.NewOAuthHandler(auth.OAuthConfig{
		ClientID:       mustEnv("GOOGLE_CLIENT_ID"),
		ClientSecret:   mustEnv("GOOGLE_CLIENT_SECRET"),
		RedirectURL:    getenv("HOSTNAME", "http://localhost:8080") + "/auth/google/callback",
		AllowedDomains: splitCSV(os.Getenv("ALLOWED_DOMAINS")),
	}, sessions, database)
	apiKeys := auth.NewAPIKeyHandler(database, sessions)
	apiAuth := auth.APIKeyInterceptor(database)
	validate, err := middleware.Validate()
	if err != nil {
		return err
	}

	jobSvc := service.NewJob(b.driver)
	queueSvc := service.NewQueue(b.driver)
	webHandler := web.NewHandler(database, sessions, b.driver)

	mux := http.NewServeMux()

	interceptors := connect.WithInterceptors(apiAuth, validate)
	jobPath, jobHandler := stevyv1connect.NewJobServiceHandler(jobSvc, interceptors)
	queuePath, queueHandler := stevyv1connect.NewQueueServiceHandler(queueSvc, interceptors)

	transcoder, err := vanguard.NewTranscoder(
		[]*vanguard.Service{
			vanguard.NewService(jobPath, jobHandler),
			vanguard.NewService(queuePath, queueHandler),
		},
		vanguard.WithCodec(func(res vanguard.TypeResolver) vanguard.Codec {
			codec := vanguard.NewJSONCodec(res)
			codec.MarshalOptions.UseProtoNames = true
			codec.MarshalOptions.EmitUnpopulated = true
			codec.UnmarshalOptions.DiscardUnknown = true
			return codec
		}),
	)
	if err != nil {
		return fmt.Errorf("vanguard: %w", err)
	}
	mux.Handle(jobPath, transcoder)
	mux.Handle(queuePath, transcoder)

	oauth.RegisterRoutes(mux)
	apiKeys.RegisterRoutes(mux)
	mux.HandleFunc("POST /scheduler/run", webHandler.RunScheduler)
	mux.HandleFunc("POST /queues/{name}/pause", webHandler.PauseQueue)
	mux.HandleFunc("POST /queues/{name}/resume", webHandler.ResumeQueue)
	mux.HandleFunc("GET /queues", webHandler.QueuesPage)
	mux.HandleFunc("GET /workers", webHandler.WorkersPage)
	mux.HandleFunc("GET /workers/list", webHandler.WorkersJSON)
	mux.HandleFunc("POST /workers", webHandler.CreateWorker)
	mux.HandleFunc("DELETE /workers/{id}", webHandler.DeleteWorker)
	mux.HandleFunc("GET /keys", webHandler.KeysPage)
	mux.HandleFunc("GET /{id}", webHandler.JobPage)
	mux.HandleFunc("/", webHandler.Index)

	hostname := getenv("HOSTNAME", "http://localhost:8080")
	publicFS := http.FileServer(http.Dir("public"))
	if spec, err := os.ReadFile("public/openapi.yaml"); err == nil {
		spec = bytes.ReplaceAll(spec, []byte("https://stevy.example.com"), []byte(hostname))
		mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
			w.Write(spec)
		})
	}
	if entries, err := os.ReadDir("public"); err == nil {
		for _, e := range entries {
			if !e.IsDir() && !strings.HasPrefix(e.Name(), ".") && e.Name() != "openapi.yaml" {
				mux.Handle("GET /"+e.Name(), publicFS)
			}
		}
	}

	addr := ":" + getenv("PORT", "8080")
	srv := &http.Server{
		Addr:    addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		_ = srv.Shutdown(context.Background())
	}()

	logger.Info("listening", "addr", addr, "dialect", b.dialect)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ─────────────────────────── Helpers ───────────────────────────

func firstArg() string {
	if len(os.Args) < 2 {
		return ""
	}
	return os.Args[1]
}

// serveArgs returns flags after `serve` (or after the program name if no
// subcommand was given), so `stevy --migrate` and `stevy serve --migrate` both work.
func serveArgs() []string {
	if len(os.Args) >= 2 && os.Args[1] == "serve" {
		return os.Args[2:]
	}
	return os.Args[1:]
}

func exit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic("missing env: " + k)
	}
	return v
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
