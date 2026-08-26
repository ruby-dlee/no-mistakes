package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/kunchenguid/no-mistakes/internal/config"
	"github.com/kunchenguid/no-mistakes/internal/coordinator"
	"github.com/kunchenguid/no-mistakes/internal/db"
	"github.com/kunchenguid/no-mistakes/internal/paths"
)

const coordinatorShutdownTimeout = 2 * time.Second

type coordinatorRuntimeOptions struct {
	Config       config.Coordinator
	DB           *db.DB
	Paths        *paths.Paths
	Getenv       func(string) (string, bool)
	Listen       func(string, string) (net.Listener, error)
	Repositories coordinator.RepositoryMapper
	GitHub       coordinator.GitHubStateClient
	Reducer      coordinator.CIStateReducer
}

type coordinatorRuntime struct {
	address  string
	cancel   context.CancelFunc
	server   *http.Server
	listener net.Listener
	errors   chan error
	wait     sync.WaitGroup
	close    sync.Once
	closeErr error
}

func startCoordinatorRuntime(parent context.Context, options coordinatorRuntimeOptions) (*coordinatorRuntime, error) {
	if !options.Config.Enabled {
		return nil, nil
	}
	if options.DB == nil || options.Paths == nil {
		return nil, errors.New("coordinator requires daemon database and paths")
	}
	if options.Getenv == nil {
		options.Getenv = os.LookupEnv
	}
	secret, ok := options.Getenv(options.Config.GitHubWebhookSecretEnv)
	if !ok || len(secret) < 16 || len(secret) > 4096 {
		return nil, errors.New("coordinator webhook secret is missing or outside the bounded length")
	}
	if options.Listen == nil {
		options.Listen = net.Listen
	}
	if options.Repositories == nil {
		options.Repositories = coordinator.RegisteredGitHubRepositories{Store: options.DB}
	}
	if options.GitHub == nil {
		authority, err := coordinator.NewGitHubAuthority(coordinator.GitHubAuthorityOptions{
			Store: options.DB, WorkDir: options.Paths.Root(),
		})
		if err != nil {
			return nil, err
		}
		options.GitHub = authority
	}
	if options.Reducer == nil {
		options.Reducer = coordinator.ExactCIStateReducer{}
	}
	service, err := coordinator.NewService(coordinator.ServiceOptions{
		Store: options.DB, GitHub: options.GitHub, Reducer: options.Reducer,
		BatchSize: options.Config.BatchSize, MaxConcurrency: options.Config.MaxConcurrency,
		Interval: options.Config.ReconcileInterval,
		OnError: func(err error) {
			slog.Warn("coordinator reconciliation deferred", "error", err)
		},
	})
	if err != nil {
		return nil, err
	}
	handler, err := coordinator.NewWebhookHandler(coordinator.WebhookOptions{
		Secret: []byte(secret), Store: options.DB, Repositories: options.Repositories,
		GitHub: options.GitHub,
	})
	if err != nil {
		return nil, err
	}
	listener, err := options.Listen("tcp", options.Config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("bind coordinator webhook listener: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	mux := http.NewServeMux()
	mux.Handle("/github", handler)
	runtime := &coordinatorRuntime{
		address: listener.Addr().String(), cancel: cancel, listener: listener,
		errors: make(chan error, 2),
	}
	runtime.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	runtime.wait.Add(2)
	go func() {
		defer runtime.wait.Done()
		if err := runtime.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			runtime.report(fmt.Errorf("coordinator webhook server: %w", err))
		}
	}()
	go func() {
		defer runtime.wait.Done()
		if err := service.Run(ctx); err != nil && ctx.Err() == nil {
			runtime.report(fmt.Errorf("coordinator service: %w", err))
		}
	}()
	slog.Info("coordinator ready",
		"listen", runtime.address,
		"batch_size", options.Config.BatchSize,
		"max_concurrency", options.Config.MaxConcurrency,
		"reconcile_interval", options.Config.ReconcileInterval,
	)
	return runtime, nil
}

func (r *coordinatorRuntime) report(err error) {
	select {
	case r.errors <- err:
	default:
	}
}

func (r *coordinatorRuntime) Address() string {
	if r == nil {
		return ""
	}
	return r.address
}

func (r *coordinatorRuntime) Errors() <-chan error {
	if r == nil {
		return nil
	}
	return r.errors
}

func (r *coordinatorRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.close.Do(func() {
		r.cancel()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), coordinatorShutdownTimeout)
		defer cancel()
		if err := r.server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.closeErr = fmt.Errorf("shut down coordinator webhook server: %w", err)
			_ = r.listener.Close()
		}
		done := make(chan struct{})
		go func() {
			r.wait.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			r.closeErr = errors.Join(r.closeErr, errors.New("coordinator shutdown timed out"))
		}
	})
	return r.closeErr
}
