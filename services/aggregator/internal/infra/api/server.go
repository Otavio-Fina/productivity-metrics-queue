package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const shutdownTimeout = 10 * time.Second

type Server struct {
	srv *http.Server
}

func NewServer(addr string, handler http.Handler) *Server {
	return &Server{
		srv: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		slog.InfoContext(ctx, "http server: listening", "addr", s.srv.Addr)
		err := s.srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			errCh <- nil
			return
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("http listen: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		slog.Info("http server: stopped")
		return nil
	}
}
