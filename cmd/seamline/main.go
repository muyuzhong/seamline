package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/muyuzhong/seamline/internal/broker"
	"github.com/muyuzhong/seamline/internal/hotwindow"
	"github.com/muyuzhong/seamline/internal/server"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "HTTP listen address")
		demo      = flag.Bool("demo", true, "enable embedded F5 live playground at / and /v1/demo/simulate")
		subBuffer = flag.Int("sub-buffer", 128, "per-subscriber live event channel buffer size")
		idleTTL   = flag.Duration("idle-ttl", 30*time.Minute, "idle session actor eviction TTL")
		keepalive = flag.Duration("keepalive", 15*time.Second, "SSE keepalive comment interval")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	store := hotwindow.NewMemoryStorage()
	brk := broker.New(store, broker.Config{
		SubscriberBufferSize: *subBuffer,
		IdleSessionTTL:       *idleTTL,
	})

	httpHandler := server.NewHandler(brk, server.Config{
		KeepaliveInterval: *keepalive,
		EnableDemo:        *demo,
	})

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           httpHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("seamline server listening",
			"addr", *addr,
			"instance_id", brk.InstanceID(),
			"demo", *demo,
			"sub_buffer", *subBuffer,
			"idle_ttl", idleTTL.String(),
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			logger.Error("http server failed", "error", err)
			_ = brk.Close()
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutting down seamline server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Close broker first so active SSE handlers unblock and exit promptly during httpServer.Shutdown.
		if err := brk.Close(); err != nil {
			logger.Error("broker close error", "error", err)
		}
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful http shutdown error", "error", err)
		}
		logger.Info("seamline server stopped cleanly")
	}
}
