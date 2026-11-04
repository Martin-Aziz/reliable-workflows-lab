package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/httpapi"
	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	queue := jobs.NewQueue()
	processor := &jobs.Processor{Queue: queue, Handlers: jobs.DefaultHandlers(), Logger: logger, TaskTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workers := 2
	if raw := os.Getenv("WORKERS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 1 && n <= 32 {
			workers = n
		} else {
			logger.Error("WORKERS must be between 1 and 32")
			os.Exit(1)
		}
	}
	for i := 0; i < workers; i++ {
		id := "worker-" + strconv.Itoa(i+1)
		go processor.Run(ctx, id, 100*time.Millisecond)
	}
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	server := &http.Server{Addr: addr, Handler: (&httpapi.API{Queue: queue}).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", "error", err)
		}
	}()
	logger.Info("phase-1 server started", "address", addr, "workers", workers, "persistence", "in-memory")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server failed", "error", err)
		os.Exit(1)
	}
}
