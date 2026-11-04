package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/Martin-Aziz/reliable-workflows-lab/internal/httpapi"
	"github.com/Martin-Aziz/reliable-workflows-lab/internal/jobs"
	"github.com/Martin-Aziz/reliable-workflows-lab/internal/postgres"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var store jobs.Store
	mode := "in-memory (not durable)"
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			logger.Error("database initialization failed", "error", err)
			os.Exit(1)
		}
		defer db.Close()
		db.SetMaxOpenConns(16)
		db.SetMaxIdleConns(8)
		db.SetConnMaxLifetime(30 * time.Minute)
		dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		pg := postgres.New(db)
		err = pg.Ready(dbCtx)
		if err == nil {
			err = pg.Migrate(dbCtx)
		}
		cancel()
		if err != nil {
			logger.Error("database unavailable or schema initialization failed", "error", err)
			os.Exit(1)
		}
		store = pg
		mode = "postgresql"
	} else {
		logger.Warn("DATABASE_URL is unset; queued jobs and idempotency keys will disappear on restart")
		store = jobs.MemoryStore{Queue: jobs.NewQueue()}
	}

	workers := 2
	if raw := os.Getenv("WORKERS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > 32 {
			logger.Error("WORKERS must be between 0 and 32")
			os.Exit(1)
		}
		workers = n
	}
	processor := &jobs.Processor{Store: store, Handlers: jobs.DefaultHandlers(), Logger: logger, TaskTimeout: 5 * time.Second}
	for i := 0; i < workers; i++ {
		workerID := jobs.ProcessWorkerID(i + 1)
		go processor.Run(ctx, workerID, 100*time.Millisecond)
	}
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	server := &http.Server{
		Addr: addr, Handler: (&httpapi.API{Store: store}).Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", "error", err)
		}
	}()
	logger.Info("server started", "address", addr, "workers", workers, "persistence", mode)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server failed", "error", err)
		os.Exit(1)
	}
}
