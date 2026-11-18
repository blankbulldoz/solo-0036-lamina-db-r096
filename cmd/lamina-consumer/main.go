package main

import (
	"context"
	"flag"
	"fmt"
	"lamina/internal/consumer"
	"lamina/internal/downstream"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("lamina consumer stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("data", "var/downstream", "consumer state directory exclusively owned by this process")
	source := flag.String("source", "http://127.0.0.1:7386/v1/lamina", "source database /v1/lamina base URL")
	addr := flag.String("listen", "127.0.0.1:7396", "HTTP bind address for observation and sync control")
	name := flag.String("name", "default", "consumer name recorded by the source acknowledgement registry")
	flag.Parse()
	if *source == "" {
		return fmt.Errorf("source URL is required")
	}
	sink, err := consumer.Open(*dir, trimSlash(*source), *name)
	if err != nil {
		return err
	}
	defer sink.Close()
	server := &http.Server{Addr: *addr, Handler: downstream.New(sink), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 45 * time.Second, MaxHeaderBytes: 32 << 10}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stopped)
	finished := make(chan error, 1)
	go func() {
		slog.Info("lamina consumer ready", "address", *addr, "source", *source)
		finished <- server.ListenAndServe()
	}()
	select {
	case err = <-finished:
		if err != http.ErrServerClosed {
			return err
		}
	case <-stopped:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Shutdown(ctx); err != nil {
			server.Close()
			return err
		}
	}
	return nil
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
