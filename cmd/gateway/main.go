package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cozy-critter-puzzle-parlor/internal/gateway"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	addr := ":" + getEnv("PORT", "8080")
	allowedOrigins := strings.Split(getEnv("ALLOWED_ORIGINS", "localhost:8081"), ",")

	gw := gateway.New(brokers, "echo-test", allowedOrigins)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := gw.EnsureTopic(ctx); err != nil {
		log.Fatalf("gateway: ensure topic: %v", err)
	}
	cancel()

	log.Printf("gateway: listening on %s (kafka brokers: %v)", addr, brokers)
	if err := http.ListenAndServe(addr, gw.Handler()); err != nil {
		log.Fatal(err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
