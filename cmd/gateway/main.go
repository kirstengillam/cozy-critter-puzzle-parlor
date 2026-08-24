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

	gw := gateway.New(brokers, allowedOrigins)

	setupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := gw.EnsureTopic(setupCtx, "player-positions", gateway.PlayerPositionsPartitions); err != nil {
		log.Fatalf("gateway: ensure player-positions topic: %v", err)
	}
	if err := gw.EnsureTopic(setupCtx, "chat-messages", gateway.ChatMessagesPartitions); err != nil {
		log.Fatalf("gateway: ensure chat-messages topic: %v", err)
	}
	if err := gw.EnsureTopic(setupCtx, "game-sessions", gateway.GameSessionsPartitions); err != nil {
		log.Fatalf("gateway: ensure game-sessions topic: %v", err)
	}
	cancel()

	if err := gw.StartMovementBroadcast(context.Background()); err != nil {
		log.Fatalf("gateway: start movement broadcast: %v", err)
	}
	if err := gw.StartChatFilter(context.Background()); err != nil {
		log.Fatalf("gateway: start chat filter: %v", err)
	}

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
