// Package gateway is the WebSocket entrypoint for the game. Milestone 1
// proved the WebSocket<->Kafka wiring with a throwaway echo topic; this
// package now implements the real message protocol, starting with room
// lifecycle (Milestone 2). See IMPLEMENTATION_PLAN.md.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/coder/websocket"
	"github.com/segmentio/kafka-go"

	"cozy-critter-puzzle-parlor/internal/room"
	"cozy-critter-puzzle-parlor/internal/schema"
)

type Gateway struct {
	brokers        []string
	allowedOrigins []string
	rooms          *room.Registry
}

// New creates a Gateway. allowedOrigins lists origin patterns (per
// coder/websocket's AcceptOptions.OriginPatterns) permitted to open a
// WebSocket connection from a browser; a nil/empty slice accepts same-origin
// requests only.
func New(brokers []string, allowedOrigins []string) *Gateway {
	return &Gateway{
		brokers:        brokers,
		allowedOrigins: allowedOrigins,
		rooms:          room.NewRegistry(),
	}
}

// EnsureTopic creates a topic (single partition, no replication) if it
// doesn't already exist. Call once at startup for each topic the gateway
// produces/consumes.
func (g *Gateway) EnsureTopic(ctx context.Context, topic string) error {
	conn, err := kafka.DialContext(ctx, "tcp", g.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return err
	}
	return nil
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.handleWS)
	return mux
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: g.allowedOrigins,
	})
	if err != nil {
		log.Printf("gateway: accept: %v", err)
		return
	}
	defer conn.CloseNow()

	ctx := r.Context()

	// Which room (if any) this connection has joined. Only referenced
	// within this handler for now; becomes relevant to broadcast once
	// movement/chat land later in Milestone 2.
	var joinedRoomCode string

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}

		var env schema.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			g.sendError(ctx, conn, "invalid message envelope")
			continue
		}

		switch env.Type {
		case schema.TypeCreateRoom:
			g.handleCreateRoom(ctx, conn)

		case schema.TypeJoinRoom:
			code, err := g.handleJoinRoom(ctx, conn, env.Payload)
			if err == nil {
				joinedRoomCode = code
			}

		default:
			g.sendError(ctx, conn, "unknown message type: "+env.Type)
		}
	}

	_ = joinedRoomCode
}

func (g *Gateway) handleCreateRoom(ctx context.Context, conn *websocket.Conn) {
	code, err := g.rooms.Create()
	if err != nil {
		log.Printf("gateway: create room: %v", err)
		g.sendError(ctx, conn, "could not create room")
		return
	}
	g.send(ctx, conn, schema.TypeRoomCreated, schema.RoomCreated{RoomCode: code})
}

// handleJoinRoom returns the joined room code on success.
func (g *Gateway) handleJoinRoom(ctx context.Context, conn *websocket.Conn, payload json.RawMessage) (string, error) {
	var req schema.JoinRoomRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		g.sendError(ctx, conn, "invalid join_room payload")
		return "", err
	}
	if !g.rooms.Exists(req.RoomCode) {
		g.sendError(ctx, conn, "unknown room code")
		return "", errors.New("unknown room code")
	}
	g.send(ctx, conn, schema.TypeJoined, schema.Joined{RoomCode: req.RoomCode, PlayerID: req.PlayerID})
	return req.RoomCode, nil
}

func (g *Gateway) sendError(ctx context.Context, conn *websocket.Conn, message string) {
	g.send(ctx, conn, schema.TypeError, schema.ErrorPayload{Message: message})
}

func (g *Gateway) send(ctx context.Context, conn *websocket.Conn, msgType string, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Printf("gateway: marshal %s payload: %v", msgType, err)
		return
	}
	data, err := json.Marshal(schema.Envelope{Type: msgType, Payload: raw})
	if err != nil {
		log.Printf("gateway: marshal %s envelope: %v", msgType, err)
		return
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		log.Printf("gateway: write %s: %v", msgType, err)
	}
}
