// Package gateway is the WebSocket entrypoint for the game. Milestone 1
// proved the WebSocket<->Kafka wiring with a throwaway echo topic; this
// package now implements the real message protocol: room lifecycle and
// movement (Milestone 2). See IMPLEMENTATION_PLAN.md.
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/segmentio/kafka-go"

	"cozy-critter-puzzle-parlor/internal/room"
	"cozy-critter-puzzle-parlor/internal/schema"
)

const topicPlayerPositions = "player-positions"

const PlayerPositionsPartitions = 6

type Gateway struct {
	brokers        []string
	allowedOrigins []string
	rooms          *room.Registry
	hub            *hub
	moveWriter     *kafka.Writer
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
		hub:            newHub(),
		moveWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topicPlayerPositions,
			// Keying by room code keeps a room's events on a single
			// partition (ordering per room), and is the natural unit a
			// future multi-instance broadcaster would parallelize over.
			Balancer: &kafka.Hash{},
		},
	}
}

// EnsureTopic creates a topic (no replication) if it doesn't already
// exist. Call once at startup for each topic the gateway produces/consumes.
func (g *Gateway) EnsureTopic(ctx context.Context, topic string, numPartitions int) error {
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
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	})
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return err
	}
	return nil
}

// StartMovementBroadcast starts one background consumer per partition of
// player-positions, each fanning events out to every connection in that
// event's room. Call once at startup, after
// EnsureTopic(ctx, "player-positions", ...) and before accepting any
// connections: it synchronously positions every partition reader at the
// topic's current end before returning, so a message produced right after
// this call can't be missed.
//
// A consumer-group reader would auto-balance partitions across future
// gateway instances, but its initial join/rebalance handshake takes long
// enough that a message produced immediately after start-up can land
// before the group finishes joining — and since a partition assignment,
// once positioned, only sees new messages, that first message is silently
// dropped (the same class of race Milestone 1 hit and fixed for the
// echo-test reader, just with a much bigger window). Direct partition
// readers avoid that handshake entirely. The tradeoff: splitting rooms
// across multiple gateway instances later needs a different mechanism
// than "join the same group" — see "Open / deferred" in
// IMPLEMENTATION_PLAN.md.
func (g *Gateway) StartMovementBroadcast(ctx context.Context) error {
	for partition := 0; partition < PlayerPositionsPartitions; partition++ {
		if err := g.startPartitionBroadcaster(ctx, partition); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gateway) startPartitionBroadcaster(ctx context.Context, partition int) error {
	leader, err := kafka.DialLeader(ctx, "tcp", g.brokers[0], topicPlayerPositions, partition)
	if err != nil {
		return err
	}
	startOffset, err := leader.ReadLastOffset()
	leader.Close()
	if err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   g.brokers,
		Topic:     topicPlayerPositions,
		Partition: partition,
	})
	if err := reader.SetOffset(startOffset); err != nil {
		reader.Close()
		return err
	}

	go func() {
		defer reader.Close()
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				return
			}
			var evt schema.PlayerPositionEvent
			if err := json.Unmarshal(msg.Value, &evt); err != nil {
				log.Printf("gateway: bad player-position event: %v", err)
				continue
			}

			g.hub.setPosition(evt.RoomID, evt.PlayerID, position{X: evt.TargetX, Y: evt.TargetY})

			data, err := marshalEnvelope(schema.TypePlayerMoved, evt)
			if err != nil {
				log.Printf("gateway: marshal player-moved broadcast: %v", err)
				continue
			}
			g.hub.broadcast(ctx, evt.RoomID, data)
		}
	}()
	return nil
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.handleWS)
	return mux
}

func (g *Gateway) handleWS(w http.ResponseWriter, r *http.Request) {
	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: g.allowedOrigins,
	})
	if err != nil {
		log.Printf("gateway: accept: %v", err)
		return
	}
	defer wsConn.CloseNow()

	conn := &safeConn{conn: wsConn}
	ctx := r.Context()

	// Which room/player this connection has joined, if any.
	var joinedRoomCode, playerID string
	defer func() {
		if joinedRoomCode != "" && playerID != "" {
			g.hub.leave(joinedRoomCode, playerID)
		}
	}()

	for {
		_, data, err := wsConn.Read(ctx)
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
			code, id, err := g.handleJoinRoom(ctx, conn, env.Payload)
			if err == nil {
				joinedRoomCode, playerID = code, id
			}

		case schema.TypeMove:
			g.handleMove(ctx, conn, joinedRoomCode, playerID, env.Payload)

		default:
			g.sendError(ctx, conn, "unknown message type: "+env.Type)
		}
	}
}

func (g *Gateway) handleCreateRoom(ctx context.Context, conn *safeConn) {
	code, err := g.rooms.Create()
	if err != nil {
		log.Printf("gateway: create room: %v", err)
		g.sendError(ctx, conn, "could not create room")
		return
	}
	g.send(ctx, conn, schema.TypeRoomCreated, schema.RoomCreated{RoomCode: code})
}

// handleJoinRoom returns the joined room code and player id on success.
func (g *Gateway) handleJoinRoom(ctx context.Context, conn *safeConn, payload json.RawMessage) (string, string, error) {
	var req schema.JoinRoomRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		g.sendError(ctx, conn, "invalid join_room payload")
		return "", "", err
	}
	if !g.rooms.Exists(req.RoomCode) {
		g.sendError(ctx, conn, "unknown room code")
		return "", "", errors.New("unknown room code")
	}
	g.hub.join(req.RoomCode, req.PlayerID, conn)
	g.send(ctx, conn, schema.TypeJoined, schema.Joined{RoomCode: req.RoomCode, PlayerID: req.PlayerID})
	return req.RoomCode, req.PlayerID, nil
}

func (g *Gateway) handleMove(ctx context.Context, conn *safeConn, roomCode, playerID string, payload json.RawMessage) {
	if roomCode == "" || playerID == "" {
		g.sendError(ctx, conn, "must join a room before moving")
		return
	}
	var req schema.MoveRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		g.sendError(ctx, conn, "invalid move payload")
		return
	}

	current := g.hub.currentPosition(roomCode, playerID)
	evt := schema.PlayerPositionEvent{
		EventID:         newEventID(),
		Timestamp:       time.Now().UnixMilli(),
		PlayerID:        playerID,
		RoomID:          roomCode,
		Action:          "MOVE",
		CurrentX:        current.X,
		CurrentY:        current.Y,
		TargetX:         req.TargetX,
		TargetY:         req.TargetY,
		FacingDirection: req.FacingDirection,
	}

	raw, err := json.Marshal(evt)
	if err != nil {
		log.Printf("gateway: marshal move event: %v", err)
		g.sendError(ctx, conn, "internal error")
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := g.moveWriter.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(roomCode),
		Value: raw,
	}); err != nil {
		log.Printf("gateway: produce move event: %v", err)
		g.sendError(ctx, conn, "could not process move")
	}
}

func (g *Gateway) sendError(ctx context.Context, conn *safeConn, message string) {
	g.send(ctx, conn, schema.TypeError, schema.ErrorPayload{Message: message})
}

func (g *Gateway) send(ctx context.Context, conn *safeConn, msgType string, payload any) {
	data, err := marshalEnvelope(msgType, payload)
	if err != nil {
		log.Printf("gateway: marshal %s: %v", msgType, err)
		return
	}
	if err := conn.Write(ctx, data); err != nil {
		log.Printf("gateway: write %s: %v", msgType, err)
	}
}

func marshalEnvelope(msgType string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(schema.Envelope{Type: msgType, Payload: raw})
}

func newEventID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
