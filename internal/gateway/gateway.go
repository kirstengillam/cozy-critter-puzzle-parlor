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
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/segmentio/kafka-go"

	"cozy-critter-puzzle-parlor/internal/room"
	"cozy-critter-puzzle-parlor/internal/schema"
	"cozy-critter-puzzle-parlor/internal/wordgame"
)

const topicPlayerPositions = "player-positions"
const topicChatMessages = "chat-messages"
const topicGameSessions = "game-sessions"

const PlayerPositionsPartitions = 6
const ChatMessagesPartitions = 6
const GameSessionsPartitions = 6

type Gateway struct {
	brokers           []string
	allowedOrigins    []string
	rooms             *room.Registry
	hub               *hub
	sessions          *sessionStore
	moveWriter        *kafka.Writer
	chatWriter        *kafka.Writer
	gameSessionWriter *kafka.Writer
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
		sessions:       newSessionStore(),
		moveWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topicPlayerPositions,
			// Keying by room code keeps a room's events on a single
			// partition (ordering per room), and is the natural unit a
			// future multi-instance broadcaster would parallelize over.
			Balancer: &kafka.Hash{},
		},
		chatWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topicChatMessages,
			Balancer: &kafka.Hash{},
		},
		gameSessionWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topicGameSessions,
			Balancer: &kafka.Hash{}, // keyed by session id — see produce() call sites
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
		partition := partition
		err := startPartitionConsumer(ctx, g.brokers, topicPlayerPositions, partition, func(msg kafka.Message) {
			var evt schema.PlayerPositionEvent
			if err := json.Unmarshal(msg.Value, &evt); err != nil {
				log.Printf("gateway: bad player-position event: %v", err)
				return
			}

			g.hub.setPosition(evt.RoomID, evt.PlayerID, position{X: evt.TargetX, Y: evt.TargetY})

			data, err := marshalEnvelope(schema.TypePlayerMoved, evt)
			if err != nil {
				log.Printf("gateway: marshal player-moved broadcast: %v", err)
				return
			}
			g.hub.broadcast(ctx, evt.RoomID, data)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// StartChatFilter starts one background consumer per partition of
// chat-messages, running each message through the stub filter (see
// filter.go) and either broadcasting it (APPROVED) or sending it back
// only to its sender (REJECTED). Call once at startup, after
// EnsureTopic(ctx, "chat-messages", ...) — same startup-ordering and
// direct-partition-reader reasoning as StartMovementBroadcast.
func (g *Gateway) StartChatFilter(ctx context.Context) error {
	for partition := 0; partition < ChatMessagesPartitions; partition++ {
		partition := partition
		err := startPartitionConsumer(ctx, g.brokers, topicChatMessages, partition, func(msg kafka.Message) {
			var evt schema.ChatMessageEvent
			if err := json.Unmarshal(msg.Value, &evt); err != nil {
				log.Printf("gateway: bad chat-message event: %v", err)
				return
			}

			if isChatApproved(evt.RawText) {
				evt.Status = "APPROVED"
				data, err := marshalEnvelope(schema.TypeChatMessage, evt)
				if err != nil {
					log.Printf("gateway: marshal chat-message broadcast: %v", err)
					return
				}
				g.hub.broadcast(ctx, evt.RoomID, data)
			} else {
				evt.Status = "REJECTED"
				data, err := marshalEnvelope(schema.TypeChatRejected, evt)
				if err != nil {
					log.Printf("gateway: marshal chat-rejected: %v", err)
					return
				}
				g.hub.sendTo(ctx, evt.RoomID, evt.PlayerID, data)
			}
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// startPartitionConsumer positions a reader at partition's current end
// (retrying briefly, since a partition's leader metadata can take a
// moment to propagate right after CreateTopics) and starts a goroutine
// calling handle for every subsequent message. Positioning happens
// synchronously, before this function returns, so a message produced
// right after can't be missed — see StartMovementBroadcast's doc comment
// for why this avoids a consumer group.
func startPartitionConsumer(ctx context.Context, brokers []string, topic string, partition int, handle func(kafka.Message)) error {
	var startOffset int64
	err := retry(ctx, 25, 300*time.Millisecond, func() error {
		leader, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, partition)
		if err != nil {
			return err
		}
		defer leader.Close()
		startOffset, err = leader.ReadLastOffset()
		return err
	})
	if err != nil {
		return err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
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
			handle(msg)
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

		case schema.TypeChat:
			g.handleChat(ctx, conn, joinedRoomCode, playerID, env.Payload)

		case schema.TypeStartGame:
			g.handleStartGame(ctx, conn, env.Payload)

		case schema.TypeGuess:
			g.handleGuess(ctx, conn, env.Payload)

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

	if err := produce(ctx, g.moveWriter, roomCode, raw); err != nil {
		log.Printf("gateway: produce move event: %v", err)
		g.sendError(ctx, conn, "could not process move")
	}
}

func (g *Gateway) handleChat(ctx context.Context, conn *safeConn, roomCode, playerID string, payload json.RawMessage) {
	if roomCode == "" || playerID == "" {
		g.sendError(ctx, conn, "must join a room before chatting")
		return
	}
	var req schema.ChatRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		g.sendError(ctx, conn, "invalid chat payload")
		return
	}

	evt := schema.ChatMessageEvent{
		MessageID: newEventID(),
		Timestamp: time.Now().UnixMilli(),
		PlayerID:  playerID,
		RoomID:    roomCode,
		RawText:   req.Text,
		Status:    "PENDING_VALIDATION",
	}

	raw, err := json.Marshal(evt)
	if err != nil {
		log.Printf("gateway: marshal chat event: %v", err)
		g.sendError(ctx, conn, "internal error")
		return
	}

	if err := produce(ctx, g.chatWriter, roomCode, raw); err != nil {
		log.Printf("gateway: produce chat event: %v", err)
		g.sendError(ctx, conn, "could not process chat message")
	}
}

func (g *Gateway) handleStartGame(ctx context.Context, conn *safeConn, payload json.RawMessage) {
	var req schema.StartGameRequest
	if err := json.Unmarshal(payload, &req); err != nil || req.PlayerID == "" {
		g.sendError(ctx, conn, "invalid start_game payload")
		return
	}

	target, err := wordgame.RandomAnswer()
	if err != nil {
		log.Printf("gateway: pick random answer: %v", err)
		g.sendError(ctx, conn, "internal error")
		return
	}

	sessionID := newEventID()
	g.sessions.create(sessionID, req.PlayerID, target)

	evt := schema.GameSessionEvent{
		SessionID:  sessionID,
		PlayerID:   req.PlayerID,
		Timestamp:  time.Now().UnixMilli(),
		Action:     "SESSION_STARTED",
		WordLength: wordgame.WordLength,
		Guesses:    []string{},
		Status:     statusInProgress,
	}
	raw, err := json.Marshal(evt)
	if err != nil {
		log.Printf("gateway: marshal game-session event: %v", err)
		g.sendError(ctx, conn, "internal error")
		return
	}
	if err := produce(ctx, g.gameSessionWriter, sessionID, raw); err != nil {
		log.Printf("gateway: produce game-session event: %v", err)
		g.sendError(ctx, conn, "could not start game")
		return
	}

	g.send(ctx, conn, schema.TypeGameStarted, schema.GameStarted{
		SessionID:        sessionID,
		WordLength:       wordgame.WordLength,
		GuessesRemaining: wordgame.MaxGuesses,
	})
}

func (g *Gateway) handleGuess(ctx context.Context, conn *safeConn, payload json.RawMessage) {
	var req schema.GuessRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		g.sendError(ctx, conn, "invalid guess payload")
		return
	}

	sess, ok := g.sessions.get(req.SessionID)
	if !ok {
		g.sendError(ctx, conn, "unknown session id")
		return
	}
	if sess.Status != statusInProgress {
		g.sendError(ctx, conn, "game session already finished")
		return
	}

	guess := strings.ToLower(strings.TrimSpace(req.Guess))
	if len(guess) != wordgame.WordLength {
		g.sendError(ctx, conn, fmt.Sprintf("guess must be %d letters", wordgame.WordLength))
		return
	}
	if !wordgame.IsValidGuess(guess) {
		g.sendError(ctx, conn, "not a recognized word")
		return
	}

	feedback := wordgame.Evaluate(guess, sess.Target)

	status := statusInProgress
	switch {
	case guess == sess.Target:
		status = statusWon
	case len(sess.Guesses)+1 >= wordgame.MaxGuesses:
		status = statusLost
	}

	updated, ok := g.sessions.recordGuess(req.SessionID, guess, status)
	if !ok {
		g.sendError(ctx, conn, "unknown session id")
		return
	}

	guessEvt := schema.GameSessionEvent{
		SessionID:  req.SessionID,
		PlayerID:   updated.PlayerID,
		Timestamp:  time.Now().UnixMilli(),
		Action:     "GUESS_EVALUATED",
		WordLength: wordgame.WordLength,
		Guesses:    updated.Guesses,
		Status:     updated.Status,
	}
	raw, err := json.Marshal(guessEvt)
	if err != nil {
		log.Printf("gateway: marshal game-session event: %v", err)
		g.sendError(ctx, conn, "internal error")
		return
	}
	if err := produce(ctx, g.gameSessionWriter, req.SessionID, raw); err != nil {
		log.Printf("gateway: produce game-session event: %v", err)
		g.sendError(ctx, conn, "could not process guess")
		return
	}

	if status != statusInProgress {
		completedEvt := guessEvt
		completedEvt.Action = "SESSION_COMPLETED"
		if raw, err := json.Marshal(completedEvt); err != nil {
			log.Printf("gateway: marshal session-completed event: %v", err)
		} else if err := produce(ctx, g.gameSessionWriter, req.SessionID, raw); err != nil {
			log.Printf("gateway: produce session-completed event: %v", err)
		}
	}

	letterFeedback := make([]schema.LetterFeedback, len(feedback))
	for i, st := range feedback {
		letterFeedback[i] = schema.LetterFeedback{Letter: string(guess[i]), State: string(st)}
	}

	g.send(ctx, conn, schema.TypeGuessResult, schema.GuessResult{
		SessionID:        req.SessionID,
		Guess:            guess,
		Feedback:         letterFeedback,
		GuessesRemaining: wordgame.MaxGuesses - len(updated.Guesses),
		Status:           updated.Status,
	})
}

// produce writes a single message keyed by key, retrying briefly on
// transient errors like "Unknown Topic Or Partition" — the producer-side
// counterpart to startPartitionConsumer's retry: right after EnsureTopic
// creates a topic, a connection's cached metadata can be stale for a
// moment even on a single broker.
func produce(ctx context.Context, writer *kafka.Writer, key string, value []byte) error {
	return retry(ctx, 25, 300*time.Millisecond, func() error {
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return writer.WriteMessages(writeCtx, kafka.Message{Key: []byte(key), Value: value})
	})
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

// retry calls fn until it succeeds, ctx is done, or attempts is exhausted,
// waiting delay between tries.
func retry(ctx context.Context, attempts int, delay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}
