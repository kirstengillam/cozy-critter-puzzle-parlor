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

	"cozy-critter-puzzle-parlor/internal/connectionsgame"
	"cozy-critter-puzzle-parlor/internal/room"
	"cozy-critter-puzzle-parlor/internal/schema"
	"cozy-critter-puzzle-parlor/internal/wordgame"
)

const topicPlayerPositions = "player-positions"
const topicChatMessages = "chat-messages"
const topicGameSessions = "game-sessions"
const topicEconomyLedger = "economy-ledger"
const topicConnectionsSessions = "connections-sessions"

const PlayerPositionsPartitions = 6
const ChatMessagesPartitions = 6
const GameSessionsPartitions = 6
const EconomyLedgerPartitions = 6
const ConnectionsSessionsPartitions = 6

type Gateway struct {
	brokers                  []string
	allowedOrigins           []string
	rooms                    *room.Registry
	hub                      *hub
	sessions                 *sessionStore
	connectionsSessions      *connectionsSessionStore
	economy                  *economyStore
	ledgerSecret             []byte
	moveWriter               *kafka.Writer
	chatWriter               *kafka.Writer
	gameSessionWriter        *kafka.Writer
	economyLedgerWriter      *kafka.Writer
	connectionsSessionWriter *kafka.Writer
}

// New creates a Gateway. allowedOrigins lists origin patterns (per
// coder/websocket's AcceptOptions.OriginPatterns) permitted to open a
// WebSocket connection from a browser; a nil/empty slice accepts same-origin
// requests only.
func New(brokers []string, allowedOrigins []string) *Gateway {
	ledgerSecret := make([]byte, 32)
	_, _ = rand.Read(ledgerSecret)

	return &Gateway{
		brokers:             brokers,
		allowedOrigins:      allowedOrigins,
		rooms:               room.NewRegistry(),
		hub:                 newHub(),
		sessions:            newSessionStore(),
		connectionsSessions: newConnectionsSessionStore(),
		economy:             newEconomyStore(),
		ledgerSecret:        ledgerSecret,
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
		economyLedgerWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topicEconomyLedger,
			Balancer: &kafka.Hash{}, // keyed by player id
		},
		connectionsSessionWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topicConnectionsSessions,
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

// StartEconomyLedger starts one background consumer per partition of
// economy-ledger. Every entry is verified (see economy.go's
// signLedgerEvent/verifyLedgerEvent — a real HMAC check, not
// decorative) before being applied to the in-memory balance store; a
// failed check is logged and dropped rather than applied. On success,
// pushes the player's updated balance directly to them (not room-scoped
// — see hub.sendToPlayer).
func (g *Gateway) StartEconomyLedger(ctx context.Context) error {
	for partition := 0; partition < EconomyLedgerPartitions; partition++ {
		partition := partition
		err := startPartitionConsumer(ctx, g.brokers, topicEconomyLedger, partition, func(msg kafka.Message) {
			var evt schema.EconomyLedgerEvent
			if err := json.Unmarshal(msg.Value, &evt); err != nil {
				log.Printf("gateway: bad economy-ledger event: %v", err)
				return
			}
			if !verifyLedgerEvent(g.ledgerSecret, evt) {
				log.Printf("gateway: economy-ledger event %s failed verification, dropping", evt.TransactionID)
				return
			}

			balance, err := g.economy.apply(evt.Action, evt.PlayerID, evt.Amount)
			if err != nil {
				log.Printf("gateway: apply ledger event %s: %v", evt.TransactionID, err)
				return
			}

			data, err := marshalEnvelope(schema.TypeBalanceUpdated, schema.BalanceUpdated{
				PlayerID:     evt.PlayerID,
				Balance:      balance,
				CurrencyType: evt.CurrencyType,
			})
			if err != nil {
				log.Printf("gateway: marshal balance-updated: %v", err)
				return
			}
			g.hub.sendToPlayer(ctx, evt.PlayerID, data)
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

	// Cloudflare (sitting in front of the production deployment) drops
	// WebSocket connections after ~100s of no data, which a quiet stretch
	// of puzzle-solving with no chat/movement can easily exceed. Ping on a
	// shorter interval to keep the connection alive through any idle proxy.
	pingCtx, cancelPing := context.WithCancel(ctx)
	defer cancelPing()
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				pingReqCtx, cancel := context.WithTimeout(pingCtx, 10*time.Second)
				err := wsConn.Ping(pingReqCtx)
				cancel()
				if err != nil {
					wsConn.Close(websocket.StatusGoingAway, "ping failed")
					return
				}
			}
		}
	}()

	// Which room/player this connection has joined, if any. playerID is
	// set by either JOIN_ROOM or START_GAME — the word game doesn't
	// require room membership, so it can be the only one of the two to
	// ever fire for a given connection.
	var joinedRoomCode, playerID string
	defer func() {
		if joinedRoomCode != "" && playerID != "" {
			g.hub.leave(joinedRoomCode, playerID)
		}
		if playerID != "" {
			g.hub.unregisterPlayer(playerID)
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
			if id := g.handleStartGame(ctx, conn, env.Payload); id != "" {
				playerID = id
			}

		case schema.TypeGuess:
			g.handleGuess(ctx, conn, env.Payload)

		case schema.TypeStartConnections:
			if id := g.handleStartConnections(ctx, conn, env.Payload); id != "" {
				playerID = id
			}

		case schema.TypeConnectionsGuess:
			g.handleConnectionsGuess(ctx, conn, env.Payload)

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
	g.hub.registerPlayer(req.PlayerID, conn)
	g.hub.setDisplayName(req.PlayerID, req.DisplayName)

	// Bring the joiner up to speed on everyone already in the room before
	// telling them they've joined, so their client has sprites for
	// existing players immediately instead of only after each one's next
	// move. The reverse direction — existing players seeing this new
	// player — is handled by announceJoin below.
	g.sendRoomSnapshot(ctx, conn, req.RoomCode, req.PlayerID)
	g.send(ctx, conn, schema.TypeJoined, schema.Joined{RoomCode: req.RoomCode, PlayerID: req.PlayerID})
	g.announceJoin(ctx, req.RoomCode, req.PlayerID)

	return req.RoomCode, req.PlayerID, nil
}

// sendRoomSnapshot sends conn one PLAYER_MOVED-shaped event per player
// already in roomCode (current position == target position, so the
// client spawns each sprite in place with no tween), skipping the
// joining player itself.
func (g *Gateway) sendRoomSnapshot(ctx context.Context, conn *safeConn, roomCode, joiningPlayerID string) {
	for id, pos := range g.hub.roomSnapshot(roomCode) {
		if id == joiningPlayerID {
			continue
		}
		evt := schema.PlayerPositionEvent{
			PlayerID:    id,
			DisplayName: g.hub.displayNameFor(id),
			RoomID:      roomCode,
			Action:      "MOVE",
			CurrentX:    pos.X,
			CurrentY:    pos.Y,
			TargetX:     pos.X,
			TargetY:     pos.Y,
		}
		g.send(ctx, conn, schema.TypePlayerMoved, evt)
	}
}

// announceJoin broadcasts playerID's current position to every connection
// already in roomCode (including the joiner's own, now-registered
// connection), so a newly-joined player's sprite appears for everyone
// else immediately instead of only after their first move.
func (g *Gateway) announceJoin(ctx context.Context, roomCode, playerID string) {
	pos := g.hub.currentPosition(roomCode, playerID)
	evt := schema.PlayerPositionEvent{
		EventID:     newEventID(),
		Timestamp:   time.Now().UnixMilli(),
		PlayerID:    playerID,
		DisplayName: g.hub.displayNameFor(playerID),
		RoomID:      roomCode,
		Action:      "JOIN",
		CurrentX:    pos.X,
		CurrentY:    pos.Y,
		TargetX:     pos.X,
		TargetY:     pos.Y,
	}
	data, err := marshalEnvelope(schema.TypePlayerMoved, evt)
	if err != nil {
		log.Printf("gateway: marshal join announcement: %v", err)
		return
	}
	g.hub.broadcast(ctx, roomCode, data)
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
		DisplayName:     g.hub.displayNameFor(playerID),
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
		MessageID:   newEventID(),
		Timestamp:   time.Now().UnixMilli(),
		PlayerID:    playerID,
		DisplayName: g.hub.displayNameFor(playerID),
		RoomID:      roomCode,
		RawText:     req.Text,
		Status:      "PENDING_VALIDATION",
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

// handleStartGame returns the requesting player's id on success, or ""
// on any failure — used by handleWS to track this connection's player
// identity for cleanup, since the word game doesn't require a prior
// room join.
func (g *Gateway) handleStartGame(ctx context.Context, conn *safeConn, payload json.RawMessage) string {
	var req schema.StartGameRequest
	if err := json.Unmarshal(payload, &req); err != nil || req.PlayerID == "" {
		g.sendError(ctx, conn, "invalid start_game payload")
		return ""
	}

	target, err := wordgame.RandomAnswer()
	if err != nil {
		log.Printf("gateway: pick random answer: %v", err)
		g.sendError(ctx, conn, "internal error")
		return ""
	}

	sessionID := newEventID()
	g.sessions.create(sessionID, req.PlayerID, target)
	g.hub.registerPlayer(req.PlayerID, conn)

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
		return ""
	}
	if err := produce(ctx, g.gameSessionWriter, sessionID, raw); err != nil {
		log.Printf("gateway: produce game-session event: %v", err)
		g.sendError(ctx, conn, "could not start game")
		return ""
	}

	g.send(ctx, conn, schema.TypeGameStarted, schema.GameStarted{
		SessionID:        sessionID,
		WordLength:       wordgame.WordLength,
		GuessesRemaining: wordgame.MaxGuesses,
	})
	return req.PlayerID
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

	if status == statusWon {
		g.creditWordGameWin(ctx, updated.PlayerID, req.SessionID, len(updated.Guesses))
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

// creditWordGameWin produces a signed CREDIT event to economy-ledger for
// a word-game win. The reward and balance update aren't computed here —
// StartEconomyLedger reacts to this event independently, same as
// movement/chat's produce-then-consume pattern.
func (g *Gateway) creditWordGameWin(ctx context.Context, playerID, sessionID string, guessesUsed int) {
	evt := schema.EconomyLedgerEvent{
		TransactionID: newEventID(),
		Timestamp:     time.Now().UnixMilli(),
		PlayerID:      playerID,
		GameSessionID: sessionID,
		Action:        "CREDIT",
		Amount:        wordgame.RewardForWin(guessesUsed),
		CurrencyType:  schema.CurrencyCritterCoins,
	}
	evt.VerificationHash = signLedgerEvent(g.ledgerSecret, evt)

	raw, err := json.Marshal(evt)
	if err != nil {
		log.Printf("gateway: marshal economy-ledger event: %v", err)
		return
	}
	if err := produce(ctx, g.economyLedgerWriter, playerID, raw); err != nil {
		log.Printf("gateway: produce economy-ledger event: %v", err)
	}
}

// handleStartConnections returns the requesting player's id on success,
// or "" on any failure — same role as handleStartGame's return value,
// used by handleWS to track this connection's player identity for
// cleanup since Connections doesn't require a prior room join either.
func (g *Gateway) handleStartConnections(ctx context.Context, conn *safeConn, payload json.RawMessage) string {
	var req schema.StartConnectionsRequest
	if err := json.Unmarshal(payload, &req); err != nil || req.PlayerID == "" {
		g.sendError(ctx, conn, "invalid start_connections payload")
		return ""
	}

	puzzle, err := connectionsgame.RandomPuzzle()
	if err != nil {
		log.Printf("gateway: pick random connections puzzle: %v", err)
		g.sendError(ctx, conn, "internal error")
		return ""
	}
	words, err := connectionsgame.ShuffledWords(puzzle)
	if err != nil {
		log.Printf("gateway: shuffle connections words: %v", err)
		g.sendError(ctx, conn, "internal error")
		return ""
	}

	sessionID := newEventID()
	g.connectionsSessions.create(sessionID, req.PlayerID, puzzle)
	g.hub.registerPlayer(req.PlayerID, conn)

	evt := schema.ConnectionsSessionEvent{
		SessionID: sessionID,
		PlayerID:  req.PlayerID,
		Timestamp: time.Now().UnixMilli(),
		Action:    "SESSION_STARTED",
		PuzzleID:  puzzle.ID,
		Status:    statusInProgress,
	}
	raw, err := json.Marshal(evt)
	if err != nil {
		log.Printf("gateway: marshal connections-session event: %v", err)
		g.sendError(ctx, conn, "internal error")
		return ""
	}
	if err := produce(ctx, g.connectionsSessionWriter, sessionID, raw); err != nil {
		log.Printf("gateway: produce connections-session event: %v", err)
		g.sendError(ctx, conn, "could not start game")
		return ""
	}

	g.send(ctx, conn, schema.TypeConnectionsStarted, schema.ConnectionsStarted{
		SessionID:   sessionID,
		Words:       words,
		MaxMistakes: connectionsgame.MaxMistakes,
	})
	return req.PlayerID
}

func (g *Gateway) handleConnectionsGuess(ctx context.Context, conn *safeConn, payload json.RawMessage) {
	var req schema.ConnectionsGuessRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		g.sendError(ctx, conn, "invalid connections_guess payload")
		return
	}

	sess, ok := g.connectionsSessions.get(req.SessionID)
	if !ok {
		g.sendError(ctx, conn, "unknown session id")
		return
	}
	if sess.Status != statusInProgress {
		g.sendError(ctx, conn, "game session already finished")
		return
	}
	if len(req.Members) != connectionsgame.GroupSize {
		g.sendError(ctx, conn, fmt.Sprintf("guess must have %d words", connectionsgame.GroupSize))
		return
	}
	for _, m := range req.Members {
		if !connectionsgame.IsValidWord(m, sess.Puzzle) {
			g.sendError(ctx, conn, "guess contains a word not in this puzzle")
			return
		}
	}

	groupIndex, oneAway := connectionsgame.EvaluateGuess(req.Members, sess.Puzzle)
	if groupIndex >= 0 && sess.SolvedGroups[groupIndex] {
		g.sendError(ctx, conn, "group already solved")
		return
	}

	status := statusInProgress
	mistakesUsed := sess.MistakesUsed
	if groupIndex < 0 {
		mistakesUsed++
		if mistakesUsed >= connectionsgame.MaxMistakes {
			status = statusLost
		}
	} else if len(sess.SolvedGroups)+1 >= len(sess.Puzzle.Answers) {
		status = statusWon
	}

	updated, ok := g.connectionsSessions.recordGuess(req.SessionID, groupIndex, status)
	if !ok {
		g.sendError(ctx, conn, "unknown session id")
		return
	}

	guessEvt := schema.ConnectionsSessionEvent{
		SessionID:    req.SessionID,
		PlayerID:     updated.PlayerID,
		Timestamp:    time.Now().UnixMilli(),
		Action:       "GUESS_EVALUATED",
		PuzzleID:     updated.Puzzle.ID,
		MistakesUsed: updated.MistakesUsed,
		Status:       updated.Status,
	}
	raw, err := json.Marshal(guessEvt)
	if err != nil {
		log.Printf("gateway: marshal connections-session event: %v", err)
		g.sendError(ctx, conn, "internal error")
		return
	}
	if err := produce(ctx, g.connectionsSessionWriter, req.SessionID, raw); err != nil {
		log.Printf("gateway: produce connections-session event: %v", err)
		g.sendError(ctx, conn, "could not process guess")
		return
	}

	if status != statusInProgress {
		completedEvt := guessEvt
		completedEvt.Action = "SESSION_COMPLETED"
		if raw, err := json.Marshal(completedEvt); err != nil {
			log.Printf("gateway: marshal session-completed event: %v", err)
		} else if err := produce(ctx, g.connectionsSessionWriter, req.SessionID, raw); err != nil {
			log.Printf("gateway: produce session-completed event: %v", err)
		}
	}

	if status == statusWon {
		g.creditConnectionsWin(ctx, updated.PlayerID, req.SessionID, updated.MistakesUsed)
	}

	var solvedGroup *schema.ConnectionsSolvedGroup
	solvedGroups := make([]schema.ConnectionsSolvedGroup, 0, len(updated.SolvedGroups))
	for idx := range updated.Puzzle.Answers {
		if !updated.SolvedGroups[idx] {
			continue
		}
		grp := updated.Puzzle.Answers[idx]
		sg := schema.ConnectionsSolvedGroup{Level: grp.Level, Name: grp.Name, Members: grp.Members}
		solvedGroups = append(solvedGroups, sg)
		if idx == groupIndex {
			solvedGroup = &sg
		}
	}

	g.send(ctx, conn, schema.TypeConnectionsResult, schema.ConnectionsResult{
		SessionID:    req.SessionID,
		Correct:      groupIndex >= 0,
		OneAway:      oneAway,
		SolvedGroup:  solvedGroup,
		SolvedGroups: solvedGroups,
		MistakesUsed: updated.MistakesUsed,
		MaxMistakes:  connectionsgame.MaxMistakes,
		Status:       updated.Status,
	})
}

// creditConnectionsWin produces a signed CREDIT event to economy-ledger
// for a Connections win — same produce-then-consume pattern as
// creditWordGameWin.
func (g *Gateway) creditConnectionsWin(ctx context.Context, playerID, sessionID string, mistakesUsed int) {
	evt := schema.EconomyLedgerEvent{
		TransactionID: newEventID(),
		Timestamp:     time.Now().UnixMilli(),
		PlayerID:      playerID,
		GameSessionID: sessionID,
		Action:        "CREDIT",
		Amount:        connectionsgame.RewardForWin(mistakesUsed),
		CurrencyType:  schema.CurrencyCritterCoins,
	}
	evt.VerificationHash = signLedgerEvent(g.ledgerSecret, evt)

	raw, err := json.Marshal(evt)
	if err != nil {
		log.Printf("gateway: marshal economy-ledger event: %v", err)
		return
	}
	if err := produce(ctx, g.economyLedgerWriter, playerID, raw); err != nil {
		log.Printf("gateway: produce economy-ledger event: %v", err)
	}
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
