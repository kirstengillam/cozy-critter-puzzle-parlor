package gateway

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// safeConn serializes writes to a single WebSocket connection. It's
// needed because both the connection's own read loop (responses,
// errors) and the shared hub's broadcaster goroutine (other players'
// moves) can write to the same connection concurrently.
type safeConn struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (c *safeConn) Write(ctx context.Context, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

type position struct {
	X, Y int
}

// hub tracks, per room, which connections are present and each
// player's last known position. It's the single-instance-gateway
// broadcast mechanism described in IMPLEMENTATION_PLAN.md 2.6 — the
// seam where multi-instance fan-out would later need to route across
// gateway instances instead of just iterating an in-process map.
type hub struct {
	mu       sync.Mutex
	conns    map[string]map[string]*safeConn // room code -> player id -> conn
	position map[string]map[string]position  // room code -> player id -> last position
}

func newHub() *hub {
	return &hub{
		conns:    make(map[string]map[string]*safeConn),
		position: make(map[string]map[string]position),
	}
}

func (h *hub) join(roomCode, playerID string, conn *safeConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[roomCode] == nil {
		h.conns[roomCode] = make(map[string]*safeConn)
		h.position[roomCode] = make(map[string]position)
	}
	h.conns[roomCode][playerID] = conn
	if _, ok := h.position[roomCode][playerID]; !ok {
		h.position[roomCode][playerID] = position{} // spawn at the origin
	}
}

func (h *hub) leave(roomCode, playerID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns[roomCode], playerID)
	delete(h.position[roomCode], playerID)
}

func (h *hub) currentPosition(roomCode, playerID string) position {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.position[roomCode][playerID]
}

func (h *hub) setPosition(roomCode, playerID string, p position) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.position[roomCode] == nil {
		h.position[roomCode] = make(map[string]position)
	}
	h.position[roomCode][playerID] = p
}

// broadcast sends data to every connection currently in roomCode.
func (h *hub) broadcast(ctx context.Context, roomCode string, data []byte) {
	h.mu.Lock()
	targets := make([]*safeConn, 0, len(h.conns[roomCode]))
	for _, c := range h.conns[roomCode] {
		targets = append(targets, c)
	}
	h.mu.Unlock()

	for _, c := range targets {
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := c.Write(writeCtx, data); err != nil {
			log.Printf("gateway: broadcast to room %s: %v", roomCode, err)
		}
		cancel()
	}
}

// sendTo sends data to a single connection, if that player is currently
// in roomCode. Used for feedback (like a rejected chat message) that
// should reach only the player who triggered it, not the whole room.
func (h *hub) sendTo(ctx context.Context, roomCode, playerID string, data []byte) {
	h.mu.Lock()
	c := h.conns[roomCode][playerID]
	h.mu.Unlock()
	if c == nil {
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Write(writeCtx, data); err != nil {
		log.Printf("gateway: send to %s/%s: %v", roomCode, playerID, err)
	}
}
