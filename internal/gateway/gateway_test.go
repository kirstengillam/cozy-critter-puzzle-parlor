package gateway

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"cozy-critter-puzzle-parlor/internal/schema"
)

func dial(t *testing.T, srv *httptest.Server) (*websocket.Conn, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	return conn, ctx, cancel
}

func sendEnvelope(t *testing.T, ctx context.Context, conn *websocket.Conn, msgType string, payload any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := schema.Envelope{Type: msgType, Payload: raw}
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readEnvelope(t *testing.T, ctx context.Context, conn *websocket.Conn) schema.Envelope {
	t.Helper()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var env schema.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	return env
}

func TestCreateAndJoinRoom(t *testing.T) {
	gw := New(nil, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	creator, ctx, cancel := dial(t, srv)
	defer cancel()
	defer creator.CloseNow()

	sendEnvelope(t, ctx, creator, schema.TypeCreateRoom, struct{}{})
	env := readEnvelope(t, ctx, creator)
	if env.Type != schema.TypeRoomCreated {
		t.Fatalf("got envelope type %q, want %q", env.Type, schema.TypeRoomCreated)
	}
	var created schema.RoomCreated
	if err := json.Unmarshal(env.Payload, &created); err != nil {
		t.Fatalf("unmarshal RoomCreated: %v", err)
	}
	if created.RoomCode == "" {
		t.Fatal("RoomCreated.RoomCode is empty")
	}

	joiner, jctx, jcancel := dial(t, srv)
	defer jcancel()
	defer joiner.CloseNow()

	sendEnvelope(t, jctx, joiner, schema.TypeJoinRoom, schema.JoinRoomRequest{
		PlayerID: "player_capy_1",
		RoomCode: created.RoomCode,
	})
	joinEnv := readEnvelope(t, jctx, joiner)
	if joinEnv.Type != schema.TypeJoined {
		t.Fatalf("got envelope type %q, want %q", joinEnv.Type, schema.TypeJoined)
	}
	var joined schema.Joined
	if err := json.Unmarshal(joinEnv.Payload, &joined); err != nil {
		t.Fatalf("unmarshal Joined: %v", err)
	}
	if joined.RoomCode != created.RoomCode {
		t.Fatalf("joined room code %q, want %q", joined.RoomCode, created.RoomCode)
	}
	if joined.PlayerID != "player_capy_1" {
		t.Fatalf("joined player_id %q, want %q", joined.PlayerID, "player_capy_1")
	}
}

func TestJoinUnknownRoomCode(t *testing.T) {
	gw := New(nil, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	conn, ctx, cancel := dial(t, srv)
	defer cancel()
	defer conn.CloseNow()

	sendEnvelope(t, ctx, conn, schema.TypeJoinRoom, schema.JoinRoomRequest{
		PlayerID: "player_frog_1",
		RoomCode: "NOPE00",
	})
	env := readEnvelope(t, ctx, conn)
	if env.Type != schema.TypeError {
		t.Fatalf("got envelope type %q, want %q", env.Type, schema.TypeError)
	}
}

func TestUnknownMessageType(t *testing.T) {
	gw := New(nil, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	conn, ctx, cancel := dial(t, srv)
	defer cancel()
	defer conn.CloseNow()

	sendEnvelope(t, ctx, conn, "NOT_A_REAL_TYPE", struct{}{})
	env := readEnvelope(t, ctx, conn)
	if env.Type != schema.TypeError {
		t.Fatalf("got envelope type %q, want %q", env.Type, schema.TypeError)
	}
}
