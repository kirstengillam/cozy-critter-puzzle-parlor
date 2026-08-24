package gateway

import (
	"context"
	"encoding/json"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"cozy-critter-puzzle-parlor/internal/schema"
	"cozy-critter-puzzle-parlor/internal/wordgame"
)

// dial opens a WebSocket connection to srv. Callers provide their own
// context for subsequent reads/writes so one shared deadline covers the
// whole test, rather than each connection racing its own clock.
func dial(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	conn, _, err := websocket.Dial(dialCtx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gw := New(nil, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	creator := dial(t, srv)
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

	joiner := dial(t, srv)
	defer joiner.CloseNow()

	sendEnvelope(t, ctx, joiner, schema.TypeJoinRoom, schema.JoinRoomRequest{
		PlayerID: "player_capy_1",
		RoomCode: created.RoomCode,
	})
	joinEnv := readEnvelope(t, ctx, joiner)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gw := New(nil, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	conn := dial(t, srv)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gw := New(nil, nil)
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	conn := dial(t, srv)
	defer conn.CloseNow()

	sendEnvelope(t, ctx, conn, "NOT_A_REAL_TYPE", struct{}{})
	env := readEnvelope(t, ctx, conn)
	if env.Type != schema.TypeError {
		t.Fatalf("got envelope type %q, want %q", env.Type, schema.TypeError)
	}
}

func TestMovementBroadcast(t *testing.T) {
	const broker = "localhost:9092"

	tcpConn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s (is `docker compose up` running in deploy/compose?): %v", broker, err)
	}
	tcpConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{broker}, nil)
	if err := gw.EnsureTopic(ctx, "player-positions", PlayerPositionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	if err := gw.StartMovementBroadcast(ctx); err != nil {
		t.Fatalf("start movement broadcast: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	mover := dial(t, srv)
	defer mover.CloseNow()

	observer := dial(t, srv)
	defer observer.CloseNow()

	sendEnvelope(t, ctx, mover, schema.TypeCreateRoom, struct{}{})
	created := readEnvelope(t, ctx, mover)
	var room schema.RoomCreated
	if err := json.Unmarshal(created.Payload, &room); err != nil {
		t.Fatalf("unmarshal RoomCreated: %v", err)
	}

	sendEnvelope(t, ctx, mover, schema.TypeJoinRoom, schema.JoinRoomRequest{PlayerID: "mover", RoomCode: room.RoomCode})
	if env := readEnvelope(t, ctx, mover); env.Type != schema.TypeJoined {
		t.Fatalf("mover join: got %q, want JOINED", env.Type)
	}

	sendEnvelope(t, ctx, observer, schema.TypeJoinRoom, schema.JoinRoomRequest{PlayerID: "observer", RoomCode: room.RoomCode})
	if env := readEnvelope(t, ctx, observer); env.Type != schema.TypeJoined {
		t.Fatalf("observer join: got %q, want JOINED", env.Type)
	}

	sendEnvelope(t, ctx, mover, schema.TypeMove, schema.MoveRequest{TargetX: 42, TargetY: 7, FacingDirection: "NORTH_EAST"})

	checkMoved := func(who string, env schema.Envelope) {
		t.Helper()
		if env.Type != schema.TypePlayerMoved {
			t.Fatalf("%s: got envelope type %q, want %q", who, env.Type, schema.TypePlayerMoved)
		}
		var evt schema.PlayerPositionEvent
		if err := json.Unmarshal(env.Payload, &evt); err != nil {
			t.Fatalf("%s: unmarshal PlayerPositionEvent: %v", who, err)
		}
		if evt.PlayerID != "mover" || evt.RoomID != room.RoomCode || evt.TargetX != 42 || evt.TargetY != 7 {
			t.Fatalf("%s: unexpected event %+v", who, evt)
		}
	}

	checkMoved("observer", readEnvelope(t, ctx, observer))
	checkMoved("mover", readEnvelope(t, ctx, mover))
}

func TestChatApprovedBroadcastsAndRejectedStaysPrivate(t *testing.T) {
	const broker = "localhost:9092"

	tcpConn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s (is `docker compose up` running in deploy/compose?): %v", broker, err)
	}
	tcpConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{broker}, nil)
	if err := gw.EnsureTopic(ctx, "chat-messages", ChatMessagesPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	if err := gw.StartChatFilter(ctx); err != nil {
		t.Fatalf("start chat filter: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	speaker := dial(t, srv)
	defer speaker.CloseNow()

	listener := dial(t, srv)
	defer listener.CloseNow()

	sendEnvelope(t, ctx, speaker, schema.TypeCreateRoom, struct{}{})
	created := readEnvelope(t, ctx, speaker)
	var room schema.RoomCreated
	if err := json.Unmarshal(created.Payload, &room); err != nil {
		t.Fatalf("unmarshal RoomCreated: %v", err)
	}

	sendEnvelope(t, ctx, speaker, schema.TypeJoinRoom, schema.JoinRoomRequest{PlayerID: "speaker", RoomCode: room.RoomCode})
	if env := readEnvelope(t, ctx, speaker); env.Type != schema.TypeJoined {
		t.Fatalf("speaker join: got %q, want JOINED", env.Type)
	}

	sendEnvelope(t, ctx, listener, schema.TypeJoinRoom, schema.JoinRoomRequest{PlayerID: "listener", RoomCode: room.RoomCode})
	if env := readEnvelope(t, ctx, listener); env.Type != schema.TypeJoined {
		t.Fatalf("listener join: got %q, want JOINED", env.Type)
	}

	// Approved message: both speaker and listener should see it broadcast.
	sendEnvelope(t, ctx, speaker, schema.TypeChat, schema.ChatRequest{Text: "hey everyone!"})

	checkApproved := func(who, wantText string, env schema.Envelope) {
		t.Helper()
		if env.Type != schema.TypeChatMessage {
			t.Fatalf("%s: got envelope type %q, want %q", who, env.Type, schema.TypeChatMessage)
		}
		var evt schema.ChatMessageEvent
		if err := json.Unmarshal(env.Payload, &evt); err != nil {
			t.Fatalf("%s: unmarshal ChatMessageEvent: %v", who, err)
		}
		if evt.PlayerID != "speaker" || evt.RoomID != room.RoomCode || evt.RawText != wantText || evt.Status != "APPROVED" {
			t.Fatalf("%s: unexpected event %+v", who, evt)
		}
	}
	checkApproved("listener", "hey everyone!", readEnvelope(t, ctx, listener))
	checkApproved("speaker", "hey everyone!", readEnvelope(t, ctx, speaker))

	// Rejected message: only the speaker should hear back, and only
	// CHAT_REJECTED — the listener gets nothing for this one.
	sendEnvelope(t, ctx, speaker, schema.TypeChat, schema.ChatRequest{Text: "you're a badword"})

	rejectedEnv := readEnvelope(t, ctx, speaker)
	if rejectedEnv.Type != schema.TypeChatRejected {
		t.Fatalf("speaker: got envelope type %q, want %q", rejectedEnv.Type, schema.TypeChatRejected)
	}
	var rejected schema.ChatMessageEvent
	if err := json.Unmarshal(rejectedEnv.Payload, &rejected); err != nil {
		t.Fatalf("unmarshal rejected ChatMessageEvent: %v", err)
	}
	if rejected.Status != "REJECTED" || rejected.PlayerID != "speaker" {
		t.Fatalf("unexpected rejected event %+v", rejected)
	}

	// Confirm the listener never receives anything for the rejected
	// message: send one more approved message and check it's the very
	// next thing the listener sees.
	sendEnvelope(t, ctx, speaker, schema.TypeChat, schema.ChatRequest{Text: "sorry, ignore that"})
	checkApproved("listener (after rejection)", "sorry, ignore that", readEnvelope(t, ctx, listener))
}

// differentAnswer returns a random answer guaranteed not to equal avoid.
func differentAnswer(t *testing.T, avoid string) string {
	t.Helper()
	for i := 0; i < 50; i++ {
		w, err := wordgame.RandomAnswer()
		if err != nil {
			t.Fatalf("RandomAnswer: %v", err)
		}
		if w != avoid {
			return w
		}
	}
	t.Fatal("could not find an answer different from avoid after 50 tries")
	return ""
}

func TestWordGameStartAndWin(t *testing.T) {
	const broker = "localhost:9092"
	tcpConn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s (is `docker compose up` running in deploy/compose?): %v", broker, err)
	}
	tcpConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{broker}, nil)
	if err := gw.EnsureTopic(ctx, "game-sessions", GameSessionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	player := dial(t, srv)
	defer player.CloseNow()

	sendEnvelope(t, ctx, player, schema.TypeStartGame, schema.StartGameRequest{PlayerID: "wordy"})
	startEnv := readEnvelope(t, ctx, player)
	if startEnv.Type != schema.TypeGameStarted {
		t.Fatalf("got envelope type %q, want %q", startEnv.Type, schema.TypeGameStarted)
	}
	var started schema.GameStarted
	if err := json.Unmarshal(startEnv.Payload, &started); err != nil {
		t.Fatalf("unmarshal GameStarted: %v", err)
	}
	if started.SessionID == "" {
		t.Fatal("GameStarted.SessionID is empty")
	}
	if started.WordLength != wordgame.WordLength {
		t.Fatalf("WordLength = %d, want %d", started.WordLength, wordgame.WordLength)
	}
	if started.GuessesRemaining != wordgame.MaxGuesses {
		t.Fatalf("GuessesRemaining = %d, want %d", started.GuessesRemaining, wordgame.MaxGuesses)
	}

	// White-box peek at the session's target — the protocol deliberately
	// never reveals it to a real client, but the test needs it to guess
	// correctly on the first try.
	sess, ok := gw.sessions.get(started.SessionID)
	if !ok {
		t.Fatalf("session %q not found in store", started.SessionID)
	}
	target := sess.Target

	// A wrong guess first (a real word, guaranteed not the target),
	// exercising per-letter feedback and IN_PROGRESS status.
	wrong := differentAnswer(t, target)
	sendEnvelope(t, ctx, player, schema.TypeGuess, schema.GuessRequest{SessionID: started.SessionID, Guess: wrong})
	wrongEnv := readEnvelope(t, ctx, player)
	if wrongEnv.Type != schema.TypeGuessResult {
		t.Fatalf("got envelope type %q, want %q", wrongEnv.Type, schema.TypeGuessResult)
	}
	var wrongResult schema.GuessResult
	if err := json.Unmarshal(wrongEnv.Payload, &wrongResult); err != nil {
		t.Fatalf("unmarshal GuessResult: %v", err)
	}
	if wrongResult.Status != "IN_PROGRESS" {
		t.Fatalf("status after wrong guess = %q, want IN_PROGRESS", wrongResult.Status)
	}
	if wrongResult.GuessesRemaining != wordgame.MaxGuesses-1 {
		t.Fatalf("guesses remaining = %d, want %d", wrongResult.GuessesRemaining, wordgame.MaxGuesses-1)
	}
	allCorrect := true
	for _, f := range wrongResult.Feedback {
		if f.State != "CORRECT" {
			allCorrect = false
		}
	}
	if allCorrect {
		t.Fatalf("wrong guess %q reported all CORRECT against target %q", wrong, target)
	}

	// Now the winning guess.
	sendEnvelope(t, ctx, player, schema.TypeGuess, schema.GuessRequest{SessionID: started.SessionID, Guess: target})
	winEnv := readEnvelope(t, ctx, player)
	var winResult schema.GuessResult
	if err := json.Unmarshal(winEnv.Payload, &winResult); err != nil {
		t.Fatalf("unmarshal GuessResult: %v", err)
	}
	if winResult.Status != "WON" {
		t.Fatalf("status after correct guess = %q, want WON", winResult.Status)
	}
	for _, f := range winResult.Feedback {
		if f.State != "CORRECT" {
			t.Fatalf("winning guess feedback has non-CORRECT letter: %+v", winResult.Feedback)
		}
	}

	// The session is over: one more guess should be rejected.
	sendEnvelope(t, ctx, player, schema.TypeGuess, schema.GuessRequest{SessionID: started.SessionID, Guess: target})
	afterWinEnv := readEnvelope(t, ctx, player)
	if afterWinEnv.Type != schema.TypeError {
		t.Fatalf("guessing after a win: got envelope type %q, want %q", afterWinEnv.Type, schema.TypeError)
	}
}

func TestWordGameLossAfterMaxGuesses(t *testing.T) {
	const broker = "localhost:9092"
	tcpConn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s (is `docker compose up` running in deploy/compose?): %v", broker, err)
	}
	tcpConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{broker}, nil)
	if err := gw.EnsureTopic(ctx, "game-sessions", GameSessionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	player := dial(t, srv)
	defer player.CloseNow()

	sendEnvelope(t, ctx, player, schema.TypeStartGame, schema.StartGameRequest{PlayerID: "wordy"})
	var started schema.GameStarted
	if err := json.Unmarshal(readEnvelope(t, ctx, player).Payload, &started); err != nil {
		t.Fatalf("unmarshal GameStarted: %v", err)
	}
	sess, ok := gw.sessions.get(started.SessionID)
	if !ok {
		t.Fatalf("session %q not found in store", started.SessionID)
	}
	target := sess.Target

	var lastResult schema.GuessResult
	for i := 0; i < wordgame.MaxGuesses; i++ {
		wrong := differentAnswer(t, target)
		sendEnvelope(t, ctx, player, schema.TypeGuess, schema.GuessRequest{SessionID: started.SessionID, Guess: wrong})
		env := readEnvelope(t, ctx, player)
		if err := json.Unmarshal(env.Payload, &lastResult); err != nil {
			t.Fatalf("unmarshal GuessResult (attempt %d): %v", i, err)
		}
	}

	if lastResult.Status != "LOST" {
		t.Fatalf("status after %d wrong guesses = %q, want LOST", wordgame.MaxGuesses, lastResult.Status)
	}
	if lastResult.GuessesRemaining != 0 {
		t.Fatalf("guesses remaining after loss = %d, want 0", lastResult.GuessesRemaining)
	}
}

func TestWordGameRejectsBadGuesses(t *testing.T) {
	const broker = "localhost:9092"
	tcpConn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s (is `docker compose up` running in deploy/compose?): %v", broker, err)
	}
	tcpConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{broker}, nil)
	if err := gw.EnsureTopic(ctx, "game-sessions", GameSessionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	player := dial(t, srv)
	defer player.CloseNow()

	sendEnvelope(t, ctx, player, schema.TypeStartGame, schema.StartGameRequest{PlayerID: "wordy"})
	var started schema.GameStarted
	if err := json.Unmarshal(readEnvelope(t, ctx, player).Payload, &started); err != nil {
		t.Fatalf("unmarshal GameStarted: %v", err)
	}

	sendEnvelope(t, ctx, player, schema.TypeGuess, schema.GuessRequest{SessionID: started.SessionID, Guess: "hi"})
	if env := readEnvelope(t, ctx, player); env.Type != schema.TypeError {
		t.Fatalf("too-short guess: got envelope type %q, want %q", env.Type, schema.TypeError)
	}

	sendEnvelope(t, ctx, player, schema.TypeGuess, schema.GuessRequest{SessionID: started.SessionID, Guess: "zzzzz"})
	if env := readEnvelope(t, ctx, player); env.Type != schema.TypeError {
		t.Fatalf("not-a-word guess: got envelope type %q, want %q", env.Type, schema.TypeError)
	}

	sendEnvelope(t, ctx, player, schema.TypeGuess, schema.GuessRequest{SessionID: "not-a-real-session", Guess: "crane"})
	if env := readEnvelope(t, ctx, player); env.Type != schema.TypeError {
		t.Fatalf("unknown session: got envelope type %q, want %q", env.Type, schema.TypeError)
	}
}
