package gateway

import (
	"context"
	"encoding/json"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"cozy-critter-puzzle-parlor/internal/connectionsgame"
	"cozy-critter-puzzle-parlor/internal/schema"
)

// dialKafka skips the test if Kafka isn't reachable, same convention as
// the word-game integration tests above.
func dialKafka(t *testing.T) {
	t.Helper()
	const broker = "localhost:9092"
	tcpConn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s (is `docker compose up` running in deploy/compose?): %v", broker, err)
	}
	tcpConn.Close()
}

// crossGroupGuess picks one word from each of the puzzle's 4 groups —
// guaranteed to overlap at most 1 word with any single group, so it's
// wrong (not even "one away") for every group.
func crossGroupGuess(p connectionsgame.Puzzle) []string {
	guess := make([]string, len(p.Answers))
	for i, g := range p.Answers {
		guess[i] = g.Members[0]
	}
	return guess
}

func TestConnectionsStartAndWin(t *testing.T) {
	dialKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{"localhost:9092"}, nil)
	if err := gw.EnsureTopic(ctx, "connections-sessions", ConnectionsSessionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	player := dial(t, srv)
	defer player.CloseNow()

	sendEnvelope(t, ctx, player, schema.TypeStartConnections, schema.StartConnectionsRequest{PlayerID: "connie"})
	startEnv := readEnvelope(t, ctx, player)
	if startEnv.Type != schema.TypeConnectionsStarted {
		t.Fatalf("got envelope type %q, want %q", startEnv.Type, schema.TypeConnectionsStarted)
	}
	var started schema.ConnectionsStarted
	if err := json.Unmarshal(startEnv.Payload, &started); err != nil {
		t.Fatalf("unmarshal ConnectionsStarted: %v", err)
	}
	if started.SessionID == "" {
		t.Fatal("ConnectionsStarted.SessionID is empty")
	}
	if len(started.Words) != connectionsgame.GroupSize*connectionsgame.GroupSize {
		t.Fatalf("got %d words, want %d", len(started.Words), connectionsgame.GroupSize*connectionsgame.GroupSize)
	}
	if started.MaxMistakes != connectionsgame.MaxMistakes {
		t.Fatalf("MaxMistakes = %d, want %d", started.MaxMistakes, connectionsgame.MaxMistakes)
	}

	// White-box peek at the session's puzzle — never revealed to a real
	// client, but the test needs it to guess correctly.
	sess, ok := gw.connectionsSessions.get(started.SessionID)
	if !ok {
		t.Fatalf("session %q not found in store", started.SessionID)
	}
	puzzle := sess.Puzzle

	// A wrong guess first, exercising MistakesUsed and IN_PROGRESS.
	sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
		SessionID: started.SessionID,
		Members:   crossGroupGuess(puzzle),
	})
	wrongEnv := readEnvelope(t, ctx, player)
	if wrongEnv.Type != schema.TypeConnectionsResult {
		t.Fatalf("got envelope type %q, want %q", wrongEnv.Type, schema.TypeConnectionsResult)
	}
	var wrongResult schema.ConnectionsResult
	if err := json.Unmarshal(wrongEnv.Payload, &wrongResult); err != nil {
		t.Fatalf("unmarshal ConnectionsResult: %v", err)
	}
	if wrongResult.Correct {
		t.Fatal("cross-group guess reported Correct = true")
	}
	if wrongResult.Status != "IN_PROGRESS" {
		t.Fatalf("status after wrong guess = %q, want IN_PROGRESS", wrongResult.Status)
	}
	if wrongResult.MistakesUsed != 1 {
		t.Fatalf("MistakesUsed = %d, want 1", wrongResult.MistakesUsed)
	}

	// Now solve all 4 groups correctly.
	var lastResult schema.ConnectionsResult
	for i, g := range puzzle.Answers {
		sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
			SessionID: started.SessionID,
			Members:   g.Members,
		})
		env := readEnvelope(t, ctx, player)
		if err := json.Unmarshal(env.Payload, &lastResult); err != nil {
			t.Fatalf("unmarshal ConnectionsResult (group %d): %v", i, err)
		}
		if !lastResult.Correct {
			t.Fatalf("group %d (%s): Correct = false, want true", i, g.Name)
		}
		if lastResult.SolvedGroup == nil || lastResult.SolvedGroup.Name != g.Name {
			t.Fatalf("group %d: SolvedGroup = %+v, want name %q", i, lastResult.SolvedGroup, g.Name)
		}
	}
	if lastResult.Status != "WON" {
		t.Fatalf("status after solving all 4 groups = %q, want WON", lastResult.Status)
	}
	if len(lastResult.SolvedGroups) != len(puzzle.Answers) {
		t.Fatalf("SolvedGroups has %d entries, want %d", len(lastResult.SolvedGroups), len(puzzle.Answers))
	}

	// The session is over: one more guess should be rejected.
	sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
		SessionID: started.SessionID,
		Members:   puzzle.Answers[0].Members,
	})
	if env := readEnvelope(t, ctx, player); env.Type != schema.TypeError {
		t.Fatalf("guessing after a win: got envelope type %q, want %q", env.Type, schema.TypeError)
	}
}

func TestConnectionsLossAfterMaxMistakes(t *testing.T) {
	dialKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{"localhost:9092"}, nil)
	if err := gw.EnsureTopic(ctx, "connections-sessions", ConnectionsSessionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	player := dial(t, srv)
	defer player.CloseNow()

	sendEnvelope(t, ctx, player, schema.TypeStartConnections, schema.StartConnectionsRequest{PlayerID: "connie"})
	var started schema.ConnectionsStarted
	if err := json.Unmarshal(readEnvelope(t, ctx, player).Payload, &started); err != nil {
		t.Fatalf("unmarshal ConnectionsStarted: %v", err)
	}
	sess, ok := gw.connectionsSessions.get(started.SessionID)
	if !ok {
		t.Fatalf("session %q not found in store", started.SessionID)
	}
	guess := crossGroupGuess(sess.Puzzle)

	var lastResult schema.ConnectionsResult
	for i := 0; i < connectionsgame.MaxMistakes; i++ {
		sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
			SessionID: started.SessionID,
			Members:   guess,
		})
		env := readEnvelope(t, ctx, player)
		if err := json.Unmarshal(env.Payload, &lastResult); err != nil {
			t.Fatalf("unmarshal ConnectionsResult (attempt %d): %v", i, err)
		}
	}

	if lastResult.Status != "LOST" {
		t.Fatalf("status after %d wrong guesses = %q, want LOST", connectionsgame.MaxMistakes, lastResult.Status)
	}
	if lastResult.MistakesUsed != connectionsgame.MaxMistakes {
		t.Fatalf("MistakesUsed = %d, want %d", lastResult.MistakesUsed, connectionsgame.MaxMistakes)
	}
}

func TestConnectionsRejectsBadGuesses(t *testing.T) {
	dialKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{"localhost:9092"}, nil)
	if err := gw.EnsureTopic(ctx, "connections-sessions", ConnectionsSessionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	player := dial(t, srv)
	defer player.CloseNow()

	sendEnvelope(t, ctx, player, schema.TypeStartConnections, schema.StartConnectionsRequest{PlayerID: "connie"})
	var started schema.ConnectionsStarted
	if err := json.Unmarshal(readEnvelope(t, ctx, player).Payload, &started); err != nil {
		t.Fatalf("unmarshal ConnectionsStarted: %v", err)
	}

	// Wrong count.
	sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
		SessionID: started.SessionID,
		Members:   []string{"ONE", "TWO"},
	})
	if env := readEnvelope(t, ctx, player); env.Type != schema.TypeError {
		t.Fatalf("wrong-count guess: got envelope type %q, want %q", env.Type, schema.TypeError)
	}

	// Words not in this puzzle.
	sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
		SessionID: started.SessionID,
		Members:   []string{"ZZZNOTAWORD1", "ZZZNOTAWORD2", "ZZZNOTAWORD3", "ZZZNOTAWORD4"},
	})
	if env := readEnvelope(t, ctx, player); env.Type != schema.TypeError {
		t.Fatalf("junk guess: got envelope type %q, want %q", env.Type, schema.TypeError)
	}

	// Unknown session.
	sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
		SessionID: "not-a-real-session",
		Members:   []string{"A", "B", "C", "D"},
	})
	if env := readEnvelope(t, ctx, player); env.Type != schema.TypeError {
		t.Fatalf("unknown session: got envelope type %q, want %q", env.Type, schema.TypeError)
	}
}

func TestConnectionsWinCreditsCurrency(t *testing.T) {
	dialKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	gw := New([]string{"localhost:9092"}, nil)
	if err := gw.EnsureTopic(ctx, "connections-sessions", ConnectionsSessionsPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	if err := gw.EnsureTopic(ctx, "economy-ledger", EconomyLedgerPartitions); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}
	if err := gw.StartEconomyLedger(ctx); err != nil {
		t.Fatalf("start economy ledger: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	player := dial(t, srv)
	defer player.CloseNow()

	sendEnvelope(t, ctx, player, schema.TypeStartConnections, schema.StartConnectionsRequest{PlayerID: "connie-econ"})
	var started schema.ConnectionsStarted
	if err := json.Unmarshal(readEnvelope(t, ctx, player).Payload, &started); err != nil {
		t.Fatalf("unmarshal ConnectionsStarted: %v", err)
	}
	sess, ok := gw.connectionsSessions.get(started.SessionID)
	if !ok {
		t.Fatalf("session %q not found in store", started.SessionID)
	}
	puzzle := sess.Puzzle

	// One mistake, then solve all 4 groups.
	sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
		SessionID: started.SessionID,
		Members:   crossGroupGuess(puzzle),
	})
	readEnvelope(t, ctx, player) // discard the wrong-guess result

	var winResult schema.ConnectionsResult
	for _, g := range puzzle.Answers {
		sendEnvelope(t, ctx, player, schema.TypeConnectionsGuess, schema.ConnectionsGuessRequest{
			SessionID: started.SessionID,
			Members:   g.Members,
		})
		if err := json.Unmarshal(readEnvelope(t, ctx, player).Payload, &winResult); err != nil {
			t.Fatalf("unmarshal ConnectionsResult: %v", err)
		}
	}
	if winResult.Status != "WON" {
		t.Fatalf("status = %q, want WON", winResult.Status)
	}

	// The balance update arrives asynchronously (produce -> economy
	// consumer -> push), so it's the next message.
	balEnv := readEnvelope(t, ctx, player)
	if balEnv.Type != schema.TypeBalanceUpdated {
		t.Fatalf("got envelope type %q, want %q", balEnv.Type, schema.TypeBalanceUpdated)
	}
	var bal schema.BalanceUpdated
	if err := json.Unmarshal(balEnv.Payload, &bal); err != nil {
		t.Fatalf("unmarshal BalanceUpdated: %v", err)
	}
	if bal.PlayerID != "connie-econ" {
		t.Fatalf("player_id = %q, want connie-econ", bal.PlayerID)
	}
	wantReward := connectionsgame.RewardForWin(1)
	if bal.Balance != wantReward {
		t.Fatalf("balance = %d, want %d (reward for winning with 1 mistake)", bal.Balance, wantReward)
	}
	if bal.CurrencyType != schema.CurrencyCritterCoins {
		t.Fatalf("currency_type = %q, want %q", bal.CurrencyType, schema.CurrencyCritterCoins)
	}
}
