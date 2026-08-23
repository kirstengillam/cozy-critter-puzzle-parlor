package gateway

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestEchoRoundTrip(t *testing.T) {
	const broker = "localhost:9092"

	conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s (is `docker compose up` running in deploy/compose?): %v", broker, err)
	}
	conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	gw := New([]string{broker}, "echo-test")
	if err := gw.EnsureTopic(ctx); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	client, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.CloseNow()

	want := "hello through kafka"
	if err := client.Write(ctx, websocket.MessageText, []byte(want)); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, got, err := client.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	client.Close(websocket.StatusNormalClosure, "")
}
