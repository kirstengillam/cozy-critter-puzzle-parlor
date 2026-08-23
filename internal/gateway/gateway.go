// Package gateway implements the Milestone 1 loopback: a WebSocket
// connection whose inbound messages are produced to Kafka and whose
// outbound messages are whatever comes back out of that same topic.
package gateway

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/segmentio/kafka-go"
)

type Gateway struct {
	brokers        []string
	topic          string
	allowedOrigins []string
}

// New creates a Gateway. allowedOrigins lists origin patterns (per
// coder/websocket's AcceptOptions.OriginPatterns) permitted to open a
// WebSocket connection from a browser; a nil/empty slice accepts same-origin
// requests only.
func New(brokers []string, topic string, allowedOrigins []string) *Gateway {
	return &Gateway{brokers: brokers, topic: topic, allowedOrigins: allowedOrigins}
}

// EnsureTopic creates the gateway's topic (single partition, no
// replication) if it doesn't already exist. Call once at startup.
func (g *Gateway) EnsureTopic(ctx context.Context) error {
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
		Topic:             g.topic,
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

	writer := &kafka.Writer{
		Addr:     kafka.TCP(g.brokers...),
		Topic:    g.topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	// Capture the partition's current end before we let the client send
	// anything, so the reader below is positioned to catch every message
	// this connection produces — no race against a consumer-group join.
	leader, err := kafka.DialLeader(ctx, "tcp", g.brokers[0], g.topic, 0)
	if err != nil {
		log.Printf("gateway: dial leader: %v", err)
		return
	}
	startOffset, err := leader.ReadLastOffset()
	leader.Close()
	if err != nil {
		log.Printf("gateway: read last offset: %v", err)
		return
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   g.brokers,
		Topic:     g.topic,
		Partition: 0,
	})
	if err := reader.SetOffset(startOffset); err != nil {
		log.Printf("gateway: set offset: %v", err)
		return
	}

	done := make(chan struct{})

	// Kafka -> WebSocket: whatever comes back out of the topic goes to the client.
	go func() {
		defer close(done)
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				return
			}
			if err := conn.Write(ctx, websocket.MessageText, msg.Value); err != nil {
				return
			}
		}
	}()

	// WebSocket -> Kafka: every inbound message is produced to the topic.
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = writer.WriteMessages(writeCtx, kafka.Message{Value: data})
		cancel()
		if err != nil {
			log.Printf("gateway: produce: %v", err)
			break
		}
	}

	// Unblocks the Kafka->WebSocket goroutine's pending ReadMessage call.
	reader.Close()
	<-done
}
