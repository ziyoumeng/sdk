package rabbitmq

import (
	"bufio"
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"log"
	"time"
)

var defaultConf = PubSubConf{
	Url: "amqp:///",
	ExchangeDeclare: ExchangeDeclare{
		Exchange:   "pubsub",
		Type:       "fanout",
		Durable:    false,
		AutoDelete: false,
	},
}

type PubSub struct {
	PubSubConf
}

type PubSubConf struct {
	Url             string
	ExchangeDeclare ExchangeDeclare
	messages        chan Message
}

func main() {
}

// read is this application's translation to the Message format, scanning from
// stdin.
func read(r io.Reader) <-chan []byte {
	lines := make(chan []byte)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			lines <- scan.Bytes()
		}
	}()
	return lines
}

func NewPubSub(conf PubSubConf) *PubSub {
	pb := &PubSub{
		conf}
	ctx := context.Background()
	pb.runPublish(ctx)
	return pb
}

type ExchangeDeclare struct {
	Exchange   string
	Type       string
	Durable    bool
	AutoDelete bool
}

func (p PubSubConf) createSession() any {
	ses := newSession(p.Url)
	d := p.ExchangeDeclare
	if err := ses.Channel.ExchangeDeclare(d.Exchange, d.Type, d.Durable, d.AutoDelete, false, false, nil); err != nil {
		log.Fatalf("cannot declare fanout exchange: %v", err)
	}
	return ses
}

func (p PubSubConf) publishMessage() func(session any) {
	var (
		reading = p.messages
		pending = make(chan Message, 1)
	)

	return func(session any) {
		var (
			running bool
			confirm = make(chan amqp.Confirmation, 1)
		)

		pub := session.(Session)

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil {
			log.Printf("publisher confirms not supported")
			close(confirm) // confirms not supported, simulate by always nacking
		} else {
			pub.NotifyPublish(confirm)
		}

		log.Printf("publishing...")

	Publish:
		for {
			var msg Message
			select {
			case confirmed, ok := <-confirm:
				if !ok {
					break Publish
				}
				if !confirmed.Ack {
					log.Printf("nack Message %d, msg: %q", confirmed.DeliveryTag, string(msg.Body))
				}
				reading = p.messages
			case msg = <-pending:
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				err := pub.PublishWithContext(ctx, p.ExchangeDeclare.Exchange, msg.RoutingKey, false, false, amqp.Publishing{
					Body: msg.Body,
				})
				// Retry failed delivery on the next Session
				if err != nil {
					pending <- msg
					pub.Close()
					break Publish
				}

			case msg, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- msg
				reading = nil
			}
		}
	}
}

func (p PubSubConf) Publish(msg Message) {
	go func() {
		p.messages <- msg
	}()
}

func (p *PubSub) runPublish(cancelCtx context.Context) {
	ctx, done := context.WithCancel(cancelCtx)
	go func() {
		sessions := redial(ctx, p.createSession)
		for session := range sessions {
			p.publishMessage()(<-session)
		}
		done()
	}()
}

type Message struct {
	RoutingKey string
	Body       []byte
}

type Session struct {
	*amqp.Connection
	*amqp.Channel
}

func newSession(url string) Session {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("cannot (re)dial: %v: %q", err, url)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("cannot create channel: %v", err)
	}
	return Session{conn, ch}
}

func (s Session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}
