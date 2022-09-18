package rabbitmq

import (
	"context"
	"crypto/sha1"
	"flag"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"log"
	"os"
	"time"
)

var url = flag.String("url", "amqp:///", "AMQP url for both the publisher and subscriber")

// exchange binds the publishers to the subscribers
const exchange = "pubsub"

type ExchangeConf struct {
}

func tmp(ses Session) {
}

type SetUp func()

// redial continually connects to the URL, exiting the program when no longer possible
func redial(ctx context.Context, factory func() any) chan chan any {
	sessions := make(chan chan any) // 双chan模式

	go func() {
		sessCh := make(chan any)
		defer close(sessions)

		for {
			select {
			case sessions <- sessCh: // 通过外部chan，接收者让生产者开始生产
			case <-ctx.Done():
				log.Println("shutting down Session factory")
				return
			}

			msg := factory()

			select {
			case sessCh <- msg: // 通过内部chan,接受者在接收完成的消息
			case <-ctx.Done():
				log.Println("shutting down new Session")
				return
			}
		}
	}()

	return sessions
}

func do() {

	ctx, done := context.WithCancel(context.Background())
	go func() {
		sessions := redial(ctx, createSession)
		for session := range sessions {
			obj := <-session
			publisher(obj)
		}
		done()
	}()
}

// publish publishes messages to a reconnecting Session to a fanout exchange.
// It receives from the application specific source of messages.
func publish(sessions chan chan any, messages <-chan Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		reading = messages
		pending = make(chan Message, 1)
	)

	for session := range sessions {
		obj := <-session

		var (
			running bool
			confirm = make(chan amqp.Confirmation, 1)
		)

		pub := obj.(Session)

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
			var body Message
			select {
			case confirmed, ok := <-confirm:
				if !ok {
					break Publish
				}
				if !confirmed.Ack {
					log.Printf("nack Message %d, body: %q", confirmed.DeliveryTag, string(body))
				}
				reading = messages

			case body = <-pending:
				routingKey := "ignored for fanout exchanges, application dependent for other exchanges"
				err := pub.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
					Body: body,
				})
				// Retry failed delivery on the next Session
				if err != nil {
					pending <- body
					pub.Close()
					break Publish
				}

			case body, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- body
				reading = nil
			}
		}
	}
}

// identity returns the same host/process unique string for the lifetime of
// this process so that subscriber reconnections reuse the same queue name.
func identity() string {
	hostname, err := os.Hostname()
	h := sha1.New()
	fmt.Fprint(h, hostname)
	fmt.Fprint(h, err)
	fmt.Fprint(h, os.Getpid())
	return fmt.Sprintf("%x", h.Sum(nil))
}

// subscribe consumes deliveries from an exclusive queue from a fanout exchange and sends to the application specific messages chan.
func subscribe(sessions chan chan Session, messages chan<- Message) {
	queue := identity()

	for session := range sessions {
		sub := <-session

		if _, err := sub.QueueDeclare(queue, false, true, true, false, nil); err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
			return
		}

		routingKey := "application specific routing key for fancy topologies"
		if err := sub.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", exchange, err)
			return
		}

		deliveries, err := sub.Consume(queue, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queue, err)
			return
		}

		log.Printf("subscribed...")

		for msg := range deliveries {
			messages <- msg.Body
			sub.Ack(msg.DeliveryTag, false)
		}
	}
}

// write is this application's subscriber of application messages, printing to
// stdout.
func write(w io.Writer) chan<- Message {
	lines := make(chan Message)
	go func() {
		for line := range lines {
			fmt.Fprintln(w, string(line))
		}
	}()
	return lines
}

func main() {
	flag.Parse()

	ctx, done := context.WithCancel(context.Background())

	go func() {
		publish(redial(ctx, createSession), read(os.Stdin))
		done()
	}()

	go func() {
		subscribe(redial(ctx, *url), write(os.Stdout))
		done()
	}()

	<-ctx.Done()
}
