package rabbitmq

import (
	"os"
	"testing"
)

func TestNewPubSub(t *testing.T) {
	pb := NewPubSub(defaultConf)
	for bytes := range read(os.Stdin) {
		msg := Message{
			RoutingKey: "test fanout",
			Body:       bytes,
		}
		pb.Publish(msg)
	}
}
