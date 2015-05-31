package worker

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"go_live/syslib"
	"testing"
	"time"
	"github.com/roeepolegfiverr/gofiverr/connectors"
	"github.com/adjust/goenv"
)

func init() {
	connectors.InitConnectors(goenv.DefaultGoenv(), true)
}

func publishMessages(n int) {
	conn, err := amqp.Dial(goenv.DefaultGoenv().GetNamedAmqp("rabbit"))
	if err != nil {
		fmt.Printf("error connecting rabbit %s", err)
	}
	defer conn.Close()

	var channel *amqp.Channel
	channel, err = conn.Channel()
	defer channel.Close()

	if err != nil {
		fmt.Printf("error connecting channel rabbit %s", err)
	}

	message := map[string]interface{}{
		"event":      "test",
		"booltest":   true,
		"inttest":    1,
		"stringtest": "hello world",
	}

	j, _ := json.Marshal(message)

	for i := 0; i < n; i++ {
		channel.Publish(
			"fiverr.topic",                 //exchange
			"fiverr.events.#.gig_worker.#", //routingKey
			false,
			false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "UTF-8",
				Body:            j,
				DeliveryMode:    amqp.Transient,
				Priority:        0,
			},
		)
	}
}

func TestStress(t *testing.T) {
	n := 10
	publishMessages(n)
	listener := make(chan bool)
	start := time.Now()
	queueName := syslib.Config.Get("worker_queue", "")
	routingKey := syslib.Config.Get("routing_key", "")
	workerName := syslib.Config.Get("worker_name", "")
	go Consume(queueName, workerName, routingKey,1, listener)

	for i := 0; i < n; i++ {
		fmt.Printf("Got new message %d with %v\n", i, <-listener)
		<-listener
	}
	t.Logf("this test took :%s", time.Since(start))
	if 1 != 1 {
		t.Error("failed")
	}

}
