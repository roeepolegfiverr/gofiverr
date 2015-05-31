// Package worker handles connecting to a queue and pulling messages.
package worker

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"go_live/shared/connectors"
	"go_live/shared/errors"
	"go_live/shared/logger"
	"go_live/shared/statsd"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

type WorkerTask func(*Event) (err error)
type WorkerTasks struct {
	Tasks map[string]WorkerTask
}
type Event struct {
	Params          map[string]interface{}
	Valid           bool
	Name            string
	OriginalMessage amqp.Delivery
}
type hash map[string]interface{}

var (
	Tasks      = &WorkerTasks{Tasks: map[string]WorkerTask{}}
	routingKey string
	workerName string
)

func (tasks *WorkerTasks) AddTask(key string, task WorkerTask) bool {
	if tasks == nil {
		tasks = &WorkerTasks{Tasks: map[string]WorkerTask{}}
	}
	if tasks.Tasks == nil {
		tasks.Tasks = map[string]WorkerTask{}
	}
	if _, ok := tasks.Tasks[key]; !ok {
		tasks.Tasks[key] = task
		return true
	}
	return false
}

func (tasks *WorkerTasks) RemoveTask(key string) bool {
	if _, ok := tasks.Tasks[key]; ok {
		delete(tasks.Tasks, key)
		return true
	}
	return false
}

func (event *Event) GetString(key string) (string, error) {
	if val, ok := event.Params[key]; ok {
		switch vv := val.(type) {
		case string:
			return vv, nil
		default:
			return "", errors.Newf("%s is not of type string in message %s", val, event.Params)
		}
	}
	return "", errors.Newf("Couldn't find value for key %s in message %s", key, event.Params)
}

func (event *Event) GetInt(key string) (int, error) {
	if val, ok := event.Params[key]; ok {
		switch vv := val.(type) {
		case float64:
			return int(vv), nil
		case int:
			return vv, nil
		default:
			return 0, errors.Newf("%s is not of type int in message %s", val, event.Params)
		}
	}
	return 0, errors.Newf("Couldn't find value for key %s in message %s", key, event.Params)
}

func (event *Event) GetFloat(key string) (float64, error) {
	if val, ok := event.Params[key]; ok {
		switch vv := val.(type) {
		case float64:
			return vv, nil
		case float32:
			return float64(vv), nil
		default:
			return 0, errors.Newf("%s is not of type float in message %s", val, event.Params)
		}
	}
	return 0, errors.Newf("Couldn't find value for key %s in message %s", key, event.Params)
}

func (event *Event) GetBool(key string) (bool, error) {
	if val, ok := event.Params[key]; ok {
		switch vv := val.(type) {
		case bool:
			return vv, nil
		default:
			return false, errors.Newf("%s is not of type bool in message %s", val, event.Params)
		}
	}
	return false, errors.Newf("Couldn't find value for key %s in message %s", key, event.Params)
}

// Consume is the main worker method. It connect to a given queue and infinitely reads and handle messages.
func Consume(queueName string, pworkerName string, proutingKey string, workersInPool int, listener chan *Event) {
	routingKey = proutingKey
	workerName = pworkerName
	if queueName == "" {
		logger.ErrorLog(errors.New("Worker queue name is empty"))
		return
	}
	if len(Tasks.Tasks) == 0 {
		logger.ErrorLog(errors.New("Worker Tasks are empty, nothing to work on"))
		return
	}
	// TODO - need to be tested
	//infinite loop for reconnecting after channel closed or some other failure
	for {
		var channel *amqp.Channel
		channel = connectors.Clients.Rabbit()
		host, err := os.Hostname()
		consumerName := fmt.Sprintf("%s-%s-go-consumer", host, queueName)
		messages, err := channel.Consume(queueName, consumerName, false, false, false, false, nil)

		if err != nil {
			fmt.Printf("Error consuming rabbit %s", err)
			logger.ErrorLog(errors.Wrap(err, err.Error()))
			return
		}
		jobs := make(chan *Event)
		for w := 0; w < workersInPool; w++ {
			go worker(w, jobs)
		}
		fmt.Printf("Worker starting on queue %s with %d minions\nWaiting for some messages to work on\n", queueName, workersInPool)
		for message := range messages {
			//fmt.Printf("got message with body:%s \n", message.Body)
			eventMessage, err := parseMessage(message)
			if err != nil {
				logger.ErrorLog(errors.Wrap(err, err.Error()))
			}
			if listener != nil {
				listener <- eventMessage
			}

			jobs <- eventMessage
		}
		logger.ErrorLog(errors.New("Consuming failed, trying to reconnect"))
	}
}

func worker(id int, jobs <-chan *Event) {
	for job := range jobs {
		//fmt.Printf("%d minion got some work\n", id)
		//wrap the main process function so I can pass it to the wrappers
		fn := func() error {
			return process(job)
		}
		// invoke the process method with middlewares wrappers.
		ackMessage(statsd.StatsDWrapper(job.Name, logger.RecoverAndLogWrapper(fn)), job.OriginalMessage)
	}
}

func process(event *Event) error {

	if !event.Valid {
		return errors.New("Event is not valid")
	}

	if event.Name == "" {
		return errors.New("Event name is empty")
	}
	//check if there is valid task for the event
	if task, found := Tasks.Tasks[event.Name]; found {
		return task(event)
	}
	return errors.Newf("Couldn't find task for event %+v", event)

}

func ackMessage(fn func() error, message amqp.Delivery) {
	defer message.Ack(false)
	if err := fn(); err != nil {
		fmt.Println("Got an error, falling back")
		sendToFailedQueue(message, err)
	}
}

func parseMessage(message amqp.Delivery) (*Event, error) {
	var params map[string]interface{}
	err := json.Unmarshal(message.Body, &params)
	if err != nil {
		logger.ErrorLog(errors.Wrap(err, err.Error()))
	}

	var eventName string
	if val, ok := params["event"]; ok {
		eventName = val.(string)
	}

	if eventName == "" {
		err = errors.New("Event name is empty")
	}

	event := &Event{
		Params:          params,
		Valid:           err == nil && eventName != "",
		Name:            eventName,
		OriginalMessage: message,
	}
	return event, err
}

func sendToFailedQueue(message amqp.Delivery, err error) {
	m_err := err.(errors.FiverrError)
	eventMessage, _ := parseMessage(message)
	jsonMessage := sanitizeMessage(eventMessage.Params)
	session, err := connectors.Clients.NamedMongo("failed_queue")
	if err != nil {
		logger.ErrorLog(errors.Wrap(err, err.Error()))
		return
	}
	defer session.Close()
	doc := hash{
		"message":         jsonMessage,
		"routing_key":     routingKey,
		"error_message":   m_err.GetMessage(),
		"error_backtrace": m_err.Error(),
		"created_at":      time.Now(),
	}
	collection := session.DB("").C(
		fmt.Sprintf("%s_failed_queue", workerName))
	collection.Insert(doc)

}

func sanitizeMessage(message hash) hash {
	fixedMessage := hash{}
	for k, v := range message {
		k = fixMongoKeys(k)
		if val, ok := v.(map[string]interface{}); ok {
			fixedMessage[k] = sanitizeMessage(val)
		} else if val, ok := v.(string); ok {
			if !utf8.ValidString(val) {
				v = sanitizeString(val)
			}
		}
		fixedMessage[k] = v
	}
	return fixedMessage
}

func fixMongoKeys(key string) string {
	return strings.Replace(key, ".", "_", -1)
}

func sanitizeString(s string) interface{} {
	v := make([]rune, 0, len(s))
	for i, r := range s {
		if r == utf8.RuneError {
			_, size := utf8.DecodeRuneInString(s[i:])
			if size == 1 {
				continue
			}
		}
		v = append(v, r)
	}
	s = string(v)
	return s
}
