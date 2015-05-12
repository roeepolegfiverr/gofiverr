package shared

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/peterbourgon/g2s"
	"go_live/shared/errors"
	"os"
	"time"
)

const (
	sampleRate = 1.0
)

type statsDClient struct {
	Client       g2s.Statter
	Prefix       string
	GlobalPrefix string
}

var (
	stats *statsDClient
)

func InitStatsD() {
	host := Config.Get("statsd.host", "localhost")
	port := Config.GetInt("statsd.port", 8125)
	prefix := Config.Get("statsd.prefix", "my_service")

	stats = initClient(fmt.Sprintf("%s:%d", host, port), prefix)
}

// StatsDMiddleware is the middleware handler that sends the status code and response time to StatsD server
func StatsDMiddleware() gin.HandlerFunc {

	return func(c *gin.Context) {
		startTime := time.Now()
		c.Next()
		responseTime := time.Since(startTime)
		status_code := string(c.Writer.Status())

		//Send metrics
		go sendMetrics(status_code, "", responseTime)

	}
}

// StatsDWrapper wraps the worker work to it can measure its times and other stats.
// it will return a func so we can continue and wrap it with other wrappers, such as logger.
func StatsDWrapper(o options, worker wrappedFn) wrappedFn {
	return func() error {
		start := time.Now()
		err := worker()
		end := time.Since(start)
		//Send metrics
		logWork(end, err, o)
		fmt.Printf("Time: %.5f s with status %v\n", end.Seconds(), err == nil)
		return err
	}
}

func logWork(elapsed time.Duration, err error, o options) {
	status := "success"
	if err != nil {
		status = "failed"
	}

	if val, ok := o["event"]; ok {
		event := val.(string)
		go sendMetrics(status, event, elapsed)
	}
}

func sendMetrics(status string, event string, elapsed time.Duration) {
	incrementWorkCounters(status, event)
	timeWorkTimers(elapsed, event)
}

func incrementWorkCounters(status string, event string) {
	stats.Client.Counter(sampleRate, fmt.Sprintf("%s.status_code.%s", stats.Prefix, status), 1)
	stats.Client.Counter(sampleRate, fmt.Sprintf("%s.status_code.%s", stats.GlobalPrefix, status), 1)
	stats.Client.Counter(sampleRate, fmt.Sprintf("%s.total_requests", stats.Prefix), 1)
	stats.Client.Counter(sampleRate, fmt.Sprintf("%s.total_requests", stats.GlobalPrefix), 1)
	if event != "" {
		stats.Client.Counter(sampleRate, fmt.Sprintf("%s.types.%s.total_requests", stats.GlobalPrefix, event), 1)
	}
}

func timeWorkTimers(elapsed time.Duration, event string) {
	stats.Client.Timing(sampleRate, fmt.Sprintf("%s.response_time", stats.Prefix), elapsed)
	stats.Client.Timing(sampleRate, fmt.Sprintf("%s.response_time", stats.GlobalPrefix), elapsed)
	if event != "" {
		stats.Client.Timing(sampleRate, fmt.Sprintf("%s.types.%s.response_time", stats.GlobalPrefix, event), elapsed)
	}
}

func initClient(server string, prefix string) *statsDClient {
	client, err := g2s.Dial("udp", server)
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		ErrorLog(errors.Wrap(err, err.Error()))
		return nil
	}

	return &statsDClient{
		Client:       client,
		GlobalPrefix: prefix,
		Prefix:       fmt.Sprintf("servers.%s.%s", prefix, hostname),
	}
}
