package logger

import (
	"encoding/json"
	"fmt"
	// "github.com/gin-gonic/gin"
	"github.com/robertkowalski/graylog-golang"
	"gofiverr/errors"
	"log"
	// "net/http"
	"os"
	"time"
)

var (
	Graylog *gelf.Gelf
)

type logEntry struct {
	Host         string `json:"host"`
	Version      string `json:"version"`
	Timestamp    int64  `json:"timestamp"`
	Facility     string `json:"facility"`
	ShortMessage string `json:"short_message"`
	LogLevel     int    `json:"log_level"`
	FullMessage  string `json:"full_message"`
}

func InitLogger(host string, port int) {
	Graylog = gelf.New(gelf.Config{
		GraylogHostname: host,
		GraylogPort:     port,
	})
}

func ErrorLog(err error) {
	Log(err, 3)
}

func FatalLog(err error) {
	Log(err, 4)
}

func Log(err error, level int) {
	fiverrr := err.(errors.FiverrError)
	hostname, _ := os.Hostname()
	logMessage := logEntry{
		Host:         hostname,
		Version:      "1.0",
		Timestamp:    time.Now().Unix(),
		Facility:     "Go Template",
		ShortMessage: fiverrr.GetMessage(),
		LogLevel:     level,
		FullMessage:  fiverrr.Error(),
	}

	log.Println(logMessage)
	fmt.Println(logMessage)
	jsonByteMessage, _ := json.Marshal(logMessage)
	jsonMessage := string(jsonByteMessage)
	Graylog.Log(jsonMessage)
}

// RecoverAndLog catches an error, log it and recover. Return a gin.HandlerFunc
// func RecoverAndLog() gin.HandlerFunc {

// 	return func(c *gin.Context) {
// 		defer func() {
// 			if err := recover(); err != nil {
// 				Log(errors.New(err.(string)), 4)
// 				c.Writer.WriteHeader(http.StatusInternalServerError)
// 			}
// 		}()
// 		c.Next()
// 		if len(c.Errors) > 0 {
// 			for _, error := range c.Errors {
// 				Log(errors.New(error.Err), 3)
// 			}
// 		}
// 	}
// }

type WrappedFn func() error
type Options map[string]interface{}

func RecoverAndLogWrapper(o Options, worker WrappedFn) (fn WrappedFn) {
	fn = func() (err error) {

		defer func() error {
			if r := recover(); r != nil {
				m_err := r.(string)
				fmt.Println("Recovering from panic")
				err = errors.New(m_err)
				FatalLog(err)
				return err
			}
			return nil
		}()

		if err = worker(); err != nil {
			m_err := errors.Wrap(err, err.Error())
			fmt.Println(m_err)
			ErrorLog(m_err)
		}

		return err
	}
	return fn
}
