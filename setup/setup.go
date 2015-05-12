package shared

import (
	"flag"
	"fmt"
	"github.com/adjust/goenv"
	"os"
	"os/signal"
	"syscall"
)

var (
	Config *goenv.Goenv
)

func init() {
	Config = goenv.DefaultGoenv()
	InitLogger()

	//
	rabbitPtr := flag.Bool("rabbit", false, "Should rabbitMQ should be init")
	flag.Parse()

	// Extract the app main file (service/worker) as parameter for the init connectors.
	InitConnectors(*rabbitPtr)
	InitStatsD()
}

func ProperShutdown() {
	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Holding here until getting os interrupt...")
	fmt.Printf("Got %s, Gracfully shutting down!\n", <-ch)

	Clients.ProperShutdown()
}
