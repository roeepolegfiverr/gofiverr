package connectors

import (
	"github.com/adjust/goenv"
	"testing"
)

//you might want to use - https://github.com/stretchr/testify for better assertion and mocking

func TestSimpleStuff(t *testing.T) {
	if 1 != 2 {
		t.Error("Got wrong number")
	}
}

func TestRedisPing(t *testing.T) {
	InitConnectors(goenv.DefaultGoenv(), true)
	client, _ := Clients.Redis()
	reply, _ := client.Cmd("ping").Bool()

	if reply != true {
		t.Errorf("Expected true got %v", reply)
	}
}
