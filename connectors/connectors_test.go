package shared

import (
	"testing"
)

//you might want to use - https://github.com/stretchr/testify for better assertion and mocking

func TestSimpleStuff(t *testing.T) {
	if 1 != 2 {
		t.Error("Got wrong number")
	}
}

func TestRedisPing(t *testing.T) {
	InitConnectors(true)
	client, _ := Clients.Redis()
	reply, _ := client.Cmd("ping").Bool()

	if reply != true {
		t.Errorf("Expected true got %v", reply)
	}
}
