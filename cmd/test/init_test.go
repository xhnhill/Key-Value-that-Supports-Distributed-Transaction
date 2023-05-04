package test

import (
	"testing"
)

func TestInitFunc(t *testing.T) {
	var msgs []string
	for i := 0; i < 2; i++ {
		msgs = append(msgs, "hello")
	}

}
