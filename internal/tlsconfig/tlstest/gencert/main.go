// Command gencert writes a throwaway CA and a localhost certificate for
// end-to-end TLS checks: DIR/test-ca.pem, DIR/server.pem, DIR/server-key.pem.
//
//	go run ./internal/tlsconfig/tlstest/gencert DIR
package main

import (
	"fmt"
	"os"

	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig/tlstest"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gencert DIR")
		os.Exit(2)
	}
	dir := os.Args[1]
	ca, err := tlstest.NewCA(dir, "test")
	if err == nil {
		_, err = ca.Issue(dir, "server")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gencert:", err)
		os.Exit(1)
	}
}
