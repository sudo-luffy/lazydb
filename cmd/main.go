package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"

	"github.com/sudo-luffy/lazydb/internal/database"
	"github.com/sudo-luffy/lazydb/internal/server"
)

func isPointer(v interface{}) bool {
	val := reflect.ValueOf(v)
	return val.Kind() == reflect.Ptr
}

func main() {
	var port int
	var replica string
	flag.IntVar(&port, "port", 6379, "Port to listen on")
	// Expected format for replicaof: "host port" e.g., "localhost 6379"
	flag.StringVar(&replica, "replicaof", "", "Master host and port for replica mode (e.g., 'localhost 6379')")
	flag.Parse()

	database := database.NewDB()

	srv, err := server.NewServer(port, replica, database)
	if err != nil {
		fmt.Printf("Failed to create server: %v\n", err)
		os.Exit(1)
	}
	defer srv.Close()

	srv.Start()
}
