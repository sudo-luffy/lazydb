package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func parseRESP(reader *bufio.Reader) ([]string, error) {
	// parse string
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("invalid array prefix")
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		fmt.Println("Invalid array size")
		return nil, err
	}
	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		line, err := reader.ReadString('\n')
		if err != nil || line[0] != '$' {
			fmt.Println("Expected bulk string header")
			return nil, err
		}
		length, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		data := make([]byte, length+2)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, err
		}
		result = append(result, string(data[:length]))
	}
	return result, nil
}

func handleConnection(conn net.Conn, db *DB) {
	// handle connection logic
	defer conn.Close()
	reader := bufio.NewReader(conn)
	// handle buffers
	for {
		cmd, err := parseRESP(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
			} else {
				fmt.Println("Parsing error:", err)
			}
			return
		}

		// process cmd
		if len(cmd) > 0 {
			response := db.handleCommand(cmd)
			conn.Write([]byte(response))
		}
	}

}

func main() {
	// start with the listner
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind port on 6379")
		os.Exit(1)
	}
	fmt.Println("Listnening on PORT 6379")
	db := NewDB()
	// start listening for connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("New Connection Established ...")
		go handleConnection(conn, db)
	}
}
