package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/sudo-luffy/lazydb/internal/database"
	"github.com/sudo-luffy/lazydb/internal/handler"
	"github.com/sudo-luffy/lazydb/internal/protocol"
)

type Server struct {
	listener net.Listener
	cache    database.Storage
	port     int
	slaves   map[string]net.Conn
}

func NewServer(port int, replicaOf string, cache database.Storage) (*Server, error) {
	address := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to bind port %d: %w", port, err)
	}
	srv := &Server{
		listener: listener,
		cache:    cache,
		port:     port,
		slaves:   make(map[string]net.Conn),
	}
	if len(replicaOf) > 0 {
		cache.SetConfig("role", "slave")
		fmt.Printf("Running as replica of: %s\n", replicaOf)
		go srv.handleHandshake(replicaOf)
	}
	return srv, nil
}

func (s *Server) RegisterSlave(addr string, conn net.Conn) {
	s.slaves[addr] = conn
}

func (s *Server) Replicate(cmd []string) {
	data := protocol.EncodeArray(cmd)
	for addr, conn := range s.slaves {
		if _, err := conn.Write(data); err != nil {
			fmt.Printf("Replication to %s failed: %v. Removing from list.\n", addr, err)
			conn.Close()
			delete(s.slaves, addr)
		}
	}
}

func (s *Server) Start() {
	fmt.Printf("Listening on %s\n", s.listener.Addr().String())
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Close() error {
	fmt.Println("Server shutting down...")
	return s.listener.Close()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		fmt.Println("Client disconnected:", conn.RemoteAddr())
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	cmdHandler := handler.NewCommandHandler(s.cache, s, conn)
	for {
		cmdArgs, err := protocol.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				return
			} else {
				fmt.Printf("Parsing error from %s: %v\n", conn.RemoteAddr(), err)
				conn.Write([]byte(protocol.EncodeError(fmt.Sprintf("protocol error: %v", err))))
				return
			}
		}
		_ = cmdHandler.Handle(cmdArgs)
	}
}

func (s *Server) handleHandshake(replicaAddress string) {
	parts := strings.Split(replicaAddress, " ")
	if len(parts) != 2 {
		fmt.Printf("Invalid replicaof format: %s. Expected 'host port'\n", replicaAddress)
		return
	}
	masterHost, masterPort := parts[0], parts[1]
	address := net.JoinHostPort(masterHost, masterPort)

	fmt.Printf("Attempting to connect to master at %s for handshake...\n", address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Failed to connect to master for handshake: %v\n", err)
		return
	}
	fmt.Println("Connected to master for handshake.")

	reader := bufio.NewReader(conn)

	fmt.Println("Sending PING...")
	_, err = conn.Write(protocol.EncodeArray([]string{"PING"}))
	if err != nil {
		fmt.Printf("Failed to send PING to master: %v\n", err)
		return
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Failed to read PING response from master: %v\n", err)
		return
	}
	if strings.TrimSpace(line) != "+PONG" {
		fmt.Printf("Unexpected PING response: %s\n", strings.TrimSpace(line))
		return
	}
	fmt.Println("Received PONG from master.")

	replconfPortCmd := protocol.EncodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(s.port)})
	_, err = conn.Write(replconfPortCmd)
	if err != nil {
		fmt.Printf("Failed to send REPLCONF listening-port to master: %v\n", err)
		return
	}
	line, err = reader.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "+OK" {
		fmt.Printf("Unexpected REPLCONF listening-port response: %s (err: %v)\n", strings.TrimSpace(line), err)
		return
	}
	fmt.Println("Received OK for REPLCONF listening-port.")

	replconfCapaCmd := protocol.EncodeArray([]string{"REPLCONF", "capa", "psync2"})
	_, err = conn.Write(replconfCapaCmd)
	if err != nil {
		fmt.Printf("Failed to send REPLCONF capa to master: %v\n", err)
		return
	}
	line, err = reader.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "+OK" {
		fmt.Printf("Unexpected REPLCONF capa response: %s (err: %v)\n", strings.TrimSpace(line), err)
		return
	}
	fmt.Println("Received OK for REPLCONF capa psync2.")

	psyncCmd := protocol.EncodeArray([]string{"PSYNC", "?", "-1"})
	_, err = conn.Write(psyncCmd)
	if err != nil {
		fmt.Printf("Failed to send PSYNC to master: %v\n", err)
		return
	}

	fullResyncLine, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Failed to read FULLRESYNC response: %v\n", err)
		return
	}
	if !strings.HasPrefix(fullResyncLine, "+FULLRESYNC") {
		fmt.Printf("Expected +FULLRESYNC, got: %s\n", strings.TrimSpace(fullResyncLine))
		return
	}
	fmt.Printf("Received FULLRESYNC response: %s\n", strings.TrimSpace(fullResyncLine))

	parts = strings.Split(strings.TrimSpace(fullResyncLine), " ")
	if len(parts) >= 3 {
		s.cache.SetConfig("master_replid", parts[1])
		s.cache.SetConfig("master_repl_offset", parts[2])
		fmt.Printf("Updated slave config: replid=%s, offset=%s\n", parts[1], parts[2])
	}

	rdbHeaderLine, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Failed to read RDB bulk string header: %v\n", err)
		return
	}
	if len(rdbHeaderLine) == 0 || rdbHeaderLine[0] != '$' {
		fmt.Printf("Expected RDB bulk string header, got: %s\n", strings.TrimSpace(rdbHeaderLine))
		return
	}

	rdbLengthStr := strings.TrimSpace(rdbHeaderLine[1:])
	rdbLength, err := strconv.Atoi(rdbLengthStr)
	if err != nil {
		fmt.Printf("Invalid RDB length: %s (err: %v)\n", rdbLengthStr, err)
		return
	}

	fmt.Printf("Receiving RDB file of size: %d bytes\n", rdbLength)
	rdbData := make([]byte, rdbLength+2)
	_, err = io.ReadFull(reader, rdbData)
	if err != nil {
		fmt.Printf("Failed to read RDB data: %v\n", err)
		return
	}
	fmt.Println("RDB file received (not processed for now).")
	fmt.Println("Replication handshake complete. Slave is ready to receive commands.")

	go s.receiveCommandsFromMaster(conn, reader)
}

func (s *Server) receiveCommandsFromMaster(conn net.Conn, reader *bufio.Reader) {
	fmt.Println("Slave entering command receiving mode from master...")
	for {
		cmdArgs, err := protocol.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Master disconnected (EOF).")
			} else {
				fmt.Printf("Error reading command from master: %v\n", err)
			}
			return
		}

		if len(cmdArgs) > 0 {
			commandName := strings.ToUpper(cmdArgs[0])
			fmt.Printf("Slave received command from master: %v\n", cmdArgs)

			if commandName == "REPLCONF" && len(cmdArgs) >= 2 && strings.ToUpper(cmdArgs[1]) == "GETACK" {
				offsetStr := s.cache.GetInfo()["master_repl_offset"]
				fmt.Printf("Slave sending REPLCONF ACK %s\n", offsetStr)
				ackCmd := protocol.EncodeArray([]string{"REPLCONF", "ACK", offsetStr})
				_, err := conn.Write(ackCmd)
				if err != nil {
					fmt.Printf("Error sending REPLCONF ACK to master: %v\n", err)
					return
				}
				continue
			}

			if commandName == "SET" && len(cmdArgs) >= 3 {
				key, value := cmdArgs[1], cmdArgs[2]
				px := int64(0)
				if len(cmdArgs) == 5 && strings.ToUpper(cmdArgs[3]) == "PX" {
					if expiry, err := strconv.ParseInt(cmdArgs[4], 10, 64); err == nil {
						px = expiry
					}
				}
				s.cache.Set(key, value, px)
				fmt.Printf("Slave applied SET command: %s = %s (PX: %d)\n", key, value, px)
			} else {
				fmt.Printf("Slave skipping non-SET command from master: %s\n", commandName)
			}
		}
	}
}
