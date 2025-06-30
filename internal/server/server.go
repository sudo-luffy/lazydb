package server

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sudo-luffy/lazydb/internal/database"
	"github.com/sudo-luffy/lazydb/internal/handler"
	"github.com/sudo-luffy/lazydb/internal/protocol"
	"github.com/sudo-luffy/lazydb/internal/slave"
	"github.com/sudo-luffy/lazydb/internal/types"
)

// State holds the replication state of the server.
type State struct {
	mu               sync.RWMutex
	Role             string
	MasterReplid     string
	MasterReplOffset int
}

func NewState() *State {
	return &State{
		Role:             "master",
		MasterReplid:     generateReplID(),
		MasterReplOffset: 0,
	}
}

func generateReplID() string {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("Failed to generate replication ID: %v", err)
	}
	return hex.EncodeToString(b)
}

// Server represents the Redis server instance.
type Server struct {
	listener net.Listener
	cache    database.Storage
	port     int
	slaves   map[string]*slave.Slave
	slavesMu sync.RWMutex
	state    *State
}

func NewServer(port int, replicaOf string, cache database.Storage, state *State) (*Server, error) {
	address := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to bind port %d: %w", port, err)
	}
	srv := &Server{
		listener: listener,
		cache:    cache,
		port:     port,
		slaves:   make(map[string]*slave.Slave),
		state:    state,
	}
	if len(replicaOf) > 0 {
		srv.state.SetRole("slave")
		log.Printf("Running as replica of: %s\n", replicaOf)
		go srv.handleHandshake(replicaOf)
	}
	return srv, nil
}

func (s *Server) Start() {
	log.Printf("Listening on %s\n", s.listener.Addr().String())
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if the error is due to listener being closed
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				log.Println("Listener timeout, retrying accept.")
				continue
			}
			if err == net.ErrClosed {
				log.Println("Server listener closed, shutting down.")
				return
			}
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		// Use goroutine pool or limit active goroutines for high concurrency scenarios
		go s.handleConnection(conn)
	}
}

func (s *Server) Close() error {
	log.Println("Server shutting down...")
	// Close all slave connections
	s.slavesMu.Lock()
	for addr, slv := range s.slaves {
		if err := slv.Close(); err != nil {
			log.Printf("Error closing slave connection %s: %v", addr, err)
		}
	}
	s.slaves = make(map[string]*slave.Slave)
	s.slavesMu.Unlock()
	return s.listener.Close()
}

func (s *Server) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("Client connected: %s", remoteAddr)
	defer func() {
		log.Printf("Client disconnected: %s", remoteAddr)
		conn.Close()
	}()

	// Use buffered I/O with explicit flush for writes if needed, but RESP writes are usually self-contained.
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	cmdHandler := handler.NewCommandHandler(s, conn, writer)

	for {
		cmdArgs, err := protocol.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client %s disconnected (EOF)", remoteAddr)
				return
			}
			log.Printf("Parsing error from %s: %v", remoteAddr, err)
			// Write error to client, but careful not to block/deadlock
			if _, writeErr := conn.Write([]byte(protocol.EncodeError(fmt.Sprintf("protocol error: %v", err)))); writeErr != nil {
				log.Printf("Error writing error response to %s: %v", remoteAddr, writeErr)
			}
			return
		}
		if len(cmdArgs) == 0 {
			continue
		}

		if err := cmdHandler.Handle(cmdArgs); err != nil {
			log.Printf("Error handling command from %s: %v", remoteAddr, err)
			return
		}
	}
}

// Slave related stuff
func (s *Server) RegisterSlave(addr string, conn net.Conn) {
	s.slavesMu.Lock()
	defer s.slavesMu.Unlock()
	s.slaves[addr] = &slave.Slave{
		Conn:   conn,
		Offset: 0,
		Writer: bufio.NewWriter(conn),
	}
	log.Printf("Registered slave %s", addr)
}

func (s *Server) GetSlave(addr string) *slave.Slave {
	s.slavesMu.RLock()
	defer s.slavesMu.RUnlock()
	return s.slaves[addr]
}

func (s *Server) GetSlaveCount() int {
	s.slavesMu.RLock()
	defer s.slavesMu.RUnlock()
	return len(s.slaves)
}

// Replicate sends a command to all connected slaves.
func (s *Server) Replicate(cmd []string) {
	encodedCmd := protocol.EncodeArray(cmd)
	s.state.IncrementReplicationOffset(len(encodedCmd))

	s.slavesMu.RLock()
	slavesToSendTo := make(map[string]*slave.Slave, len(s.slaves))
	for addr, slv := range s.slaves {
		slavesToSendTo[addr] = slv
	}
	s.slavesMu.RUnlock()

	if len(slavesToSendTo) == 0 {
		return
	}

	log.Printf("Replicating command %v (encoded length: %d) to %d slaves", cmd, len(encodedCmd), len(slavesToSendTo))

	var failedSlaves []string
	for addr, slv := range slavesToSendTo {
		if err := slv.Write(encodedCmd); err != nil {
			log.Printf("Replication to %s failed: %v. Marking for removal.", addr, err)
			slv.Close()
			failedSlaves = append(failedSlaves, addr)
		}
	}

	// Remove failed slaves outside the RLock
	if len(failedSlaves) > 0 {
		s.slavesMu.Lock()
		for _, addr := range failedSlaves {
			delete(s.slaves, addr)
			log.Printf("Removed disconnected slave: %s", addr)
		}
		s.slavesMu.Unlock()
	}
}

// handleHandshake initiates the replication handshake for a slave.
func (s *Server) handleHandshake(replicaAddress string) {
	parts := strings.Split(replicaAddress, " ")
	if len(parts) != 2 {
		log.Printf("Invalid replicaof format: %s. Expected 'host port'", replicaAddress)
		return
	}
	masterHost, masterPort := parts[0], parts[1]
	address := net.JoinHostPort(masterHost, masterPort)

	// Implement reconnection logic
	const maxRetries = 5
	for attempt := 1; ; attempt++ {
		log.Printf("Attempting to connect to master at %s for handshake (Attempt %d)...", address, attempt)
		conn, err := net.Dial("tcp", address)
		if err != nil {
			log.Printf("Failed to connect to master for handshake: %v", err)
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second) // Exponential backoff
				continue
			}
			log.Printf("Max retries reached for master connection %s. Giving up.", address)
			return
		}
		log.Printf("Connected to master at %s for handshake.", address)

		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)

		if err := s.performHandshakeSteps(reader, writer); err != nil {
			log.Printf("Handshake with master %s failed: %v. Closing connection and retrying...", address, err)
			conn.Close()
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			log.Printf("Max retries reached for handshake with master %s. Giving up.", address)
			return
		}

		log.Println("Replication handshake complete. Slave is ready to receive commands.")
		go s.receiveCommandsFromMaster(conn, reader, writer)
		return
	}
}

// performHandshakeSteps contains the actual handshake logic.
func (s *Server) performHandshakeSteps(reader *bufio.Reader, writer *bufio.Writer) error {
	var err error

	// PING
	if err = protocol.SendAndExpect(writer, reader, protocol.EncodeArray([]string{"PING"}), "+PONG"); err != nil {
		return fmt.Errorf("PING failed: %w", err)
	}
	log.Println("Received PONG from master.")

	// REPLCONF listening-port
	replconfPortCmd := protocol.EncodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(s.port)})
	if err = protocol.SendAndExpect(writer, reader, replconfPortCmd, "+OK"); err != nil {
		return fmt.Errorf("REPLCONF listening-port failed: %w", err)
	}
	log.Println("Received OK for REPLCONF listening-port.")

	// REPLCONF capa psync2
	replconfCapaCmd := protocol.EncodeArray([]string{"REPLCONF", "capa", "psync2"})
	if err = protocol.SendAndExpect(writer, reader, replconfCapaCmd, "+OK"); err != nil {
		return fmt.Errorf("REPLCONF capa psync2 failed: %w", err)
	}
	log.Println("Received OK for REPLCONF capa psync2.")

	// PSYNC
	psyncCmd := protocol.EncodeArray([]string{"PSYNC", "?", "-1"})
	if _, err = writer.Write(psyncCmd); err != nil {
		return fmt.Errorf("failed to send PSYNC to master: %w", err)
	}
	if err = writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush PSYNC: %w", err)
	}

	fullResyncLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read FULLRESYNC response: %w", err)
	}
	if !strings.HasPrefix(fullResyncLine, "+FULLRESYNC") {
		return fmt.Errorf("expected +FULLRESYNC, got: %s", strings.TrimSpace(fullResyncLine))
	}
	log.Printf("Received FULLRESYNC response: %s", strings.TrimSpace(fullResyncLine))

	parts := strings.Split(strings.TrimSpace(fullResyncLine), " ")
	if len(parts) >= 3 {
		replid := parts[1]
		offset, parseErr := strconv.Atoi(parts[2])
		if parseErr != nil {
			log.Printf("Warning: Could not parse master offset from FULLRESYNC: %v", parseErr)
		} else {
			s.state.SetMasterInfo(replid, offset)
			log.Printf("Updated slave config: replid=%s, offset=%d", replid, offset)
		}
	}

	// RDB file
	rdbHeaderLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB bulk string header: %w", err)
	}
	if len(rdbHeaderLine) == 0 || rdbHeaderLine[0] != '$' {
		return fmt.Errorf("expected RDB bulk string header, got: %s", strings.TrimSpace(rdbHeaderLine))
	}

	rdbLengthStr := strings.TrimSpace(rdbHeaderLine[1:])
	rdbLength, err := strconv.Atoi(rdbLengthStr)
	if err != nil {
		return fmt.Errorf("invalid RDB length: %s (err: %w)", rdbLengthStr, err)
	}

	log.Printf("Receiving RDB file of size: %d bytes", rdbLength)
	rdbData := make([]byte, rdbLength)
	if _, err = io.ReadFull(reader, rdbData); err != nil {
		return fmt.Errorf("failed to read RDB data: %w", err)
	}
	log.Println("RDB file received (not processed for now).")
	return nil
}

// receiveCommandsFromMaster continuously reads and processes commands from the master.
func (s *Server) receiveCommandsFromMaster(conn net.Conn, reader *bufio.Reader, writer *bufio.Writer) {
	log.Println("Slave entering command receiving mode from master...")
	defer func() {
		log.Println("Slave connection to master closed.")
		conn.Close()
		// Reconnect logic: this is a simple retry, consider exponential backoff and max retries in production.
		go s.handleHandshake(net.JoinHostPort(strings.Split(conn.RemoteAddr().String(), ":")[0], strconv.Itoa(s.port)))
	}()

	for {
		cmdArgs, err := protocol.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				log.Println("Master disconnected (EOF) during command reception.")
			} else {
				log.Printf("Error reading command from master: %v", err)
			}
			return
		}
		if len(cmdArgs) == 0 {
			continue
		}

		receivedCommandBytes := protocol.EncodeArray(cmdArgs)
		offsetChange := len(receivedCommandBytes)
		commandName := strings.ToUpper(cmdArgs[0])

		log.Printf("Slave received command from master: %v", cmdArgs)

		// Process commands
		switch commandName {
		case "SET":
			if len(cmdArgs) >= 3 {
				key, value := cmdArgs[1], cmdArgs[2]
				px := int64(0)
				if len(cmdArgs) == 5 && strings.ToUpper(cmdArgs[3]) == "PX" {
					if expiry, err := strconv.ParseInt(cmdArgs[4], 10, 64); err == nil {
						px = expiry
					} else {
						log.Printf("Warning: Invalid PX value in SET command from master: %s", cmdArgs[4])
					}
				}
				s.cache.Set(key, value, px)
				log.Printf("Slave applied SET command: %s = %s (PX: %d)", key, value, px)
				s.state.IncrementReplicationOffset(offsetChange)

			} else {
				log.Printf("Slave received malformed SET command from master: %v", cmdArgs)
			}
		case "PING":
			log.Println("Slave received PING from master.")
			s.state.IncrementReplicationOffset(offsetChange)
		case "REPLCONF":
			if len(cmdArgs) >= 2 && strings.ToUpper(cmdArgs[1]) == "GETACK" {
				offsetStr := strconv.Itoa(s.state.GetMasterReplOffset())
				ackCmd := protocol.EncodeArray([]string{"REPLCONF", "ACK", offsetStr})
				if _, err := writer.Write(ackCmd); err != nil {
					log.Printf("Error sending REPLCONF ACK to master: %v", err)
					return
				}
				if err := writer.Flush(); err != nil {
					log.Printf("Error flushing REPLCONF ACK to master: %v", err)
					return
				}
				log.Printf("Slave sent REPLCONF ACK %s", offsetStr)
				s.state.IncrementReplicationOffset(offsetChange)

			} else {
				log.Printf("Slave skipping unknown REPLCONF subcommand from master: %v", cmdArgs)
			}
		default:
			log.Printf("Slave skipping unknown replication command from master: %v", cmdArgs)
		}
	}
}

func (s *Server) GetCache() database.Storage {
	return s.cache
}

func (s *Server) GetState() map[string]string {
	return s.state.ToMap()
}

func (s *Server) GetRegistry() types.SlaveRegistry {
	return s
}

func (s *Server) GetInfo() State {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	return State{
		Role:             s.state.Role,
		MasterReplid:     s.state.MasterReplid,
		MasterReplOffset: s.state.MasterReplOffset,
	}
}

func (s *Server) CountAckedSlaves(masterOffset int) int {
	s.slavesMu.RLock()
	defer s.slavesMu.RUnlock()

	count := 0
	for _, slv := range s.slaves {
		if slv.GetOffset() >= masterOffset {
			count++
		}
	}
	return count
}

func (s *Server) SendAck() error {
	s.slavesMu.RLock()
	defer s.slavesMu.RUnlock()
	var firstErr error
	for addr, slv := range s.slaves {
		replconfGetAck := protocol.EncodeArray([]string{"REPLCONF", "GETACK", "*"})
		if err := slv.Write(replconfGetAck); err != nil {
			log.Printf("Failed to send REPLCONF GETACK to slave %s: %v", addr, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// State methods
func (s *State) GetMasterReplOffset() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MasterReplOffset
}

func (s *State) IncrementReplicationOffset(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MasterReplOffset += n
	log.Printf("Replication offset updated to: %d", s.MasterReplOffset)
}

func (s *State) SetRole(role string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Role = role
}

func (s *State) SetMasterInfo(replid string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MasterReplid = replid
	s.MasterReplOffset = offset
}

func (s *State) ToMap() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]string{
		"role":               s.Role,
		"master_replid":      s.MasterReplid,
		"master_repl_offset": strconv.Itoa(s.MasterReplOffset),
	}
}
