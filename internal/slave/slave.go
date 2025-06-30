package slave

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync"
)

// Slave represents a connected slave to the master.
type Slave struct {
	Conn   net.Conn
	Writer *bufio.Writer
	Offset int
	mu     sync.Mutex
}

// Write sends data to the slave connection, using the buffered writer.
func (s *Slave) Write(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Conn == nil {
		return fmt.Errorf("slave connection is nil, cannot write")
	}
	if _, err := s.Writer.Write(data); err != nil {
		s.Writer.Flush()
		return fmt.Errorf("failed to write to slave buffer: %w", err)
	}
	if err := s.Writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush data to slave: %w", err)
	}
	return nil
}

// Close gracefully closes the slave connection.
func (s *Slave) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Conn == nil {
		return nil
	}
	if s.Writer != nil {
		if err := s.Writer.Flush(); err != nil {
			log.Printf("Warning: Error flushing slave writer before close: %v", err)
		}
	}
	err := s.Conn.Close()
	s.Conn = nil
	s.Writer = nil // Nil out writer too
	return err
}

// UpdateOffset updates the slave's acknowledged replication offset.
func (s *Slave) UpdateOffset(offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Offset = offset
}

// GetOffset returns the slave's acknowledged replication offset.
func (s *Slave) GetOffset() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Offset
}
