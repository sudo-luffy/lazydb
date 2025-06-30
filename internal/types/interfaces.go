package types

import (
	"net"

	"github.com/sudo-luffy/lazydb/internal/database"
	"github.com/sudo-luffy/lazydb/internal/slave"
)

// SlaveRegistry defines methods for managing connected slaves.
type SlaveRegistry interface {
	RegisterSlave(addr string, conn net.Conn)
	GetSlaveCount() int
	Replicate(cmd []string)
	GetSlave(addr string) *slave.Slave
	CountAckedSlaves(masterOffset int) int
	SendAck() error
}

type ServerContext interface {
	GetCache() database.Storage
	GetRegistry() SlaveRegistry
	GetState() map[string]string
}
