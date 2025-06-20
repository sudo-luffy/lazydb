package handler

import "net"

type SlaveRegistry interface {
	RegisterSlave(addr string, conn net.Conn)
	Replicate(cmd []string)
}
