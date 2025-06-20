package handler

import (
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/sudo-luffy/lazydb/internal/database"
	"github.com/sudo-luffy/lazydb/internal/protocol"
)

type Handler interface {
	Handle(cmdArg []string) error
	Send() error
}

type CommandHandler struct {
	cache    database.Storage
	registry SlaveRegistry
	conn     net.Conn
}

func NewCommandHandler(cache database.Storage, registry SlaveRegistry, conn net.Conn) *CommandHandler {
	return &CommandHandler{
		cache:    cache,
		registry: registry,
		conn:     conn,
	}
}

func (ch *CommandHandler) Handle(args []string) error {
	fmt.Println(args[0])
	switch strings.ToUpper(args[0]) {
	case "PING":
		return ch.ping()
	case "ECHO":
		return ch.echo(args)
	case "SET":
		return ch.set(args)
	case "GET":
		return ch.get(args)
	case "INFO":
		return ch.info(args)
	case "REPLCONF":
		return ch.replconf(args)
	case "PSYNC":
		return ch.psync(args)
	default:
		fmt.Println("default")
		ch.writeError("unknown command")
	}
	return nil
}

func (ch *CommandHandler) writeError(msg string) error {
	_, err := ch.conn.Write([]byte(protocol.EncodeError(msg)))
	return err
}

func (ch *CommandHandler) ping() error {
	fmt.Println("inside ping ..")
	_, err := ch.conn.Write(protocol.EncodeSimpleString("PONG"))
	fmt.Println("pong sent")
	if err != nil {
		fmt.Println(err)
	}
	return err
}

func (ch *CommandHandler) echo(cmd []string) error {
	conn := ch.conn
	args := cmd[1:]
	if len(args) < 1 {
		_, err := conn.Write(protocol.EncodeError("wrong number of arguments for 'ECHO' command"))
		return err
	}
	_, err := conn.Write(protocol.EncodeBulkString(args[1]))
	return err
}

func (ch *CommandHandler) set(cmd []string) error {
	// set data in db store
	conn := ch.conn
	args := cmd[1:]
	if len(args) < 2 {
		_, err := conn.Write(protocol.EncodeError("wrong number of arguments for 'SET' command"))
		return err
	}
	key, value := args[0], args[1]
	px := int64(0)

	if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
		expiryMs, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			_, writeErr := conn.Write(protocol.EncodeError("PX value is not an integer or out of range"))
			return writeErr
		}
		px = expiryMs
	} else if len(args) > 2 {
		_, writeErr := conn.Write(protocol.EncodeError("syntax error"))
		return writeErr
	}

	ch.cache.Set(key, value, px)
	_, err := conn.Write(protocol.EncodeSimpleString("OK"))
	// replicate the commands here.
	ch.registry.Replicate(cmd)
	return err
}

func (ch *CommandHandler) get(cmd []string) error {
	conn := ch.conn
	args := cmd[1:]
	// retrun value from db
	if len(args) < 1 {
		_, err := conn.Write(protocol.EncodeError("wrong number of arguments for 'GET' command"))
		return err
	}
	key := args[0]
	value, exists := ch.cache.Get(key)
	if !exists {
		_, err := conn.Write(protocol.EncodeNullBulkString())
		return err
	}
	_, err := conn.Write(protocol.EncodeBulkString(value))
	return err
}

func (ch *CommandHandler) info(cmd []string) error {
	conn := ch.conn
	args := cmd[1:]
	if len(args) == 1 && strings.ToUpper(args[0]) == "REPLICATION" {
		config := ch.cache.GetInfo()
		_, err := conn.Write(protocol.EncodeMap(config))
		return err
	}
	_, err := conn.Write(protocol.EncodeError("unknown INFO section, only 'REPLICATION' is supported"))
	return err
}

func (ch *CommandHandler) replconf(cmd []string) error {
	conn := ch.conn
	_, err := conn.Write(protocol.EncodeSimpleString("OK"))
	return err
}

func (ch *CommandHandler) psync(cmd []string) error {
	conn := ch.conn
	args := cmd[1:]
	if len(args) < 2 {
		_, err := conn.Write(protocol.EncodeError("wrong number of arguments for 'PSYNC' command"))
		return err
	}

	// 1. Send +FULLRESYNC response
	replid := ch.cache.GetInfo()["master_replid"]
	offsetStr := ch.cache.GetInfo()["master_repl_offset"]
	offset, _ := strconv.Atoi(offsetStr)

	fullResyncResp := fmt.Sprintf("+FULLRESYNC %s %d\r\n", replid, offset)
	_, err := conn.Write([]byte(fullResyncResp))
	if err != nil {
		return fmt.Errorf("failed to write FULLRESYNC response: %w", err)
	}

	// 2. Send RDB file as a RESP Bulk String
	rdbBytes, decodeErr := hex.DecodeString(database.EmptyRDB)
	if decodeErr != nil {
		return fmt.Errorf("failed to decode empty RDB string: %w", decodeErr)
	}

	rdbBulkStringHeader := fmt.Sprintf("$%d\r\n", len(rdbBytes))
	_, err = conn.Write([]byte(rdbBulkStringHeader))
	if err != nil {
		return fmt.Errorf("failed to write RDB bulk string header: %w", err)
	}

	_, err = conn.Write(rdbBytes)
	if err != nil {
		return fmt.Errorf("failed to write RDB content: %w", err)
	}
	_, err = conn.Write([]byte("\r\n"))
	if err != nil {
		return fmt.Errorf("failed to write RDB bulk string terminator: %w", err)
	}

	fmt.Println("Sent FULLRESYNC and empty RDB file.")
	ch.registry.RegisterSlave(conn.RemoteAddr().String(), conn)
	fmt.Printf("Slave %s added to replication list.\n", conn.RemoteAddr().String())
	return nil
}
