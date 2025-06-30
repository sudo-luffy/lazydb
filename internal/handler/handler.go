package handler

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/sudo-luffy/lazydb/internal/database"
	"github.com/sudo-luffy/lazydb/internal/protocol"
	"github.com/sudo-luffy/lazydb/internal/types"
)

type Handler interface {
	Handle(cmdArg []string) error
}

type CommandHandler struct {
	server types.ServerContext
	conn   net.Conn
	writer *bufio.Writer
}

func NewCommandHandler(server types.ServerContext, conn net.Conn, writer *bufio.Writer) *CommandHandler {
	return &CommandHandler{
		server: server,
		conn:   conn,
		writer: writer,
	}
}

func (ch *CommandHandler) Handle(args []string) error {
	if len(args) == 0 {
		return nil
	}
	cmdName := strings.ToUpper(args[0])
	log.Printf("Handling command: %s from %s", cmdName, ch.conn.RemoteAddr())

	var err error
	switch cmdName {
	case "PING":
		err = ch.ping()
	case "ECHO":
		err = ch.echo(args)
	case "SET":
		err = ch.set(args)
	case "GET":
		err = ch.get(args)
	case "INFO":
		err = ch.info(args)
	case "REPLCONF":
		err = ch.replconf(args)
	case "PSYNC":
		err = ch.psync(args)
	case "WAIT":
		err = ch.wait(args)
	default:
		err = ch.writeError("unknown command '" + cmdName + "'")
	}

	if flushErr := ch.writer.Flush(); flushErr != nil {
		log.Printf("Error flushing writer to %s after handling %s: %v", ch.conn.RemoteAddr(), cmdName, flushErr)
		return flushErr
	}
	return err
}

func (ch *CommandHandler) writeError(msg string) error {
	_, err := ch.writer.Write([]byte(protocol.EncodeError(msg)))
	return err
}

func (ch *CommandHandler) ping() error {
	log.Println("Responding to PING...")
	_, err := ch.writer.Write(protocol.EncodeSimpleString("PONG"))
	return err
}

func (ch *CommandHandler) echo(cmd []string) error {
	if len(cmd) < 2 {
		return ch.writeError("wrong number of arguments for 'ECHO' command")
	}
	_, err := ch.writer.Write(protocol.EncodeBulkString(cmd[1]))
	return err
}

func (ch *CommandHandler) set(cmd []string) error {
	if len(cmd) < 3 {
		return ch.writeError("wrong number of arguments for 'SET' command")
	}
	key, value := cmd[1], cmd[2]
	px := int64(0)

	// Check for PX argument
	if len(cmd) >= 4 {
		if strings.ToUpper(cmd[3]) == "PX" {
			if len(cmd) < 5 {
				return ch.writeError("syntax error, PX requires a numeric argument")
			}
			expiryMs, err := strconv.ParseInt(cmd[4], 10, 64)
			if err != nil {
				return ch.writeError("PX value is not an integer or out of range")
			}
			px = expiryMs
		} else {
			return ch.writeError("syntax error, unknown option '" + cmd[3] + "'")
		}
	}

	ch.server.GetCache().Set(key, value, px)
	_, err := ch.writer.Write(protocol.EncodeSimpleString("OK"))

	// Replicate the command to slaves. The master's offset is updated in Server.Replicate.
	ch.server.GetRegistry().Replicate(cmd)
	return err
}

func (ch *CommandHandler) get(cmd []string) error {
	if len(cmd) < 2 {
		return ch.writeError("wrong number of arguments for 'GET' command")
	}
	key := cmd[1]
	value, exists := ch.server.GetCache().Get(key)
	if !exists {
		_, err := ch.writer.Write(protocol.EncodeNullBulkString())
		return err
	}
	_, err := ch.writer.Write(protocol.EncodeBulkString(value))
	return err
}

func (ch *CommandHandler) info(cmd []string) error {
	if len(cmd) < 2 || strings.ToUpper(cmd[1]) != "REPLICATION" {
		return ch.writeError("unknown INFO section, only 'REPLICATION' is supported")
	}
	config := ch.server.GetState()
	_, err := ch.writer.Write(protocol.EncodeMap(config))
	return err
}

func (ch *CommandHandler) replconf(cmd []string) error {
	if len(cmd) < 2 {
		return ch.writeError("wrong number of arguments for 'REPLCONF' command")
	}

	subcmd := strings.ToUpper(cmd[1])
	switch subcmd {
	case "LISTENING-PORT", "CAPA":
		_, err := ch.writer.Write(protocol.EncodeSimpleString("OK"))
		return err

	case "GETACK":
		offsetStr := ch.server.GetState()["master_repl_offset"]
		reply := protocol.EncodeArray([]string{"REPLCONF", "ACK", offsetStr})
		_, err := ch.writer.Write(reply)
		return err

	case "ACK":
		if len(cmd) < 3 {
			return ch.writeError("wrong number of arguments for 'REPLCONF ACK' command")
		}
		offset, err := strconv.Atoi(cmd[2])
		if err != nil {
			log.Printf("Error parsing ACK offset from slave %s: %v, value: %s", ch.conn.RemoteAddr(), err, cmd[2])
			return ch.writeError("invalid ACK offset")
		}
		remoteAddr := ch.conn.RemoteAddr().String()
		slv := ch.server.GetRegistry().GetSlave(remoteAddr)
		if slv != nil {
			slv.UpdateOffset(offset)
			log.Printf("Received ACK from slave %s, updated offset to %d", remoteAddr, offset)
		} else {
			log.Printf("Slave not found for address %s when ACK received (might have disconnected)", remoteAddr)
		}
		return nil
	default:
		return ch.writeError("unknown REPLCONF subcommand '" + subcmd + "'")
	}
}

func (ch *CommandHandler) psync(cmd []string) error {
	if len(cmd) < 3 {
		return ch.writeError("wrong number of arguments for 'PSYNC' command")
	}

	// 1. Send +FULLRESYNC response
	replid := ch.server.GetState()["master_replid"]
	offset := ch.server.GetState()["master_repl_offset"]

	fullResyncResp := fmt.Sprintf("+FULLRESYNC %s %s\r\n", replid, offset)
	if _, err := ch.writer.Write([]byte(fullResyncResp)); err != nil {
		return fmt.Errorf("failed to write FULLRESYNC response: %w", err)
	}

	// 2. Send RDB file as a RESP Bulk String
	rdbBytes, decodeErr := hex.DecodeString(database.EmptyRDB)
	if decodeErr != nil {
		return fmt.Errorf("failed to decode empty RDB string: %w", decodeErr)
	}

	// Write RDB header and content directly to the buffered writer
	rdbBulkStringHeader := fmt.Sprintf("$%d\r\n", len(rdbBytes))
	if _, err := ch.writer.Write([]byte(rdbBulkStringHeader)); err != nil {
		return fmt.Errorf("failed to write RDB bulk string header: %w", err)
	}
	if _, err := ch.writer.Write(rdbBytes); err != nil {
		return fmt.Errorf("failed to write RDB content: %w", err)
	}

	log.Println("Sent FULLRESYNC and empty RDB file.")
	ch.server.GetRegistry().RegisterSlave(ch.conn.RemoteAddr().String(), ch.conn)
	log.Printf("Slave %s added to replication list.", ch.conn.RemoteAddr().String())

	return nil
}

func (ch *CommandHandler) wait(cmd []string) error {
	if len(cmd) < 3 {
		return ch.writeError("wrong number of arguments for 'WAIT' command")
	}

	numSlaves, err1 := strconv.Atoi(cmd[1])
	timeoutMillis, err2 := strconv.Atoi(cmd[2])

	if err1 != nil || err2 != nil {
		return ch.writeError("ERR invalid arguments for WAIT")
	}

	start := time.Now()
	deadline := start.Add(time.Duration(timeoutMillis) * time.Millisecond)
	masterOffset := ch.server.GetState()["master_repl_offset"]
	masterOffsetInt, _ := strconv.Atoi(masterOffset)

	// Send initial ACK requests to all slaves
	if err := ch.server.GetRegistry().SendAck(); err != nil {
		log.Printf("Error sending initial ACK requests for WAIT: %v", err)
	}

	// Track when the last GETACK was sent, to avoid spamming
	lastGetAckSent := time.Now()

	for {
		acked := ch.server.GetRegistry().CountAckedSlaves(masterOffsetInt)
		log.Printf("WAIT: Current acked slaves %d / required %d at offset %d", acked, numSlaves, masterOffsetInt)

		if acked >= numSlaves {
			_, err := ch.writer.Write(protocol.EncodeInteger(acked))
			return err
		}

		if timeoutMillis > 0 && time.Now().After(deadline) {
			_, err := ch.writer.Write(protocol.EncodeInteger(acked))
			return err
		}

		// Don't busy-wait. Sleep for a short duration.
		time.Sleep(10 * time.Millisecond)
		// we will not send next ack until next 50sec, to prevent slaves getting unnecessary getack command.
		if time.Since(lastGetAckSent) >= 50*time.Millisecond && (time.Now().Before(deadline) || timeoutMillis == 0) {
			if err := ch.server.GetRegistry().SendAck(); err != nil {
				log.Printf("Error sending periodic ACK requests for WAIT: %v", err)
			}
			lastGetAckSent = time.Now()
		}
	}
}
