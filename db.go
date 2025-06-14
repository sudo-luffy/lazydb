package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type DB struct {
	data   map[string]string
	expiry map[string]int64
}

func NewDB() *DB {
	return &DB{
		data:   make(map[string]string),
		expiry: make(map[string]int64),
	}
}

func (db *DB) handleCommand(cmd []string) string {
	switch strings.ToUpper(cmd[0]) {
	case "PING":
		return "+PONG\r\n"
	case "ECHO":
		if len(cmd) < 2 {
			return "-ERR wrong number of arguments for 'ECHO'\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(cmd[1]), cmd[1])
	case "SET":
		if len(cmd) < 3 {
			return "-ERR wrong number of arguments for 'SET'\r\n"
		}
		key, value := cmd[1], cmd[2]
		db.data[key] = value
		if len(cmd) == 5 && strings.ToUpper(cmd[3]) == "PX" {
			// set expiry and ttl as well
			expiry, err := strconv.ParseInt(cmd[4], 10, 64)
			if err == nil {
				db.expiry[key] = time.Now().UnixMilli() + expiry
			}
		}
		return "+OK\r\n"
	case "GET":
		if len(cmd) < 2 {
			return "-ERR wrong number of arguments for 'GET'\r\n"
		}
		key := cmd[1]
		expiry, hasExpiry := db.expiry[key]
		value, exists := db.data[key]
		// return nil if no value for this key exists
		// or ttl expired
		if !exists || (hasExpiry && time.Now().UnixMilli() > expiry) {
			delete(db.data, key)
			delete(db.expiry, key)
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	default:
		return "-ERR unknown command\r\n"
	}
}
