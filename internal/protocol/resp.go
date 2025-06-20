package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ParseRESP reads a RESP array command from the bufio.Reader.
func ParseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("invalid array prefix: %q", strings.TrimSpace(line))
	}

	countStr := strings.TrimSpace(line[1:])
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, fmt.Errorf("invalid array size: %q", countStr)
	}

	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("expected bulk string header, got error: %w", err)
		}
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("invalid bulk string prefix: %q", strings.TrimSpace(line))
		}

		lengthStr := strings.TrimSpace(line[1:])
		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %q", lengthStr)
		}

		data := make([]byte, length+2) // +2 for \r\n
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, fmt.Errorf("failed to read bulk string data: %w", err)
		}
		result = append(result, string(data[:length]))
	}
	return result, nil
}

// EncodeSimpleString encodes a simple string response (+<string>\r\n).
func EncodeSimpleString(s string) []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

// EncodeBulkString encodes a bulk string response ($<length>\r\n<string>\r\n).
func EncodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

// EncodeNullBulkString encodes a null bulk string response ($-1\r\n).
func EncodeNullBulkString() []byte {
	return []byte("$-1\r\n")
}

// EncodeError encodes an error response (-ERR <message>\r\n).
func EncodeError(msg string) []byte {
	return []byte(fmt.Sprintf("-ERR %s\r\n", msg))
}

// EncodeArray encodes a RESP array.
func EncodeArray(elements []string) []byte {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(elements)))
	for _, elem := range elements {
		builder.Write(EncodeBulkString(elem)) // EncodeBulkString now returns []byte
	}
	return []byte(builder.String())
}

// EncodeMap encodes a map as a single RESP bulk string for INFO command.
func EncodeMap(m map[string]string) []byte {
	var infoLines []string
	for k, v := range m {
		infoLines = append(infoLines, fmt.Sprintf("%s:%s", k, v))
	}
	return EncodeBulkString(strings.Join(infoLines, "\n"))
}
