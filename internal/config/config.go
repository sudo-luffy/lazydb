package config

import (
	"crypto/rand"
	"encoding/hex"
)

type Config struct {
	Role             string
	MasterReplID     string
	MasterReplOffset int
}

func NewConfig() *Config {
	return &Config{
		Role:             "master",
		MasterReplID:     generateReplID(),
		MasterReplOffset: 0,
	}
}

func (c Config) getRole() string {
	return c.Role
}

func (c *Config) setRole(role string) {
	c.Role = role
}

func (c Config) GetMasterReplID() string {
	return c.MasterReplID
}

func (c Config) GetMasterReplOffset() int {
	return c.MasterReplOffset
}

func generateReplID() string {
	b := make([]byte, 20)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
