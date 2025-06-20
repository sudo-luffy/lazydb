package database

import (
	"crypto/rand"
	"encoding/hex"
)

type CacheConfig struct {
	Role             string
	MasterReplid     string
	MasterReplOffset string
}

func NewCacheConfig() CacheConfig {
	return CacheConfig{
		Role:             "master",
		MasterReplid:     generateReplID(),
		MasterReplOffset: "0",
	}
}

func generateReplID() string {
	b := make([]byte, 20)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
