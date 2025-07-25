package database

import (
	"time"
)

type Storage interface {
	Get(key string) (string, bool)
	Set(key, value string, px int64)
	// GetInfo() map[string]string
	// SetConfig(key, value string)
	// IncrementMasterReplOffset(n int)
}

type InMemoryCache struct {
	data   map[string]string
	expiry map[string]int64
}

func NewDB() *InMemoryCache {
	return &InMemoryCache{
		data:   make(map[string]string),
		expiry: make(map[string]int64),
	}
}

func (cache *InMemoryCache) Get(key string) (string, bool) {
	expiry, hasExpiry := cache.expiry[key]
	value, exists := cache.data[key]
	// return nil if no value for this key exists
	// or ttl expired
	if !exists || (hasExpiry && time.Now().UnixMilli() > expiry) {
		delete(cache.data, key)
		delete(cache.expiry, key)
		return "", false
	}
	return value, true
}

func (cache *InMemoryCache) Set(key, value string, px int64) {
	cache.data[key] = value
	if px > 0 {
		// set expiry and ttl as well
		cache.expiry[key] = time.Now().UnixMilli() + px
	} else {
		delete(cache.expiry, key)
	}
}

const EmptyRDB = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d66c2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
