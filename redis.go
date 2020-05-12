package idempotent

import (
	"github.com/easy-bus/bus"
	"github.com/gomodule/redigo/redis"
	"github.com/letsfire/redigo/v2"
)

type redisIdempotent struct {
	hashMap string
	client  *redigo.Client
}

func (ri *redisIdempotent) Acquire(key string) (bool, error) {
	return ri.client.Bool(func(conn redis.Conn) (interface{}, error) {
		return conn.Do("HSETNX", ri.hashMap, key, 1)
	})
}

func (ri *redisIdempotent) Release(key string) error {
	_, err := ri.client.Execute(func(conn redis.Conn) (interface{}, error) {
		return conn.Do("HDEL", ri.hashMap, key)
	})
	return err
}

func NewRedis(hashMap string, client *redigo.Client) bus.IdempotentInterface {
	return &redisIdempotent{hashMap: hashMap, client: client}
}
