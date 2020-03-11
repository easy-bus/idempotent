package redis_idempotent

import (
	"github.com/easy-bus/bus"
	"github.com/gomodule/redigo/redis"
	"github.com/letsfire/redigo"
)

type redisIdempotent struct {
	client *redigo.Client
}

func (ri redisIdempotent) Acquire(key string) (bool, error) {
	return ri.client.Bool(func(conn redis.Conn) (interface{}, error) {
		return conn.Do("SETNX", key, 1)
	})
}

func (ri redisIdempotent) Release(key string) error {
	_, err := ri.client.Execute(func(conn redis.Conn) (interface{}, error) {
		return conn.Do("DEL", key)
	})
	return err
}

func New(client *redigo.Client) bus.IdempotentInterface {
	return redisIdempotent{client: client}
}
