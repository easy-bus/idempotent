package idempotent

import (
	"sync/atomic"
	"testing"

	"github.com/letsfire/redigo/mode/alone"
	"github.com/stretchr/testify/assert"
)

var key = "test-key"
var idempotent = NewRedis("test", alone.NewClient())

func BenchmarkRedisIdempotent(b *testing.B) {
	var counter uint32
	assert.Nil(b, idempotent.Release(key))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ok, err := idempotent.Acquire(key)
			assert.Nil(b, err)
			if ok {
				atomic.AddUint32(&counter, 1)
			}
		}
	})
	assert.Equal(b, uint32(1), counter)
	assert.Nil(b, idempotent.Release(key))
	ok, err := idempotent.Acquire(key)
	assert.Nil(b, err)
	assert.True(b, ok)
}
