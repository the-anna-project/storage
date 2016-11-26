package redis

import (
	"github.com/garyburd/redigo/redis"
	"github.com/rafaeljusto/redigomock"
)

// redis

// DialConfig represents the configuration used to create a new redis
// dialer.
type DialConfig struct {
	// Addr represents the address used to connect to a redis server.
	Addr string
}

// DefaultDialConfig provides a default configuration to create a new
// redis dialer by best effort.
func DefaultDialConfig() DialConfig {
	newConfig := DialConfig{
		Addr: "127.0.0.1:6379",
	}

	return newConfig
}

// NewDial creates a new configured redis dialer.
func NewDial(config DialConfig) func() (redis.Conn, error) {
	newDial := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", config.Addr)
		if err != nil {
			return nil, err
		}

		return c, nil
	}

	return newDial
}

type MockDialConfig struct {
	RedisConn redis.Conn
}

func DefaultMockDialConfig() MockDialConfig {
	newConfig := MockDialConfig{
		RedisConn: redigomock.NewConn(),
	}

	return newConfig
}

func NewMockDial(config MockDialConfig) func() (redis.Conn, error) {
	newDial := func() (redis.Conn, error) {
		return config.RedisConn, nil
	}

	return newDial
}
