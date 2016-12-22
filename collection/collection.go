// Package collection implements services to persist data. The storage
// collection bundles storage instances to pass them around more easily.
package collection

import (
	"sync"

	"github.com/cenk/backoff"
	"github.com/the-anna-project/instrumentor"
	memoryinstrumentor "github.com/the-anna-project/instrumentor/memory"
	"github.com/the-anna-project/logger"

	"github.com/the-anna-project/storage"
	"github.com/the-anna-project/storage/memory"
	"github.com/the-anna-project/storage/redis"
)

const (
	// KindMemory is the kind to be used to create a memory storage services.
	KindMemory = "memory"
	// KindRedis is the kind to be used to create a collection of redis storage
	// services.
	KindRedis = "redis"
)

// RedisConfig is the config applied to each redis instance. This is not
// relevant in case the memory kind is used.
type RedisConfig struct {
	Address string
	Prefix  string
}

// Redis is a config bundle of redis configs.
type Redis struct {
	Connection RedisConfig
	Feature    RedisConfig
	General    RedisConfig
	Index      RedisConfig
	Peer       RedisConfig
	Queue      RedisConfig
}

// Config represents the configuration used to create a new storage collection.
type Config struct {
	// Dependencies.
	BackoffFactory func() storage.Backoff
	Logger         logger.Service
	Instrumentor   instrumentor.Service

	// Settings.
	Kind  string
	Redis *Redis
}

// DefaultConfig provides a default configuration to create a new storage
// collection by best effort.
func DefaultConfig() Config {
	var err error

	var loggerService logger.Service
	{
		loggerConfig := logger.DefaultConfig()
		loggerService, err = logger.New(loggerConfig)
		if err != nil {
			panic(err)
		}
	}

	var instrumentorService instrumentor.Service
	{
		instrumentorConfig := memoryinstrumentor.DefaultConfig()
		instrumentorService, err = memoryinstrumentor.New(instrumentorConfig)
		if err != nil {
			panic(err)
		}
	}

	config := Config{
		// Dependencies.
		BackoffFactory: func() storage.Backoff {
			return &backoff.StopBackOff{}
		},
		Instrumentor: instrumentorService,
		Logger:       loggerService,

		// Settings.
		Kind:  KindMemory,
		Redis: nil,
	}

	return config
}

// New creates a new configured storage Collection.
func New(config Config) (*Collection, error) {
	// Dependencies.
	if config.BackoffFactory == nil {
		return nil, maskAnyf(invalidConfigError, "backoff factory must not be empty")
	}
	if config.Instrumentor == nil {
		return nil, maskAnyf(invalidConfigError, "instrumentor must not be empty")
	}
	if config.Logger == nil {
		return nil, maskAnyf(invalidConfigError, "logger must not be empty")
	}

	// Settings.
	if config.Kind == "" {
		return nil, maskAnyf(invalidConfigError, "kind must not be empty")
	}
	if config.Kind != KindMemory && config.Kind != KindRedis {
		return nil, maskAnyf(invalidConfigError, "kind must be one of: %s, %s", KindMemory, KindRedis)
	}
	if config.Kind == KindRedis && config.Redis == nil {
		return nil, maskAnyf(invalidConfigError, "redis config must not be empty")
	}

	var err error

	var connectionService storage.Service
	{
		switch config.Kind {
		case KindMemory:
			connectionConfig := memory.DefaultConfig()
			connectionService, err = memory.New(connectionConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			connectionConfig := redis.DefaultConfig()
			connectionConfig.Address = config.Redis.Connection.Address
			connectionConfig.BackoffFactory = config.BackoffFactory
			connectionConfig.Instrumentor = config.Instrumentor
			connectionConfig.Logger = config.Logger
			connectionConfig.Prefix = config.Redis.Connection.Prefix
			connectionService, err = redis.New(connectionConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var featureService storage.Service
	{
		switch config.Kind {
		case KindMemory:
			featureConfig := memory.DefaultConfig()
			featureService, err = memory.New(featureConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			featureConfig := redis.DefaultConfig()
			featureConfig.Address = config.Redis.Feature.Address
			featureConfig.BackoffFactory = config.BackoffFactory
			featureConfig.Instrumentor = config.Instrumentor
			featureConfig.Logger = config.Logger
			featureConfig.Prefix = config.Redis.Feature.Prefix
			featureService, err = redis.New(featureConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var generalService storage.Service
	{
		switch config.Kind {
		case KindMemory:
			generalConfig := memory.DefaultConfig()
			generalService, err = memory.New(generalConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			generalConfig := redis.DefaultConfig()
			generalConfig.Address = config.Redis.General.Address
			generalConfig.BackoffFactory = config.BackoffFactory
			generalConfig.Instrumentor = config.Instrumentor
			generalConfig.Logger = config.Logger
			generalConfig.Prefix = config.Redis.General.Prefix
			generalService, err = redis.New(generalConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var indexService storage.Service
	{
		switch config.Kind {
		case KindMemory:
			indexConfig := memory.DefaultConfig()
			indexService, err = memory.New(indexConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			indexConfig := redis.DefaultConfig()
			indexConfig.Address = config.Redis.Index.Address
			indexConfig.BackoffFactory = config.BackoffFactory
			indexConfig.Instrumentor = config.Instrumentor
			indexConfig.Logger = config.Logger
			indexConfig.Prefix = config.Redis.Index.Prefix
			indexService, err = redis.New(indexConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var peerService storage.Service
	{
		switch config.Kind {
		case KindMemory:
			peerConfig := memory.DefaultConfig()
			peerService, err = memory.New(peerConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			peerConfig := redis.DefaultConfig()
			peerConfig.Address = config.Redis.Peer.Address
			peerConfig.BackoffFactory = config.BackoffFactory
			peerConfig.Instrumentor = config.Instrumentor
			peerConfig.Logger = config.Logger
			peerConfig.Prefix = config.Redis.Peer.Prefix
			peerService, err = redis.New(peerConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var queueService storage.Service
	{
		switch config.Kind {
		case KindMemory:
			queueConfig := memory.DefaultConfig()
			queueService, err = memory.New(queueConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			queueConfig := redis.DefaultConfig()
			queueConfig.Address = config.Redis.Queue.Address
			queueConfig.BackoffFactory = config.BackoffFactory
			queueConfig.Instrumentor = config.Instrumentor
			queueConfig.Logger = config.Logger
			queueConfig.Prefix = config.Redis.Queue.Prefix
			queueService, err = redis.New(queueConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	newCollection := &Collection{
		// Internals.
		bootOnce:     sync.Once{},
		shutdownOnce: sync.Once{},

		// Public.
		List: []storage.Service{
			connectionService,
			featureService,
			generalService,
			indexService,
			peerService,
			queueService,
		},

		Connection: connectionService,
		Feature:    featureService,
		General:    generalService,
		Index:      indexService,
		Peer:       peerService,
		Queue:      queueService,
	}

	return newCollection, nil
}

// Collection is the object bundling all storages.
type Collection struct {
	// Internals.
	bootOnce     sync.Once
	shutdownOnce sync.Once

	// Public.
	List []storage.Service

	Connection storage.Service
	Feature    storage.Service
	General    storage.Service
	Index      storage.Service
	Peer       storage.Service
	Queue      storage.Service
}

func (c *Collection) Boot() {
	c.bootOnce.Do(func() {
		var wg sync.WaitGroup

		for _, s := range c.List {
			wg.Add(1)
			go func() {
				s.Boot()
				wg.Done()
			}()
		}

		wg.Wait()
	})
}

func (c *Collection) Shutdown() {
	c.shutdownOnce.Do(func() {
		var wg sync.WaitGroup

		for _, s := range c.List {
			wg.Add(1)
			go func() {
				s.Shutdown()
				wg.Done()
			}()
		}

		wg.Wait()
	})
}
