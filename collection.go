// Package storage implements services to persist data. The storage collection
// bundles storage instances to pass them around more easily.
package storage

import (
	"sync"

	"github.com/cenk/backoff"
	"github.com/the-anna-project/instrumentor"
	"github.com/the-anna-project/logger"

	"github.com/the-anna-project/storage/memory"
	"github.com/the-anna-project/storage/redis"
	"github.com/the-anna-project/storage/spec"
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
	Connection   RedisConfig
	Event        RedisConfig
	Feature      RedisConfig
	General      RedisConfig
	Index        RedisConfig
	Instrumentor RedisConfig
	Peer         RedisConfig
}

// CollectionConfig represents the configuration used to create a new storage
// collection.
type CollectionConfig struct {
	// Dependencies.
	BackoffFactory         func() spec.Backoff
	InstrumentorCollection *instrumentor.Collection
	LoggerService          logger.Service

	// Settings.
	Kind  string
	Redis *Redis
}

// DefaultCollectionConfig provides a default configuration to create a new
// storage collection by best effort.
func DefaultCollectionConfig() CollectionConfig {
	var err error

	var instrumentorCollection *instrumentor.Collection
	{
		instrumentorConfig := instrumentor.DefaultCollectionConfig()
		instrumentorCollection, err = instrumentor.NewCollection(instrumentorConfig)
		if err != nil {
			panic(err)
		}
	}

	var loggerService logger.Service
	{
		loggerConfig := logger.DefaultConfig()
		loggerService, err = logger.New(loggerConfig)
		if err != nil {
			panic(err)
		}
	}

	config := CollectionConfig{
		// Dependencies.
		BackoffFactory: func() spec.Backoff {
			return &backoff.StopBackOff{}
		},
		InstrumentorCollection: instrumentorCollection,
		LoggerService:          loggerService,

		// Settings.
		Kind:  KindMemory,
		Redis: nil,
	}

	return config
}

// NewCollection creates a new configured storage Collection.
func NewCollection(config CollectionConfig) (*Collection, error) {
	// Dependencies.
	if config.BackoffFactory == nil {
		return nil, maskAnyf(invalidConfigError, "backoff factory must not be empty")
	}
	if config.InstrumentorCollection == nil {
		return nil, maskAnyf(invalidConfigError, "instrumentor collection must not be empty")
	}
	if config.LoggerService == nil {
		return nil, maskAnyf(invalidConfigError, "logger service must not be empty")
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

	var connectionService spec.Service
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
			connectionConfig.InstrumentorCollection = config.InstrumentorCollection
			connectionConfig.LoggerService = config.LoggerService
			connectionConfig.Prefix = config.Redis.Connection.Prefix
			connectionService, err = redis.New(connectionConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var eventService spec.Service
	{
		switch config.Kind {
		case KindMemory:
			eventConfig := memory.DefaultConfig()
			eventService, err = memory.New(eventConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			eventConfig := redis.DefaultConfig()
			eventConfig.Address = config.Redis.Event.Address
			eventConfig.BackoffFactory = config.BackoffFactory
			eventConfig.InstrumentorCollection = config.InstrumentorCollection
			eventConfig.LoggerService = config.LoggerService
			eventConfig.Prefix = config.Redis.Event.Prefix
			eventService, err = redis.New(eventConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var featureService spec.Service
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
			featureConfig.InstrumentorCollection = config.InstrumentorCollection
			featureConfig.LoggerService = config.LoggerService
			featureConfig.Prefix = config.Redis.Feature.Prefix
			featureService, err = redis.New(featureConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var generalService spec.Service
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
			generalConfig.InstrumentorCollection = config.InstrumentorCollection
			generalConfig.LoggerService = config.LoggerService
			generalConfig.Prefix = config.Redis.General.Prefix
			generalService, err = redis.New(generalConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var indexService spec.Service
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
			indexConfig.InstrumentorCollection = config.InstrumentorCollection
			indexConfig.LoggerService = config.LoggerService
			indexConfig.Prefix = config.Redis.Index.Prefix
			indexService, err = redis.New(indexConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var instrumentorService spec.Service
	{
		switch config.Kind {
		case KindMemory:
			instrumentorConfig := memory.DefaultConfig()
			instrumentorService, err = memory.New(instrumentorConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			instrumentorConfig := redis.DefaultConfig()
			instrumentorConfig.Address = config.Redis.Instrumentor.Address
			instrumentorConfig.BackoffFactory = config.BackoffFactory
			instrumentorConfig.InstrumentorCollection = config.InstrumentorCollection
			instrumentorConfig.LoggerService = config.LoggerService
			instrumentorConfig.Prefix = config.Redis.Instrumentor.Prefix
			instrumentorService, err = redis.New(instrumentorConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var peerService spec.Service
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
			peerConfig.InstrumentorCollection = config.InstrumentorCollection
			peerConfig.LoggerService = config.LoggerService
			peerConfig.Prefix = config.Redis.Peer.Prefix
			peerService, err = redis.New(peerConfig)
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
		List: []spec.Service{
			connectionService,
			eventService,
			featureService,
			generalService,
			indexService,
			instrumentorService,
			peerService,
		},

		Connection:   connectionService,
		Event:        eventService,
		Feature:      featureService,
		General:      generalService,
		Index:        indexService,
		Instrumentor: instrumentorService,
		Peer:         peerService,
	}

	return newCollection, nil
}

// Collection is the object bundling all storages.
type Collection struct {
	// Internals.
	bootOnce     sync.Once
	shutdownOnce sync.Once

	// Public.
	List []spec.Service

	Connection   spec.Service
	Event        spec.Service
	Feature      spec.Service
	General      spec.Service
	Index        spec.Service
	Instrumentor spec.Service
	Peer         spec.Service
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
