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
	Configuration RedisConfig
	Connection    RedisConfig
	Event         RedisConfig
	Feature       RedisConfig
	General       RedisConfig
	Index         RedisConfig
	Instrumentor  RedisConfig
	Peer          RedisConfig
}

// CollectionConfig represents the configuration used to create a new storage
// collection.
type CollectionConfig struct {
	// Dependencies.
	BackoffFactory         func() redis.Backoff
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
		loggerConfig := logger.DefaultServiceConfig()
		loggerService, err = logger.NewService(loggerConfig)
		if err != nil {
			panic(err)
		}
	}

	config := CollectionConfig{
		// Dependencies.
		BackoffFactory: func() redis.Backoff {
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

	var configurationService Service
	{
		switch config.Kind {
		case KindMemory:
			configurationConfig := memory.DefaultConfig()
			configurationService, err = memory.New(configurationConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		case KindRedis:
			configurationConfig := redis.DefaultConfig()
			configurationConfig.Address = config.Redis.Configuration.Address
			configurationConfig.BackoffFactory = config.BackoffFactory
			configurationConfig.InstrumentorCollection = config.InstrumentorCollection
			configurationConfig.LoggerService = config.LoggerService
			configurationConfig.Prefix = config.Redis.Configuration.Prefix
			configurationService, err = redis.New(configurationConfig)
			if err != nil {
				return nil, maskAny(err)
			}
		}
	}

	var connectionService Service
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

	var eventService Service
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

	var featureService Service
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

	var generalService Service
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

	var indexService Service
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

	var instrumentorService Service
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

	var peerService Service
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
		List: []Service{
			configurationService,
			connectionService,
			eventService,
			featureService,
			generalService,
			indexService,
			instrumentorService,
			peerService,
		},

		Configuration: configurationService,
		Connection:    connectionService,
		Event:         eventService,
		Feature:       featureService,
		General:       generalService,
		Index:         indexService,
		Instrumentor:  instrumentorService,
		Peer:          peerService,
	}

	return newCollection, nil
}

// Collection is the object bundling all storages.
type Collection struct {
	// Internals.
	bootOnce     sync.Once
	shutdownOnce sync.Once

	// Public.
	List []Service

	Configuration Service
	Connection    Service
	Event         Service
	Feature       Service
	General       Service
	Index         Service
	Instrumentor  Service
	Peer          Service
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
