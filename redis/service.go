// Package redis implements a service to store data in redis. This can be used
// in production.
package redis

import (
	"strconv"
	"sync"

	"github.com/cenk/backoff"
	"github.com/garyburd/redigo/redis"

	"github.com/the-anna-project/instrumentor"
	memoryinstrumentor "github.com/the-anna-project/instrumentor/memory"
	"github.com/the-anna-project/logger"
	"github.com/the-anna-project/storage"
)

// Config represents the configuration used to create a new storage service.
type Config struct {
	// Dependencies.
	BackoffFactory func() storage.Backoff
	Logger         logger.Service
	Instrumentor   instrumentor.Service

	// Settings.
	Address string
	Pool    *redis.Pool
	Prefix  string
}

// DefaultConfig provides a default configuration to create a new storage
// service by best effort.
func DefaultConfig() Config {
	var err error

	var instrumentorService instrumentor.Service
	{
		instrumentorConfig := memoryinstrumentor.DefaultConfig()
		instrumentorService, err = memoryinstrumentor.New(instrumentorConfig)
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

	config := Config{
		// Dependencies.
		BackoffFactory: func() storage.Backoff {
			return &backoff.StopBackOff{}
		},
		Instrumentor: instrumentorService,
		Logger:       loggerService,

		// Settings.
		Address: "",
		Pool:    nil,
		Prefix:  "prefix",
	}

	return config
}

// New creates a new storage service.
func New(config Config) (storage.Service, error) {
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
	if config.Address == "" && config.Pool == nil {
		return nil, maskAnyf(invalidConfigError, "either address or pool must be given")
	}

	var pool *redis.Pool
	if config.Address == "" {
		pool = config.Pool
	}
	if config.Pool == nil {
		pool = NewPoolWithAddress(config.Address)
	}

	newService := &service{
		// Dependencies.
		backoffFactory: config.BackoffFactory,
		instrumentor:   config.Instrumentor,
		logger:         config.Logger,

		// Internals.
		bootOnce:     sync.Once{},
		closer:       make(chan struct{}, 1),
		pool:         pool,
		shutdownOnce: sync.Once{},

		// Settings.
		prefix: config.Prefix,
	}

	return newService, nil
}

type service struct {
	// Dependencies.
	backoffFactory func() storage.Backoff
	instrumentor   instrumentor.Service
	logger         logger.Service

	// Internals.
	bootOnce     sync.Once
	closer       chan struct{}
	pool         *redis.Pool
	shutdownOnce sync.Once

	// Settings.
	prefix string
}

func (s *service) Boot() {
	s.bootOnce.Do(func() {
		// Service specific boot logic goes here.
	})
}

func (s *service) Get(key string) (string, error) {
	s.logger.Log("func", "Get")

	errors := make(chan error, 1)

	var result string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.String(conn.Do("GET", s.withPrefix(key)))
		if IsNotFound(err) {
			// To return the not found error we need to break through the retrier.
			// Therefore we do not return the not found error here, but dispatch it to
			// the calling goroutine. Further we simply fall through and return nil to
			// finally stop the retrier.
			errors <- maskAny(err)
			return nil
		} else if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("Get", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return "", maskAny(err)
	}

	select {
	case err := <-errors:
		if err != nil {
			return "", maskAny(err)
		}
	default:
		// If there is no error, we simply fall through to return the result.
	}

	return result, nil
}

func (s *service) GetAllFromSet(key string) ([]string, error) {
	s.logger.Log("func", "GetAllFromSet")

	var result []string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		values, err := redis.Values(conn.Do("SMEMBERS", s.withPrefix(key)))
		if err != nil {
			return maskAny(err)
		}

		for _, v := range values {
			result = append(result, string(v.([]uint8)))
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("GetAllFromSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *service) GetElementsByScore(key string, score float64, maxElements int) ([]string, error) {
	s.logger.Log("func", "GetElementsByScore")

	var result []string
	var err error
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		result, err = redis.Strings(conn.Do("ZREVRANGEBYSCORE", s.withPrefix(key), score, score, "LIMIT", 0, maxElements))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err = backoff.RetryNotify(s.instrumentor.WrapFunc("GetElementsByScore", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *service) GetHighestScoredElements(key string, maxElements int) ([]string, error) {
	s.logger.Log("func", "GetHighestScoredElements")

	var result []string
	var err error
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		result, err = redis.Strings(conn.Do("ZREVRANGE", s.withPrefix(key), 0, maxElements, "WITHSCORES"))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err = backoff.RetryNotify(s.instrumentor.WrapFunc("GetHighestScoredElements", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *service) GetRandom() (string, error) {
	s.logger.Log("func", "GetRandom")

	var result string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.String(conn.Do("RANDOMKEY"))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("GetRandom", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *service) GetStringMap(key string) (map[string]string, error) {
	s.logger.Log("func", "GetStringMap")

	var result map[string]string
	var err error
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		result, err = redis.StringMap(conn.Do("HGETALL", s.withPrefix(key)))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err = backoff.RetryNotify(s.instrumentor.WrapFunc("GetStringMap", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *service) PopFromList(key string) (string, error) {
	s.logger.Log("func", "PopFromList")

	var result string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		strings, err := redis.Strings(conn.Do("BRPOP", s.withPrefix(key), 0))
		if err != nil {
			return maskAny(err)
		}
		if len(strings) != 2 {
			return maskAnyf(queryExecutionFailedError, "two elements must be returned")
		}
		result = strings[1]

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("PopFromList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *service) PushToList(key string, element string) error {
	s.logger.Log("func", "PushToList")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Int(conn.Do("LPUSH", s.withPrefix(key), element))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("PushToList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) PushToSet(key string, element string) error {
	s.logger.Log("func", "PushToSet")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Int(conn.Do("SADD", s.withPrefix(key), element))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("PushToSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Remove(key string) error {
	s.logger.Log("func", "Remove")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Int64(conn.Do("DEL", s.withPrefix(key)))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.Retry(s.instrumentor.WrapFunc("Remove", action), s.backoffFactory())
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) RemoveFromSet(key string, element string) error {
	s.logger.Log("func", "RemoveFromSet")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Int(conn.Do("SREM", s.withPrefix(key), element))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("RemoveFromSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) RemoveScoredElement(key string, element string) error {
	s.logger.Log("func", "RemoveScoredElement")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Int(conn.Do("ZREM", s.withPrefix(key), element))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("RemoveScoredElement", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Set(key, value string) error {
	s.logger.Log("func", "Set")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		reply, err := redis.String(conn.Do("SET", s.withPrefix(key), value))
		if err != nil {
			return maskAny(err)
		}

		if reply != "OK" {
			return maskAnyf(queryExecutionFailedError, "SET not executed correctly")
		}

		return nil
	}

	err := backoff.Retry(s.instrumentor.WrapFunc("Set", action), s.backoffFactory())
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) SetElementByScore(key, element string, score float64) error {
	s.logger.Log("func", "SetElementByScore")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Int(conn.Do("ZADD", s.withPrefix(key), score, element))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("SetElementByScore", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) SetStringMap(key string, stringMap map[string]string) error {
	s.logger.Log("func", "SetStringMap")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		reply, err := redis.String(conn.Do("HMSET", redis.Args{}.Add(s.withPrefix(key)).AddFlat(stringMap)...))
		if err != nil {
			return maskAny(err)
		}

		if reply != "OK" {
			return maskAnyf(queryExecutionFailedError, "HMSET not executed correctly")
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("SetStringMap", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Shutdown() {
	s.logger.Log("func", "Shutdown")

	s.shutdownOnce.Do(func() {
		s.pool.Close()
	})
}

func (s *service) WalkKeys(glob string, closer <-chan struct{}, cb func(key string) error) error {
	s.logger.Log("func", "WalkKeys")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var cursor int64

		// Start to scan the set until the cursor is 0 again. Note that we check for
		// the closer twice. At first we prevent scans in case the closer was
		// triggered directly, and second before each callback execution. That way
		// ending the walk immediately is guaranteed.
		for {
			select {
			case <-closer:
				return nil
			default:
			}

			reply, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", glob, "COUNT", 100))
			if err != nil {
				return maskAny(err)
			}

			cursor, values, err := parseMultiBulkReply(reply)
			if err != nil {
				return maskAny(err)
			}

			for _, v := range values {
				select {
				case <-closer:
					return nil
				default:
				}

				err := cb(v)
				if err != nil {
					return maskAny(err)
				}
			}

			if cursor == 0 {
				break
			}
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("WalkKeys", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) WalkScoredSet(key string, closer <-chan struct{}, cb func(element string, score float64) error) error {
	s.logger.Log("func", "WalkScoredSet")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var cursor int64

		// Start to scan the set until the cursor is 0 again. Note that we check for
		// the closer twice. At first we prevent scans in case the closer was
		// triggered directly, and second before each callback execution. That way
		// ending the walk immediately is guaranteed.
		for {
			select {
			case <-closer:
				return nil
			default:
			}

			reply, err := redis.Values(conn.Do("ZSCAN", s.withPrefix(key), cursor, "COUNT", 100))
			if err != nil {
				return maskAny(err)
			}

			cursor, values, err := parseMultiBulkReply(reply)
			if err != nil {
				return maskAny(err)
			}

			for i := range values {
				select {
				case <-closer:
					return nil
				default:
				}

				if i%2 != 0 {
					continue
				}

				score, err := strconv.ParseFloat(values[i+1], 64)
				if err != nil {
					return maskAny(err)
				}
				err = cb(values[i], score)
				if err != nil {
					return maskAny(err)
				}
			}

			if cursor == 0 {
				break
			}
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("WalkScoredSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) WalkSet(key string, closer <-chan struct{}, cb func(element string) error) error {
	s.logger.Log("func", "WalkSet")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var cursor int64

		// Start to scan the set until the cursor is 0 again. Note that we check for
		// the closer twice. At first we prevent scans in case the closer was
		// triggered directly, and second before each callback execution. That way
		// ending the walk immediately is guaranteed.
		for {
			select {
			case <-closer:
				return nil
			default:
			}

			reply, err := redis.Values(conn.Do("SSCAN", s.withPrefix(key), cursor, "COUNT", 100))
			if err != nil {
				return maskAny(err)
			}

			cursor, values, err := parseMultiBulkReply(reply)
			if err != nil {
				return maskAny(err)
			}

			for _, v := range values {
				select {
				case <-closer:
					return nil
				default:
				}

				err := cb(v)
				if err != nil {
					return maskAny(err)
				}
			}

			if cursor == 0 {
				break
			}
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.WrapFunc("WalkSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}
