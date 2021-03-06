// Package redis implements a service to store data in redis. This can be used
// in production.
package redis

import (
	"strconv"
	"sync"

	"github.com/cenk/backoff"
	"github.com/garyburd/redigo/redis"
	"github.com/the-anna-project/instrumentor"
	"github.com/the-anna-project/logger"
	"github.com/the-anna-project/random"
)

// Config represents the configuration used to create a new storage service.
type Config struct {
	// Dependencies.
	BackoffFactory         func() Backoff
	InstrumentorCollection *instrumentor.Collection
	LoggerService          logger.Service
	RandomService          random.Service

	// Settings.
	Address string
	Pool    *redis.Pool
	Prefix  string
}

// DefaultConfig provides a default configuration to create a new storage
// service by best effort.
func DefaultConfig() Config {
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

	var randomService random.Service
	{
		randomConfig := random.DefaultServiceConfig()
		randomService, err = random.NewService(randomConfig)
		if err != nil {
			panic(err)
		}
	}

	config := Config{
		// Dependencies.
		BackoffFactory: func() Backoff {
			return &backoff.StopBackOff{}
		},
		InstrumentorCollection: instrumentorCollection,
		LoggerService:          loggerService,
		RandomService:          randomService,

		// Settings.
		Address: "",
		Pool:    nil,
		Prefix:  "prefix",
	}

	return config
}

// New creates a new storage service.
func New(config Config) (*Service, error) {
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
	if config.RandomService == nil {
		return nil, maskAnyf(invalidConfigError, "random service must not be empty")
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

	newService := &Service{
		// Dependencies.
		backoffFactory: config.BackoffFactory,
		instrumentor:   config.InstrumentorCollection,
		logger:         config.LoggerService,
		random:         config.RandomService,

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

type Service struct {
	// Dependencies.
	backoffFactory func() Backoff
	instrumentor   *instrumentor.Collection
	logger         logger.Service
	random         random.Service

	// Internals.
	bootOnce     sync.Once
	closer       chan struct{}
	pool         *redis.Pool
	shutdownOnce sync.Once

	// Settings.
	prefix string
}

func (s *Service) Boot() {
	s.bootOnce.Do(func() {
		// Service specific boot logic goes here.
	})
}

func (s *Service) Exists(key string) (bool, error) {
	var result bool
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.Bool(conn.Do("EXISTS", s.withPrefix(key)))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("Exists", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return false, maskAny(err)
	}

	return result, nil
}

func (s *Service) ExistsInScoredSet(key, element string) (bool, error) {
	var result bool
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Float64(conn.Do("ZSCORE", s.withPrefix(key), element))
		if IsNotFound(err) {
			// In this case we receive a not found error. This error is a "redigo
			// returned nil", which means that redis returned nil. Here we now know
			// the element does not exist within the scored set.
			return nil
		} else if err != nil {
			return maskAny(err)
		}

		result = true

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("ExistsInScoredSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return false, maskAny(err)
	}

	return result, nil
}

func (s *Service) Get(key string) (string, error) {
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
			errors <- maskAny(notFoundError)
			return nil
		} else if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("Get", action), s.backoffFactory(), s.retryErrorLogger)
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

func (s *Service) GetAllFromList(key string) ([]string, error) {
	var result []string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		values, err := redis.Values(conn.Do("LRANGE", s.withPrefix(key), 0, -1))
		if err != nil {
			return maskAny(err)
		}

		for _, v := range values {
			result = append(result, string(v.([]uint8)))
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetAllFromList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetAllFromSet(key string) ([]string, error) {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetAllFromSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetElementsByScore(key string, score float64, maxElements int) ([]string, error) {
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

	err = backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetElementsByScore", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetHighestScoredElements(key string, maxElements int) ([]string, error) {
	s.logger.Log("func", "GetHighestScoredElements")

	if maxElements == 0 {
		return nil, maskAnyf(invalidExecutionError, "max elements must not be 0")
	}

	var result []string
	var err error
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		if maxElements > 0 {
			// Redis interprets the boundaries as inclusive numbers. We want to have
			// absolut numbers, because the second argument provided is about the
			// maximum number of elements. In case you want to have 1 element,
			// providing zero in this context would not make sense. Therefore we
			// decrement all numbers that are greater than zero.
			maxElements--
		}

		result, err = redis.Strings(conn.Do("ZREVRANGE", s.withPrefix(key), 0, maxElements, "WITHSCORES"))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err = backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetHighestScoredElements", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetRandom() (string, error) {
	errors := make(chan error, 1)

	var result string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.String(conn.Do("RANDOMKEY"))
		if IsNotFound(err) {
			// To return the not found error we need to break through the retrier.
			// Therefore we do not return the not found error here, but dispatch it to
			// the calling goroutine. Further we simply fall through and return nil to
			// finally stop the retrier.
			errors <- maskAny(notFoundError)
			return nil
		} else if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetRandom", action), s.backoffFactory(), s.retryErrorLogger)
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

func (s *Service) GetRandomFromScoredSet(key string) (string, error) {
	var result string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		length, err := s.LengthOfScoredSet(key)
		if err != nil {
			return maskAny(err)
		}

		random, err := s.random.CreateMax(length + 1)
		if err != nil {
			return maskAny(err)
		}
		index := random - 1

		result, err = redis.String(conn.Do("ZRANGE", s.withPrefix(key), index, index))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetRandomFromScoredSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return "", maskAny(err)
	}

	if result == "" {
		return "", maskAny(notFoundError)
	}

	return result, nil
}

func (s *Service) GetRandomFromSet(key string) (string, error) {
	errors := make(chan error, 1)

	var result string
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.String(conn.Do("SRANDMEMBER", s.withPrefix(key)))
		if IsNotFound(err) {
			// To return the not found error we need to break through the retrier.
			// Therefore we do not return the not found error here, but dispatch it to
			// the calling goroutine. Further we simply fall through and return nil to
			// finally stop the retrier.
			errors <- maskAny(notFoundError)
			return nil
		} else if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetRandomFromSet", action), s.backoffFactory(), s.retryErrorLogger)
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

func (s *Service) GetScoreOfElement(key, element string) (float64, error) {
	errors := make(chan error, 1)

	var result float64
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.Float64(conn.Do("ZSCORE", s.withPrefix(key), element))
		if IsNotFound(err) {
			// To return the not found error we need to break through the retrier.
			// Therefore we do not return the not found error here, but dispatch it to
			// the calling goroutine. Further we simply fall through and return nil to
			// finally stop the retrier.
			errors <- maskAny(notFoundError)
			return nil
		} else if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetScoreOfElement", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return 0, maskAny(err)
	}

	select {
	case err := <-errors:
		if err != nil {
			return 0, maskAny(err)
		}
	default:
		// If there is no error, we simply fall through to return the result.
	}

	return result, nil
}

func (s *Service) GetStringMap(key string) (map[string]string, error) {
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

	err = backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("GetStringMap", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) Increment(key string, n float64) (float64, error) {
	var result float64
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.Float64(conn.Do("INCRBYFLOAT", s.withPrefix(key), n))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("Increment", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) IncrementScoredElement(key, element string, n float64) (float64, error) {
	var result float64
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.Float64(conn.Do("ZINCRBY", s.withPrefix(key), element, n))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("IncrementScoredElement", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) LengthOfList(key string) (int, error) {
	var result int
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.Int(conn.Do("LLEN", s.withPrefix(key)))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("LengthOfList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) LengthOfScoredSet(key string) (int, error) {
	var result int
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		var err error
		result, err = redis.Int(conn.Do("ZCARD", s.withPrefix(key)))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("LengthOfScoredSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) PopFromList(key string) (string, error) {
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
			return maskAnyf(executionFailedError, "two elements must be returned")
		}
		result = strings[1]

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("PopFromList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *Service) PushToList(key string, element string) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("PushToList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) PushToSet(key string, element string) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("PushToSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) Remove(key string) error {
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

	err := backoff.Retry(s.instrumentor.Publisher.WrapFunc("Remove", action), s.backoffFactory())
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) RemoveFromList(key string, element string) error {
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		_, err := redis.Int(conn.Do("LREM", s.withPrefix(key), 0, element))
		if err != nil {
			return maskAny(err)
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("RemoveFromList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) RemoveFromSet(key string, element string) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("RemoveFromSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) RemoveScoredElement(key string, element string) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("RemoveScoredElement", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) Set(key, value string) error {
	s.logger.Log("func", "Set")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		reply, err := redis.String(conn.Do("SET", s.withPrefix(key), value))
		if err != nil {
			return maskAny(err)
		}

		if reply != "OK" {
			return maskAnyf(executionFailedError, "SET not executed correctly")
		}

		return nil
	}

	err := backoff.Retry(s.instrumentor.Publisher.WrapFunc("Set", action), s.backoffFactory())
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) SetElementByScore(key, element string, score float64) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("SetElementByScore", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) SetStringMap(key string, stringMap map[string]string) error {
	s.logger.Log("func", "SetStringMap")

	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		reply, err := redis.String(conn.Do("HMSET", redis.Args{}.Add(s.withPrefix(key)).AddFlat(stringMap)...))
		if err != nil {
			return maskAny(err)
		}

		if reply != "OK" {
			return maskAnyf(executionFailedError, "HMSET not executed correctly")
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("SetStringMap", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) Shutdown() {
	s.logger.Log("func", "Shutdown")

	s.shutdownOnce.Do(func() {
		s.pool.Close()
	})
}

func (s *Service) TrimEndOfList(key string, maxElements int) error {
	action := func() error {
		conn := s.pool.Get()
		defer conn.Close()

		reply, err := redis.String(conn.Do("LTRIM", s.withPrefix(key), 0, maxElements-1))
		if err != nil {
			return maskAny(err)
		}

		if reply != "OK" {
			return maskAnyf(executionFailedError, "LTRIM not executed correctly")
		}

		return nil
	}

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("TrimEndOfList", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) WalkKeys(glob string, closer <-chan struct{}, cb func(key string) error) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("WalkKeys", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) WalkScoredSet(key string, closer <-chan struct{}, cb func(element string, score float64) error) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("WalkScoredSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) WalkSet(key string, closer <-chan struct{}, cb func(element string) error) error {
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

	err := backoff.RetryNotify(s.instrumentor.Publisher.WrapFunc("WalkSet", action), s.backoffFactory(), s.retryErrorLogger)
	if err != nil {
		return maskAny(err)
	}

	return nil
}
