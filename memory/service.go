// Package memory implements a service to store data in memory. This can be used
// for development and testing.
package memory

import (
	"sync"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/cenk/backoff"
	"github.com/garyburd/redigo/redis"

	redisstorage "github.com/the-anna-project/storage/redis"
)

// Config represents the configuration used to create a new storage service.
type Config struct {
}

// DefaultConfig provides a default configuration to create a new storage
// service by best effort.
func DefaultConfig() Config {
	return Config{}
}

// New creates a new storage service. Therefore it manages an in-memory redis
// instance which can be shut down using the configured closer. This is used for
// local development.
func New(config Config) (*Service, error) {
	newService := &Service{
		// Internals.
		bootOnce:     sync.Once{},
		closer:       make(chan struct{}, 1),
		pool:         nil,
		prefix:       "memory",
		redis:        nil,
		shutdownOnce: sync.Once{},
	}

	return newService, nil
}

type Service struct {
	// Internals.
	bootOnce     sync.Once
	closer       chan struct{}
	pool         *redis.Pool
	prefix       string
	redis        *redisstorage.Service
	shutdownOnce sync.Once
}

func (s *Service) Boot() {
	s.bootOnce.Do(func() {
		addressChannel := make(chan string, 1)
		closer := make(chan struct{}, 1)
		redisAddress := ""

		go func() {
			s, err := miniredis.Run()
			if err != nil {
				panic(err)
			}
			addressChannel <- s.Addr()

			<-closer
			s.Close()
		}()
		select {
		case <-time.After(1 * time.Second):
			panic("starting miniredis timed out")
		case address := <-addressChannel:
			redisAddress = address
		}

		redisConfig := redisstorage.DefaultConfig()
		redisConfig.Address = redisAddress
		redisConfig.BackoffFactory = func() redisstorage.Backoff {
			return backoff.NewExponentialBackOff()
		}
		redisConfig.Prefix = s.prefix
		redisService, err := redisstorage.New(redisConfig)
		if err != nil {
			panic(err)
		}
		go redisService.Boot()

		s.closer = closer
		s.redis = redisService
	})
}

func (s *Service) Exists(key string) (bool, error) {
	result, err := s.redis.Exists(key)
	if err != nil {
		return false, maskAny(err)
	}

	return result, nil
}

func (s *Service) ExistsInScoredSet(key, element string) (bool, error) {
	result, err := s.redis.ExistsInScoredSet(key, element)
	if err != nil {
		return false, maskAny(err)
	}

	return result, nil
}

func (s *Service) Get(key string) (string, error) {
	result, err := s.redis.Get(key)
	if redisstorage.IsNotFound(err) {
		return "", maskAny(notFoundError)
	} else if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *Service) GetAllFromList(key string) ([]string, error) {
	result, err := s.redis.GetAllFromList(key)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetAllFromSet(key string) ([]string, error) {
	result, err := s.redis.GetAllFromSet(key)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetElementsByScore(key string, score float64, maxElements int) ([]string, error) {
	result, err := s.redis.GetElementsByScore(key, score, maxElements)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetHighestScoredElements(key string, maxElements int) ([]string, error) {
	result, err := s.redis.GetHighestScoredElements(key, maxElements)
	if redisstorage.IsInvalidExecution(err) {
		return nil, maskAny(invalidExecutionError)
	} else if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetRandom() (string, error) {
	result, err := s.redis.GetRandom()
	if redisstorage.IsNotFound(err) {
		return "", maskAny(notFoundError)
	} else if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *Service) GetRandomFromSet(key string) (string, error) {
	result, err := s.redis.GetRandomFromSet(key)
	if redisstorage.IsNotFound(err) {
		return "", maskAny(notFoundError)
	} else if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *Service) GetRandomFromScoredSet(key string) (string, error) {
	result, err := s.redis.GetRandomFromScoredSet(key)
	if redisstorage.IsNotFound(err) {
		return "", maskAny(notFoundError)
	} else if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *Service) GetScoreOfElement(key, element string) (float64, error) {
	result, err := s.redis.GetScoreOfElement(key, element)
	if redisstorage.IsNotFound(err) {
		return 0, maskAny(notFoundError)
	} else if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) GetStringMap(key string) (map[string]string, error) {
	result, err := s.redis.GetStringMap(key)
	if err != nil {
		return nil, maskAny(err)
	}

	return result, nil
}

func (s *Service) Increment(key string, n float64) (float64, error) {
	result, err := s.redis.Increment(key, n)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) IncrementScoredElement(key, element string, n float64) (float64, error) {
	result, err := s.redis.IncrementScoredElement(key, element, n)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) LengthOfList(key string) (int, error) {
	result, err := s.redis.LengthOfList(key)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) LengthOfScoredSet(key string) (int, error) {
	result, err := s.redis.LengthOfScoredSet(key)
	if err != nil {
		return 0, maskAny(err)
	}

	return result, nil
}

func (s *Service) PopFromList(key string) (string, error) {
	result, err := s.redis.PopFromList(key)
	if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *Service) PushToList(key string, element string) error {
	err := s.redis.PushToList(key, element)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) PushToSet(key string, element string) error {
	err := s.redis.PushToSet(key, element)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) Remove(key string) error {
	err := s.redis.Remove(key)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) RemoveFromList(key string, element string) error {
	err := s.redis.RemoveFromList(key, element)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) RemoveFromSet(key string, element string) error {
	err := s.redis.RemoveFromSet(key, element)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) RemoveScoredElement(key string, element string) error {
	err := s.redis.RemoveScoredElement(key, element)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) Set(key, value string) error {
	err := s.redis.Set(key, value)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) SetElementByScore(key, element string, score float64) error {
	err := s.redis.SetElementByScore(key, element, score)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) SetStringMap(key string, stringMap map[string]string) error {
	err := s.redis.SetStringMap(key, stringMap)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}

func (s *Service) TrimEndOfList(key string, maxElements int) error {
	err := s.redis.TrimEndOfList(key, maxElements)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) WalkKeys(glob string, closer <-chan struct{}, cb func(key string) error) error {
	err := s.redis.WalkKeys(glob, closer, cb)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) WalkScoredSet(key string, closer <-chan struct{}, cb func(element string, score float64) error) error {
	err := s.redis.WalkScoredSet(key, closer, cb)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *Service) WalkSet(key string, closer <-chan struct{}, cb func(element string) error) error {
	err := s.redis.WalkSet(key, closer, cb)
	if err != nil {
		return maskAny(err)
	}

	return nil
}
