package storage

import (
	"fmt"

	"github.com/juju/errgo"

	"github.com/the-anna-project/storage/memory"
	"github.com/the-anna-project/storage/redis"
)

var (
	maskAny = errgo.MaskFunc(errgo.Any)
)

func maskAnyf(err error, f string, v ...interface{}) error {
	if err == nil {
		return nil
	}

	f = fmt.Sprintf("%s: %s", err.Error(), f)
	newErr := errgo.WithCausef(nil, errgo.Cause(err), f, v...)
	newErr.(*errgo.Err).SetLocation(1)

	return newErr
}

var invalidConfigError = errgo.New("invalid config")

// IsInvalidConfig asserts invalidConfigError.
func IsInvalidConfig(err error) bool {
	return errgo.Cause(err) == invalidConfigError
}

// IsInvalidExecution combines IsInvalidExecution error matchers of all storage
// implementations. IsInvalidExecution should thus be used for error handling
// wherever a storage service is used.
func IsInvalidExecution(err error) bool {
	return redis.IsInvalidExecution(err) || memory.IsInvalidExecution(err)
}

// IsNotFound combines IsNotFound error matchers of all storage implementations.
// IsNotFound should thus be used for error handling wherever a storage service
// is used.
func IsNotFound(err error) bool {
	return redis.IsNotFound(err) || memory.IsNotFound(err)
}
