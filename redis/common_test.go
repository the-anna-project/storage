package redis

import (
	"strings"
	"testing"
)

// testLogger implements spec.RootLogger and is used to capture logger messages.
type testLogger struct {
	Args []interface{}
}

func (tl *testLogger) ArgsToString() string {
	args := ""
	for _, v := range tl.Args {
		if arg, ok := v.(error); ok {
			args += " " + arg.Error()
		}
		if arg, ok := v.(string); ok {
			args += " " + arg
		}
	}

	return args[1:]
}

func (tl *testLogger) Log(v ...interface{}) error {
	tl.Args = v
	return nil
}

func (tl *testLogger) ResetArgs() {
	tl.Args = []interface{}{}
}

func testNewLogger(t *testing.T) *testLogger {
	return &testLogger{Args: []interface{}{}}
}

func Test_RedisStorage_retryErrorLogger(t *testing.T) {
	logger := testNewLogger(t)

	storageConfig := DefaultConfig()
	storageConfig.Address = "127.0.0.1:6379"
	storageConfig.Logger = logger
	storageConfig.Prefix = "test-prefix"
	storageService, err := New(storageConfig)
	if err != nil {
		t.Fatal("expected", nil, "got", err)
	}

	storageService.(*service).retryErrorLogger(invalidConfigError, 0)
	result := logger.ArgsToString()

	if !strings.Contains(result, invalidConfigError.Error()) {
		t.Fatal("expected", invalidConfigError.Error(), "got", result)
	}
}

func Test_RedisStorage_withPrefix(t *testing.T) {
	storageConfig := DefaultConfig()
	storageConfig.Address = "127.0.0.1:6379"
	storageConfig.Prefix = "test-prefix"
	storageService, err := New(storageConfig)
	if err != nil {
		t.Fatal("expected", nil, "got", err)
	}

	expected := "test-prefix:my:test:key"
	newKey := storageService.(*service).withPrefix("my", "test", "key")
	if newKey != expected {
		t.Fatal("expected", expected, "got", newKey)
	}
}

func Test_RedisStorage_withPrefix_Empty(t *testing.T) {
	storageConfig := DefaultConfig()
	storageConfig.Address = "127.0.0.1:6379"
	storageConfig.Prefix = "test-prefix"
	storageService, err := New(storageConfig)
	if err != nil {
		t.Fatal("expected", nil, "got", err)
	}

	newKey := storageService.(*service).withPrefix()
	if newKey != "test-prefix" {
		t.Fatal("expected", "test-prefix", "got", newKey)
	}
}
