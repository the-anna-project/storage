package redis

import (
	"fmt"
	"testing"

	"github.com/garyburd/redigo/redis"
)

func Test_Error_maskAnyf(t *testing.T) {
	testCases := []struct {
		InputError  error
		InputFormat string
		InputArgs   []interface{}
		Expected    error
	}{
		{
			InputError:  nil,
			InputFormat: "",
			InputArgs:   []interface{}{},
			Expected:    nil,
		},
		{
			InputError:  fmt.Errorf("foo"),
			InputFormat: "bar",
			InputArgs:   []interface{}{},
			Expected:    nil,
		},
		{
			InputError:  fmt.Errorf("foo"),
			InputFormat: "bar %s",
			InputArgs:   []interface{}{"baz"},
			Expected:    fmt.Errorf("foo: bar baz"),
		},
	}

	for i, testCase := range testCases {
		var output error
		if len(testCase.InputArgs) == 0 {
			output = maskAnyf(testCase.InputError, testCase.InputFormat)
		} else {
			output = maskAnyf(testCase.InputError, testCase.InputFormat, testCase.InputArgs...)
		}

		if testCase.Expected != nil && output.Error() != testCase.Expected.Error() {
			t.Fatal("case", i+1, "expected", testCase.Expected, "got", output)
		}
	}
}

func Test_Error_IsNotFound(t *testing.T) {
	if !IsNotFound(redis.ErrNil) {
		t.Fatal("expected", true, "got", false)
	}
	if !IsNotFound(maskAny(redis.ErrNil)) {
		t.Fatal("expected", true, "got", false)
	}

	if IsNotFound(nil) {
		t.Fatal("expected", false, "got", true)
	}
	if IsNotFound(invalidConfigError) {
		t.Fatal("expected", false, "got", true)
	}
}
