package redis

import (
	"fmt"
	"strconv"
	"time"
)

func (s *Service) retryErrorLogger(err error, d time.Duration) {
	s.logger.Log("error", fmt.Sprintf("%#v", maskAny(err)))
}

func (s *Service) withPrefix(keys ...string) string {
	newKey := s.prefix

	for _, k := range keys {
		newKey += ":" + k
	}

	return newKey
}

func parseMultiBulkReply(reply []interface{}) (int64, []string, error) {
	cursor, err := strconv.ParseInt(string(reply[0].([]uint8)), 10, 64)
	if err != nil {
		return 0, nil, maskAny(err)
	}
	var values []string
	for _, v := range reply[1].([]interface{}) {
		values = append(values, string(v.([]uint8)))
	}

	return cursor, values, nil
}
