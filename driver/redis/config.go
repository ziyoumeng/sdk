package redis

import "time"

type Config struct {
	RedisAddr         string
	RedisPassword     string
	RedisReadTimeout  time.Duration `default:"3s"`
	RedisWriteTimeout time.Duration `default:"3s"`
	RedisMaxIdl       int           `default:"128"`
}
