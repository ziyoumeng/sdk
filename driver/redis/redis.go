package redis

import (
	"fmt"
	"github.com/creasty/defaults"
	"github.com/garyburd/redigo/redis"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"log"
)

var (
	ErrRedisAddrIncorrect = errors.New("redis addr incorrect")
)

func newPool(conf Config) (*redigo.Pool, error) {
	if conf.RedisAddr == "" {
		return nil, ErrRedisAddrIncorrect
	}

	if err := defaults.Set(&conf); err != nil {
		return nil, errors.WithMessage(err, "set defaults")
	}


	pool := &redigo.Pool{
		Dial: func() (redigo.Conn, error) {
			c, err := redis.Dial("tcp", conf.RedisAddr)
			if err != nil {
				return nil, err
			}

			if _, err := c.Do("AUTH", conf.RedisPassword); err != nil {
				c.Close()
				return nil, err
			}
			//if _, err := c.Do("SELECT", db); err != nil {
			//  c.Close()
			//  return nil, err
			//}
			return c, nil
		},
	}
	return pool, nil
}

type Wrapper struct {
	pool      *redigo.Pool
	luaScript map[string]*LuaScript //脚本名=>LuaScript
	prefix    string
}

func NewWrapper(c Config, prefix string) (*Wrapper, error) {
	if prefix == "" {
		return nil, errors.New("empty prefix")
	}

	pool, err := newPool(c)
	if err != nil {
		return nil, errors.WithMessage(err, "newPool")
	}

	cache := &Wrapper{
		pool:   pool,
		prefix: prefix,
	}
	err = cache.batchLoadLuaScript(Scripts)
	if err != nil {
		return nil, errors.WithMessage(err, "batchLoadLuaScript")
	}

	return cache, nil
}

func (w *Wrapper) batchLoadLuaScript(scripts []*LuaScript) error {
	w.luaScript = make(map[string]*LuaScript)
	for _, script := range scripts {
		err := w.loadLuaScript(script)
		if err != nil {
			return err
		}
		w.luaScript[script.GetScriptName()] = script
	}
	return nil
}

func (w *Wrapper) loadLuaScript(script *LuaScript) (err error) {
	conn := w.pool.Get()
	defer func() {
		if err1 := conn.Close(); err1 != nil {
			log.Printf("err:%s", err)
		}
	}()

	val := redigo.NewScript(script.GetKeyCount(), script.GetScript())
	err = val.Load(conn)
	if err != nil {
		err = errors.WithMessage(err, script.GetScriptName())
		return
	}
	script.SetScriptSha(val.Hash())
	return
}

// 执行redis命令, 执行完成后连接自动放回连接池
func (w *Wrapper) ExecRedisCommand(command string, args ...interface{}) (interface{}, error) {
	conn := w.pool.Get()
	defer func() {
		if err1 := conn.Close(); err1 != nil {
			log.Printf("%s", err1)
		}
	}()

	//log.Debugf("====>%+v", args)
	return conn.Do(command, args...)
}

func (w *Wrapper) WithPrefix(key string) string {
	return fmt.Sprintf("%s:%s", w.prefix, key)
}

func (w *Wrapper) GetString(key string) (ret string, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = redigo.String(conn.Do("GET", w.WithPrefix(key)))
	})
	return
}

func (w *Wrapper) GetBytes(key string) (ret []byte, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = redigo.Bytes(conn.Do("GET", w.WithPrefix(key)))
	})
	return
}

func (w *Wrapper) Get(key string) (ret interface{}, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = conn.Do("GET", w.WithPrefix(key))
	})
	return
}

func (w *Wrapper) Set(key string, value interface{}) (err error) {
	w.Wrap(func(conn redigo.Conn) {
		_, err = conn.Do("SET", w.WithPrefix(key), value)
	})
	return
}

func (w *Wrapper) Del(key string) (err error) {
	w.Wrap(func(conn redigo.Conn) {
		_, err = conn.Do("DEL", w.WithPrefix(key))
	})
	return
}

func (w *Wrapper) SetEX(key string, seconds int64, value interface{}) (err error) {
	w.Wrap(func(conn redigo.Conn) {
		if seconds <= 0 {
			_, err = conn.Do("SET", w.WithPrefix(key), value)
		} else {
			_, err = conn.Do("SETEX", w.WithPrefix(key), seconds, value)
		}
	})
	return
}

func (w *Wrapper) Wrap(doSomething func(conn redigo.Conn)) {
	conn := w.pool.Get()
	defer func() {
		if err1 := conn.Close(); err1 != nil {
			log.Printf("%s", err1)
		}
	}()

	doSomething(conn)
}

func (w *Wrapper) SetNX(key string, value interface{}) (ok bool, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ok, err = redigo.Bool(conn.Do("SETNX", w.WithPrefix(key), value))
	})
	return
}

//pair = <score, value>
func (w *Wrapper) ZAdd(key string, pairs ...interface{}) (newAddNum int, err error) {
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return 0, errors.New("invalid pairs num")
	}
	var args []interface{}
	args = append(args, w.WithPrefix(key))
	args = append(args, pairs...)
	//log.Debugf("ZADD===>%+v",args)
	w.Wrap(func(conn redigo.Conn) {
		newAddNum, err = redigo.Int(conn.Do("ZADD", args...))
	})
	return
}

func (w *Wrapper) ZRank(key, member string) (rank int, err error) {
	w.Wrap(func(conn redigo.Conn) {
		rank, err = redigo.Int(conn.Do("ZRANK", w.WithPrefix(key), member))
	})
	return
}

func (w *Wrapper) ZRange(key string, start, stop int32, isWithScore bool) (ret interface{}, err error) {
	w.Wrap(func(conn redigo.Conn) {
		if isWithScore {
			ret, err = conn.Do("zrange", w.WithPrefix(key), start, stop, "WITHSCORES")
		} else {
			ret, err = conn.Do("zrange", w.WithPrefix(key), start, stop)
		}
	})
	return
}

func (w *Wrapper) ZScore(key, member string) (score int64, err error) {
	w.Wrap(func(conn redigo.Conn) {
		score, err = redigo.Int64(conn.Do("ZSCORE", w.WithPrefix(key), member))
	})
	return
}

func (w *Wrapper) ZCard(key string) (num int64, err error) {
	w.Wrap(func(conn redigo.Conn) {
		num, err = redigo.Int64(conn.Do("ZCARD", w.WithPrefix(key)))
	})
	return
}

func (w *Wrapper) ZRem(key string, members ...interface{}) (remNum int, err error) {
	if len(members) == 0 {
		return 0, errors.New("invalid members num")
	}
	var args []interface{}
	args = append(args, w.WithPrefix(key))
	args = append(args, members...)
	w.Wrap(func(conn redigo.Conn) {
		remNum, err = redigo.Int(conn.Do("ZREM", args...))
	})
	return
}

func (w *Wrapper) HSetBytes(key, field string, value []byte) (err error) {
	w.Wrap(func(conn redigo.Conn) {
		_, err = conn.Do("HSET", w.WithPrefix(key), field, value)
	})
	return
}

func (w *Wrapper) HGetBytes(key, field string) (value []byte, err error) {
	w.Wrap(func(conn redigo.Conn) {
		value, err = redigo.Bytes(conn.Do("HGET", w.WithPrefix(key), field))
	})
	return
}

func (w *Wrapper) HSetString(key, field, value string) (ret int, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = redigo.Int(conn.Do("HSET", w.WithPrefix(key), field, value))
	})
	return
}

func (w *Wrapper) LPushInt64(key string, value int64) (err error) {
	w.Wrap(func(conn redigo.Conn) {
		_, err = conn.Do("LPUSH", w.WithPrefix(key), value)
	})
	return
}

func (w *Wrapper) LRangeAllInt64(key string) (ret []int64, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = redigo.Int64s(conn.Do("LRANGE", w.WithPrefix(key), 0, -1))
	})
	return
}

func (w *Wrapper) HIncrby(key, field string, num int32) (err error) {
	w.Wrap(func(conn redigo.Conn) {
		_, err = redigo.Int(conn.Do("HINCRBY", w.WithPrefix(key), field, num))
	})
	return
}

func (w *Wrapper) HGetInt64(key, field string) (num int64, err error) {
	w.Wrap(func(conn redigo.Conn) {
		num, err = redigo.Int64(conn.Do("HGET", w.WithPrefix(key), field))
	})
	return
}

func (w *Wrapper) HGetString(key, field string) (ret string, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = redigo.String(conn.Do("HGET", w.WithPrefix(key), field))
	})
	return
}

//注意: 即使都不存在会,ret长度不为0,返回空字符串数组
func (w *Wrapper) HMGetString(key string, field ...interface{}) (ret []string, err error) {
	w.Wrap(func(conn redigo.Conn) {
		var args []interface{}
		args = append(args, w.WithPrefix(key))
		args = append(args, field...)
		ret, err = redigo.Strings(conn.Do("HMGET", args...))
	})
	return
}

func (w *Wrapper) HMGet(key string, field ...interface{}) (ret interface{}, err error) {
	w.Wrap(func(conn redigo.Conn) {
		var args []interface{}
		args = append(args, w.WithPrefix(key))
		args = append(args, field...)
		ret, err = conn.Do("HMGET", args...)
	})
	return
}

func (w *Wrapper) BRPOP(key string, timeout int64) (ret interface{}, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = conn.Do("BRPOP", w.WithPrefix(key), timeout)
	})
	return
}

func (w *Wrapper) HGetAllBytes(key string) (bytes [][]byte, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err := redigo.StringMap(conn.Do("HGETALL", w.WithPrefix(key)))
		if err != nil {
			return
		}
		for _, s := range ret {
			if len(s) <= 0 {
				continue
			}
			bytes = append(bytes, []byte(s))
		}
	})
	return
}

func (w *Wrapper) HGetAll(key string) (ret map[string]string, err error) {
	w.Wrap(func(conn redigo.Conn) {
		ret, err = redigo.StringMap(conn.Do("HGETALL", w.WithPrefix(key)))
		return
	})
	return
}

func (w *Wrapper) HDel(hKey string, field ...interface{}) (delNum int, err error) {
	w.Wrap(func(conn redigo.Conn) {
		var args []interface{}
		args = append(args, w.WithPrefix(hKey))
		args = append(args, field...)
		delNum, err = redigo.Int(conn.Do("HDEL", args...))
	})
	return
}

func (w *Wrapper) IncrBy(key string, val int) (num int, err error) {
	w.Wrap(func(conn redigo.Conn) {
		num, err = redigo.Int(conn.Do("incrby", w.WithPrefix(key), val))
	})
	return
}

func (w *Wrapper) Expire(key string, sec int32) (err error) {
	w.Wrap(func(conn redigo.Conn) {
		_, err = conn.Do("Expire", w.WithPrefix(key), sec)
	})
	return
}

func (w *Wrapper) Keys(pattern string) (keys []string, err error) {
	w.Wrap(func(conn redigo.Conn) {
		keys, err = redigo.Strings(conn.Do("KEYS", pattern))
	})
	return
}
