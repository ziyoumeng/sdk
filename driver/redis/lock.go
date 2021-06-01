package redis

import (
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	"github.com/pkg/errors"
	"time"
)

var LockScriptName = "lock"

func init() {
	AddScript(NewLuaScript(LockScriptName, lockScript, 1))
}

//简单分布式锁
//KEYS[1] 锁key
//ARGV[1] 随便设置
//ARGV[2] 超时时间；注意：业务必须在超时时间内执行完，释放锁
var lockScript = `
if (redis.call('setnx', KEYS[1], ARGV[1]) < 1)
then return 0;
end;
redis.call('expire', KEYS[1], tonumber(ARGV[2]));
return 1;
`

type Locker struct {
	key   string
	redis *Wrapper
}

func NewLocker(key string, wrap *Wrapper) *Locker {
	return &Locker{
		key:   key,
		redis: wrap,
	}
}

//value 随便填
func (l *Locker) Lock(value interface{}, seconds int) (lockSuccess bool, err error) {
	if value == nil {
		value = time.Now().Unix()
	}
	keys := []string{l.key}
	args := []interface{}{value, seconds}
	ret, err := l.redis.EvalSha(LockScriptName, keys, args)
	//log.Debugf("%+v,%+v",ret,err)
	return redigo.Bool(ret, err)
}

func (l *Locker) Unlock() error {
	return l.redis.Del(l.key)
}

//悲观锁策略
func (l *Locker) PessimisticLock(seconds int, doSomething func() error) error {
	var count int
	start := time.Now()
	defer func() {
		fmt.Printf("spend %s tryCount %d", time.Now().Sub(start), count)
	}()
	for {
		count++
		ok, err := l.Lock(nil, seconds)
		if err != nil {
			return errors.WithMessage(err, "lock")
		}

		if !ok {
			fmt.Printf("wait lock %d!", count)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		defer func() {
			err = l.Unlock()
			if err != nil {
				fmt.Printf("unlock failed:%s", err)
			}
		}()

		return doSomething()
	}

}
