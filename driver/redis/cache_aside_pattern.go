package redis

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/ziyoumeng/sdk/util"
)

type Persist interface {
	Get(id interface{}) (CacheValue, error)
	Save(value CacheValue) (interface{}, error)
	Del(id interface{}) error
	GetCachePrefix() string //不要带:
	GetEmpty() CacheValue
}

type CacheValue interface {
	GetID() interface{}
	ExpiredSecond() int64
	SetID(id interface{})
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type CacheAsidePattern struct {
	persist Persist
	cache   *Wrapper
}

func NewCacheAsidePattern(cache *Wrapper, persist Persist) CacheAsidePattern {
	return CacheAsidePattern{
		persist: persist,
		cache:   cache,
	}
}

func (c CacheAsidePattern) Get(id interface{}) (interface{}, error) {
	if id == nil {
		return nil, errors.New("id is nil")
	}

	key, err := c.genKey(id)
	if err != nil {
		return nil, errors.WithMessage(err, "genKey")
	}

	val, err := c.cache.Get(key)
	if err != nil {
		return nil, errors.WithMessage(err, "cache.Get")
	}

	if val != nil {
		bytes, ok := val.([]byte)
		if !ok {
			return nil, errors.New("not []byte")
		}

		cacheVal := c.persist.GetEmpty()
		err = cacheVal.Unmarshal(bytes)
		if err != nil {
			return nil, errors.WithMessage(err, "cacheVal.Unmarshal")
		}
		return cacheVal, nil
	}

	cacheVal, err := c.persist.Get(id)
	if err != nil {
		return nil, errors.WithMessage(err, "persist.Get")
	}

	if cacheVal == nil {
		return nil, nil
	}

	bytes, err := cacheVal.Marshal()
	if err != nil {
		return nil, errors.WithMessage(err, "Marshal")
	}

	err = c.cache.SetEX(key, cacheVal.ExpiredSecond(), bytes)
	return cacheVal, errors.WithMessage(err, "cache.setEX")
}

func (c CacheAsidePattern) Set(value CacheValue) error {
	newID, err := c.persist.Save(value)
	if err != nil {
		return errors.WithMessage(err, "persist.Set")
	}

	if value.GetID() == nil {
		value.SetID(newID)
		return nil
	} else {
		key, err := c.genKey(value.GetID())
		if err != nil {
			return errors.WithMessage(err, "genKey")
		}

		err = c.cache.Del(key)
		return errors.WithMessage(err, "cache.Del")
	}
}

func (c CacheAsidePattern) genKey(id interface{}) (string, error) {
	prefix := c.persist.GetCachePrefix()
	if prefix == "" {
		return "", errors.New("empty prefix")
	}

	return fmt.Sprintf("%s:%s", prefix, util.ToStr(id)), nil
}

func (c CacheAsidePattern) Del(id interface{}) error {
	err := c.persist.Del(id)
	if err != nil {
		return errors.WithMessage(err, "persist.Del")
	}

	key, err := c.genKey(id)
	if err != nil {
		return errors.WithMessage(err, "genKey")
	}

	err = c.cache.Del(key)
	return errors.WithMessage(err, "cache.Del")
}
