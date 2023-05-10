package consul

import (
	"fmt"
	"github.com/creasty/defaults"
	"github.com/go-playground/validator"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
	"reflect"
	"strings"
	"sync"
)

type Loader struct {
	*sync.RWMutex
	data           map[string]Conf
	client         *Client
	errChan        chan error
	updateCallback func(val Conf)
}

type Conf interface {
	Key() string
}

func NewLoader(client *Client) *Loader {
	loader := &Loader{
		RWMutex: &sync.RWMutex{},
		data:    make(map[string]Conf),
		errChan: make(chan error, 100),
		client:  client,
	}
	return loader
}

// 设置更新配置时的回调方法
func (l *Loader) SetUpdateCallback(updateCallback func(val Conf)) {
	l.Lock()
	defer l.Unlock()
	l.updateCallback = updateCallback
}

// callback是watch到新对象时的回调方法
func (l *Loader) Watch(conf Conf, callback func()) {
	key := conf.Key()
	var watchFunc func(key string, obj interface{}, callback func()) error
	if strings.HasSuffix(key, ".yaml") {
		watchFunc = l.client.WatchYaml
	} else {
		watchFunc = l.client.WatchJson
	}
	err := watchFunc(conf.Key(), conf, func() {
		l.add(conf)
		if callback != nil {
			callback()
		}
	})
	if err != nil {
		panic(err)
	}
}

func (l *Loader) add(val Conf) {
	fmt.Println("adding")
	err := checkValidate(val)
	if err != nil {
		l.sendErrChan(errors.WithMessage(err, "loader.store failed"))
		return
	}

	l.RLock()
	updateCallback := l.updateCallback
	l.RUnlock()

	if updateCallback != nil {
		updateCallback(val)
	}

	l.Lock()
	defer l.Unlock()
	l.data[val.Key()] = val
	return
}

func (l *Loader) sendErrChan(err error) {
	select {
	case l.errChan <- err:
	default:
		logx.Errorf("sendErrChan chan full,errChan:%s", err)
	}
}

// 获取错误消息
func (l *Loader) GetErrChan() <-chan error {
	return l.errChan
}

func (l *Loader) Get(k string) Conf {
	l.RLock()
	defer l.RUnlock()
	conf := l.data[k]
	return conf
}

func checkValidate(r interface{}) error {
	tp := reflect.TypeOf(r)
	if tp.Kind() != reflect.Ptr {
		return errors.Errorf("must be a pointer")
	}

	// 设置默认值
	if err := defaults.Set(r); err != nil {
		return errors.WithMessage(err, "defaults.Set")
	}

	// 使用插件校验
	validate := validator.New()
	err := validate.Struct(r)
	if err != nil {
		return errors.WithMessage(err, "validate")
	}

	// 校验自定义的validate
	if val, ok := r.(Validate); ok {
		err = val.Validate()
		if err != nil {
			return errors.WithMessage(err, "validate")
		}
	}

	// 递归struct的field上的自定义Validate
	err = validateStruct(r)
	return errors.WithMessage(err, "validateStruct")
}

func validateStruct(r interface{}) error {
	fmt.Println(" validateStruct", r)
	val := reflect.ValueOf(r)
	switch val.Kind() {
	case reflect.Struct:
	case reflect.Ptr:
		if !val.IsNil() {
			val = val.Elem()
		}
	default:
		return nil
	}

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		tField := field.Type()
		fmt.Println("field:", tField.Name(), field.String())
		fieldObj, ok := field.Interface().(Validate)
		if ok {
			if tField.Kind() == reflect.Ptr && field.IsNil() {
				continue
			}
			err := fieldObj.Validate()
			if err != nil {
				return errors.WithMessage(err, tField.Name())
			}
		}

		// 递归处理
		if tField.Kind() == reflect.Struct || tField.Kind() == reflect.Ptr {
			err := validateStruct(field.Interface())
			if err != nil {
				return errors.WithMessage(err, "validateStruct")
			}
		}
	}
	return nil
}

// 不要用指针对象实现
type Validate interface {
	Validate() error
}
