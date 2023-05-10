package consul

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/pkg/errors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/zero-contrib/zrpc/registry/consul"
	"gopkg.in/yaml.v2"
	"reflect"
	"strings"
	"time"
)

type Client struct {
	address string
	option  option
	client  *api.Client
}

func NewClient(addr string, opts ...WithOption) (*Client, error) {
	clientOption, err := newOption(opts...)
	if err != nil {
		return nil, errors.WithMessage(err, "new option")
	}

	consulClient, err := api.NewClient(&api.Config{Address: addr})
	if err != nil {
		return nil, errors.WithMessage(err, "api.NewClient")
	}

	return &Client{
		client:  consulClient,
		option:  clientOption,
		address: addr,
	}, nil
}

func (c *Client) GetValue(key string) ([]byte, error) {
	return c.getValue(key)
}

func (c *Client) GetClient() *api.Client {
	return c.client
}

func (c *Client) GetJson(key string, obj interface{}) error {
	return c.getWithUnmarshal(key, obj, json.Unmarshal)
}

func (c *Client) GetYaml(key string, obj interface{}) error {
	return c.getWithUnmarshal(key, obj, yaml.Unmarshal)
}

// WatchJson watch到新值时会调用callback,callback可空
func (c *Client) WatchJson(key string, obj interface{}, callback func()) error {
	return c.watch(key, obj, callback, json.Unmarshal)
}

func (c *Client) WatchYaml(key string, obj interface{}, callback func()) error {
	return c.watch(key, obj, callback, yaml.Unmarshal)
}

// RegisterService 服务注册,并返回注销函数
//参考:来自https://github.com/zeromicro/zero-contrib/blob/main/zrpc/registry/consul/register.go
func (c *Client) RegisterService(serverIP string, port int, conf consul.Conf) (func(), error) {
	serviceID := fmt.Sprintf("%s-%s-%d", conf.Key, serverIP, port)
	sidField := logx.Field("serviceID", serviceID)

	if conf.TTL <= 0 {
		conf.TTL = 20
	}

	ttl := fmt.Sprintf("%ds", conf.TTL)
	expiredTTL := fmt.Sprintf("%ds", conf.TTL*3)
	reg := &api.AgentServiceRegistration{
		ID:      serviceID, // 服务节点的名称,唯一标识服务
		Name:    conf.Key,  // 服务名称
		Tags:    conf.Tag,  // tag，可以为空
		Meta:    conf.Meta, // meta， 可以为空
		Port:    port,      // 服务端口
		Address: serverIP,  // 服务 IP
		Checks: []*api.AgentServiceCheck{ // 健康检查
			{
				CheckID:                        serviceID, // 服务节点的名称
				TTL:                            ttl,       // 健康检查间隔,低于这个评率发送ttl会导致服务被标记不健康
				Status:                         "passing",
				DeregisterCriticalServiceAfter: expiredTTL, // 注销时间，相当于过期时间
			},
		},
	}

	client := c.GetClient()
	// 注册服务
	if err := client.Agent().ServiceRegister(reg); err != nil {
		return nil, fmt.Errorf("initial register service '%s' serverIP to consul error: %s", conf.Key, err.Error())
	}

	logx.Infow("register service successfully.", sidField)

	// 注册服务检测
	//check := api.AgentServiceCheck{TTL: "30s", Status: "passing", DeregisterCriticalServiceAfter: "90s"}
	//err := client.Agent().CheckRegister(&api.AgentCheckRegistration{ID: serviceID, Name: conf.Key, ServiceID: serviceID, AgentServiceCheck: check})
	//if err != nil {
	//	return nil, fmt.Errorf("initial register service Check to consul error: %s", err.Error())
	//}

	// routine to update ttl
	stopTicker := make(chan int)
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(conf.TTL) / 2) // 注意：小于注册的check ttl时间
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := client.Agent().UpdateTTL(serviceID, "", "passing")
				if err != nil {
					logx.Errorf("update ttl failed: %v", err.Error())
					if err := client.Agent().UpdateTTL(serviceID, "TTL expired!", "fail"); err != nil {
						logx.Errorf("fail ttl failed: %s", err)
					} else {
						logx.Infow("fail ttl successfully.", sidField)
					}
				} else {
					logx.Debug("update ttl successfully.")
				}
			case <-stopTicker:
				logx.Infow("exit update ttl.", sidField) // 不退出的话ticker会尝试重新注册
				return
			}
		}
	}()

	// consul deregister
	fn := proc.AddShutdownListener(func() {
		stopTicker <- 1
		err := client.Agent().ServiceDeregister(serviceID)
		if err != nil {
			logx.Errorw("deregister service failed ", sidField, logx.Field("err", err))
		} else {
			logx.Infow("deregistered service from consul server.", sidField)
		}
		//err = client.Agent().CheckDeregister(serviceID)
		//if err != nil {
		//	logx.Errorw("deregister service Check error.", logx.Field("err", err))
		//}
	})
	return fn, nil
}

type Unmarshal func(data []byte, v interface{}) error

func (c *Client) getWithUnmarshal(key string, obj interface{}, unmarshal Unmarshal) error {
	bytes, err := c.getValue(key)
	if err != nil {
		return errors.WithMessage(err, "getValue")
	}

	t := reflect.TypeOf(obj)
	if t.Kind() != reflect.Ptr {
		return errors.New("obj must be pointer")
	}

	err = unmarshal(bytes, obj)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("unmarshal bytes(%s)", string(bytes)))
	}
	return nil
}

func (c *Client) getKey(key string) string {
	if !strings.HasPrefix(key, "/") {
		fmt.Println(c)
		fmt.Println(c.option.keyPrefix, key)
		return fmt.Sprintf("%s/%s", c.option.keyPrefix, key)
	}
	return key
}

func (c *Client) getValue(key string) ([]byte, error) {
	pair, _, err := c.client.KV().Get(c.getKey(key), nil)
	if err != nil {
		return nil, errors.WithMessage(err, "kv.Get")
	}
	if pair == nil {
		return nil, errors.Errorf("consul has not key:%s", c.getKey(key))
	}
	return pair.Value, nil
}

func (c *Client) watch(key string, obj interface{}, callback func(), unmarshal Unmarshal) error {
	realKey := c.getKey(key)
	plan, err := watch.Parse(map[string]interface{}{
		"type": "key",
		"key":  realKey,
	})
	if err != nil {
		return errors.WithMessage(err, "watch.Parse")
	}

	err = c.getWithUnmarshal(key, obj, unmarshal)
	if err != nil {
		return errors.WithMessage(err, "getWithUnmarshal")
	}

	if callback != nil {
		callback()
	}

	rt := reflect.TypeOf(obj)
	plan.Handler = func(idx uint64, raw interface{}) {
		defer func() {
			//避免对外部造成影响
			if r := recover(); r != nil {
				logx.Errorf("recover panic: %v", r)
			}
		}()

		var value []byte
		if kv, ok := raw.(*api.KVPair); ok && kv != nil {
			value = kv.Value

			field := logx.Field("new value", string(value))
			//先在临时变量上修改, 没问题再设置
			tmp := reflect.New(rt.Elem()).Interface()
			if err := unmarshal(value, tmp); err != nil {
				logx.Errorw("watch unmarshal", field)
				return
			}

			reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(tmp).Elem())

			logx.Infof("consul watch value ok:%+v", obj)

			if callback != nil {
				callback()
			}
		} else {
			logx.Errorw("consul watch invalid raw",
				logx.Field("idx", idx),
				logx.Field("raw", raw))
		}
	}

	go func() {
		err = plan.Run(c.address)
		if err != nil {
			logx.Errorf("plan.Run %s", err)
		}
	}()
	return nil
}
