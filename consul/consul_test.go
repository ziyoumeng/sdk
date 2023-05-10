package consul

import (
	"fmt"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/zero-contrib/zrpc/registry/consul"
	"testing"
	"time"
)

const (
	testAddr = "127.0.0.1:8500"
)

func getClient(t *testing.T) *Client {
	c, err := NewClient(testAddr, WithPrefix("/test/service/counter"))
	if err != nil {
		t.Fatalf("NewClient %s", err)
	}
	return c
}

func TestGetJson(t *testing.T) {
	var tmp = make(map[string]interface{})
	c := getClient(t)
	err := c.GetJson("tmp.json", &tmp)
	if err != nil {
		t.Errorf("GetJson %s", err)
	}
	if tmp["D"] != "5s" {
		t.Errorf("GetJson %s", tmp["D"])
	}

}

func TestGetYaml(t *testing.T) {
	tmp := make(map[string]interface{})

	c := getClient(t)
	err := c.GetYaml("tmp.yaml", &tmp)
	if err != nil {
		t.Errorf("GetYaml %s", err)
	}
	if tmp["D"] != "1s" {
		t.Errorf("GetYaml %s", tmp["D"])
	}

	var tp string
	err = c.GetYaml("tmp", &tp)
	if err != nil {
		t.Errorf("GetYaml %s", err)
	}
	if tp != "hello" {
		t.Errorf("not hello")
	}
}

func TestWatchJson(t *testing.T) {
	c := getClient(t)

	tmp := make(map[string]interface{})
	err := c.WatchJson("tmp.json", &tmp, func() {
		fmt.Println("watchJson", tmp)
	})
	if err != nil {
		t.Errorf("watchJson %s", err)
	}
	wait := make(chan struct{})
	<-wait
}

func TestWatchYaml(t *testing.T) {
	c := getClient(t)

	tmp := make(map[string]interface{})
	err := c.WatchYaml("tmp.yaml", &tmp, func() {
		fmt.Println("watchYaml", tmp)
	})
	if err != nil {
		t.Errorf("watchYaml %s", err)
	}

	tp := ""

	err = c.WatchYaml("tmp", &tp, func() {
		fmt.Println("watchYaml", tp)
	})
	if err != nil {
		t.Errorf("watchYaml %s", err)
	}

	wait := make(chan struct{})
	<-wait
}

func TestRegister(t *testing.T) {
	c := getClient(t)
	dreg, err := c.RegisterService("127.0.0.1", 80, consul.Conf{
		Host:  "127.0.0.1",
		Key:   "test",
		Token: "teat",
		Tag:   []string{"testTag"},
		Meta:  nil,
		TTL:   4,
	})
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer dreg()

	time.Sleep(10 * time.Second)
	proc.Shutdown()
}

type tmp struct {
	D      string
	Check  *check
	Check1 check
}

func (t tmp) Validate() error {
	fmt.Println(t.D)
	if t.D != "1s" {
		return fmt.Errorf("must be 1s")
	}
	return nil
}

type check struct {
	Check string
}

func (t check) Validate() error {
	if t.Check == "" {
		return fmt.Errorf("must be not empty")
	}
	return nil
}

func (t *tmp) Key() string {
	return "tmp.json"
}

func TestLoader(t *testing.T) {
	c := getClient(t)
	l := NewLoader(c)
	tp := tmp{}
	l.Watch(&tp, func() {
		fmt.Println("i'm watching.")
	})
	go func() {
		for err := range l.GetErrChan() {
			fmt.Println("errChan:", err)
		}
	}()
	select {}
}
