package consul

import (
	"github.com/pkg/errors"
	"strings"
)

type WithOption func(*option) error

// WithPrefix 通用前缀。当key不是/开头时，使用prefix做key的前缀
func WithPrefix(prefix string) WithOption {
	return func(o *option) error {
		if !strings.HasPrefix(prefix, "/") {
			return errors.New("has not keyPrefix /")
		}
		prefix = strings.TrimPrefix(prefix, "/")
		o.keyPrefix = prefix
		return nil
	}
}

type option struct {
	keyPrefix string
	address   string
}

func newOption(opts ...WithOption) (option, error) {
	var clientOpt option
	for _, setOption := range opts {
		err := setOption(&clientOpt)
		if err != nil {
			return option{}, errors.WithMessage(err, "setOption")
		}
	}
	return clientOpt, nil
}
