package util

import (
	"encoding/json"
	"github.com/pkg/errors"
	"time"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(bytes []byte) error {
	var durStr string
	err := json.Unmarshal(bytes, &durStr)
	if err != nil {
		return err
	}
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		return errors.WithMessage(err, "ParseDuration")
	}
	*d = Duration(dur)
	return nil
}

func (d *Duration) Milliseconds() int64 {
	return time.Duration(*d).Milliseconds()
}

func (d *Duration) String() string {
	return time.Duration(*d).String()
}
