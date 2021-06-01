package util

import "time"

type MillTime struct {
	t time.Time
}

func NewMillTime(t time.Time) *MillTime {
	return &MillTime{t: t}
}

func (m MillTime) TimeStamp() int64 {
	return toMillTimestamp(m.t)
}

func (m MillTime) Add(d time.Duration) int64 {
	return toMillTimestamp(m.t.Add(d))
}

func toMillTimestamp(t time.Time) int64 {
	return t.UnixNano() / 1e6
}

func MillTimestampOfNow()int64{
	return NewMillTime(time.Now()).TimeStamp()
}

