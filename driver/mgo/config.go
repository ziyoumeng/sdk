package mgo

import "time"

type Config struct {
	MgoAddrs          []string      `json:"MongodbAddr"`
	MgoUser           string        `json:"MongodbUser"`
	MgoPassword       string        `json:"MongodbPassword"`
	MgoPoolLimit      int           `json:"MongodbPoolLimit"`
	MgoReplicate      string        `json:"MongodbReplicaSet"`
	MgoAuthSource     string        `json:"MongodbAuthSource"`
	MgoConnectTimeout time.Duration `default:"3s"`
	MgoMaxConnIdle    time.Duration `default:"3s"`
}
