package mgo

import (
	"context"
	"github.com/creasty/defaults"
	"github.com/pkg/errors"
	"github.com/ziyoumeng/sdk/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

const (
	CtxTimeout = 3 * time.Second
)

type MgoDB struct {
	Session *mongo.Client
	DBName  string
}

func NewMongoDB(cfg Config, dbName string) (*MgoDB, error) {
	client, err := connect(cfg)
	if err != nil {
		return nil, err
	}

	return &MgoDB{
		Session: client,
		DBName:  dbName,
	}, nil
}

func (m *MgoDB) TimeoutCtx() (context.Context, context.CancelFunc) {
	return timeoutCtx()
}

func connect(cfg Config) (client *mongo.Client, err error) {
	if err := defaults.Set(&cfg); err != nil {
		return nil, errors.WithMessage(err, "set defaults")
	}

	ctx, cancel := context.WithTimeout(context.Background(), CtxTimeout)
	defer cancel()
	clientOptions := options.Client().
		SetHosts(cfg.MgoAddrs).
		SetReplicaSet(cfg.MgoReplicate).
		SetMaxPoolSize(uint64(cfg.MgoPoolLimit)).
		SetConnectTimeout(cfg.MgoConnectTimeout).
		SetMaxConnIdleTime(cfg.MgoMaxConnIdle)

	if cfg.MgoUser != "" {
		clientOptions.Auth = &options.Credential{
			Username:   cfg.MgoUser,
			Password:   cfg.MgoPassword,
			AuthSource: cfg.MgoAuthSource,
		}
	}

	client, err = mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, errors.WithMessage(err, "connect")
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, errors.WithMessage(err, "mongo.client.Ping")
	}
	return client, nil
}

func timeoutCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), CtxTimeout)
}

func (m *MgoDB) tableCollection(tableName string) *mongo.Collection {
	collection := m.Session.Database(m.DBName).Collection(tableName)
	return collection
}

func (m *MgoDB) InsertOne(c string, docs interface{}) (interface{}, error) {
	coll := m.tableCollection(c)
	ctx, cancel := timeoutCtx()
	defer cancel()
	result, err := coll.InsertOne(ctx, docs)
	if err != nil {
		return nil, err
	}
	return result.InsertedID, nil
}

func (m *MgoDB) InsertMany(c string, docs []interface{}) (*mongo.InsertManyResult, error) {
	coll := m.tableCollection(c)
	ctx, cancel := timeoutCtx()
	defer cancel()

	return coll.InsertMany(ctx, docs)
}

func (m *MgoDB) UpdateMany(c string, query interface{}, update interface{}) (*mongo.UpdateResult, error) {
	coll := m.tableCollection(c)
	ctx, cancel := timeoutCtx()
	defer cancel()
	return coll.UpdateMany(ctx, query, update)
}

func (m *MgoDB) UpdateOne(c string, query interface{}, update interface{}) (*mongo.UpdateResult, error) {
	coll := m.tableCollection(c)
	ctx, cancel := timeoutCtx()
	defer cancel()
	return coll.UpdateOne(ctx, query, update)
}

func (m *MgoDB) ReplaceOne(c string, query interface{}, update interface{}) (*mongo.UpdateResult, error) {
	coll := m.tableCollection(c)
	ctx, cancel := timeoutCtx()
	defer cancel()
	return coll.ReplaceOne(ctx, query, update)
}

func (m *MgoDB) UpsertOne(c string, query interface{}, update interface{}) (*mongo.UpdateResult, error) {
	coll := m.tableCollection(c)
	opt := options.Update().SetUpsert(true)
	ctx, cancel := timeoutCtx()
	defer cancel()
	return coll.UpdateOne(ctx, query, update, opt)
}

func (m *MgoDB) UpsertMany(c string, query interface{}, update interface{}) (*mongo.UpdateResult, error) {
	coll := m.tableCollection(c)
	opt := options.Update().SetUpsert(true)
	ctx, cancel := timeoutCtx()
	defer cancel()
	return coll.UpdateMany(ctx, query, update, opt)
}

func (m *MgoDB) One(c string, query interface{}, sort bson.D, ret interface{}) error {
	coll := m.tableCollection(c)
	opt := options.FindOne()
	if sort != nil {
		opt.SetSort(sort)
	}
	ctx, cancel := timeoutCtx()
	defer cancel()
	err := coll.FindOne(ctx, query, opt).Decode(ret)
	return err
}

func (m *MgoDB) FindOne(c string, query interface{}, ret interface{}) error {
	coll := m.tableCollection(c)
	ctx, cancel := timeoutCtx()
	defer cancel()
	err := coll.FindOne(ctx, query).Decode(ret)
	return err
}

func (m *MgoDB) All(c string, query interface{}, sort bson.D, rets interface{}) error {
	coll := m.tableCollection(c)
	opt := options.Find()
	if sort != nil {
		opt.SetSort(sort)
	}
	ctx, cancel := timeoutCtx()
	defer cancel()
	cursor, err := coll.Find(ctx, query, opt)
	if err != nil {
		return err
	}
	err = cursor.All(ctx, rets)
	return err
}

func (m *MgoDB) BatchGet(c string, query interface{}, sort bson.D, rets interface{}) error {
	coll := m.tableCollection(c)
	opt := options.Find()
	if sort != nil {
		opt.SetSort(sort)
	}
	ctx, cancel := timeoutCtx()
	defer cancel()
	cursor, err := coll.Find(ctx, query, opt)
	if err != nil {
		return err
	}
	err = cursor.All(ctx, rets)
	return err
}

func (m *MgoDB) Count(c string, query interface{}) (int64, error) {
	coll := m.tableCollection(c)
	opt := options.Count()
	ctx, cancel := timeoutCtx()
	defer cancel()
	cnt, err := coll.CountDocuments(ctx, query, opt)
	return cnt, err
}

func (m *MgoDB) Aggregate(c string, pipeline interface{}, rets interface{}) error {
	coll := m.tableCollection(c)
	opt := options.Aggregate()
	ctx, cancel := timeoutCtx()
	defer cancel()
	cursor, err := coll.Aggregate(ctx, pipeline, opt)
	if err != nil {
		return err
	}
	err = cursor.All(ctx, rets)
	return err
}

func (m *MgoDB) Distinct(c string, fieldName string, filter interface{}) ([]interface{}, error) {
	coll := m.tableCollection(c)
	opt := options.Distinct()
	ctx, cancel := timeoutCtx()
	defer cancel()
	ret, err := coll.Distinct(ctx, fieldName, filter, opt)
	return ret, err
}

func (m *MgoDB) List(c string, query interface{}, sortFields bson.D, offset, limit int64, docs interface{}) error {
	coll := m.tableCollection(c)

	opt := options.Find().SetSkip(offset).SetLimit(limit)
	if sortFields != nil {
		opt.SetSort(sortFields)
	}
	ctx, cancel := timeoutCtx()
	defer cancel()
	cursor, err := coll.Find(ctx, query, opt)
	if err != nil {
		return err
	}
	err = cursor.All(ctx, docs)
	return err
}

func (m *MgoDB) DeleteMany(c string, query interface{}) (*mongo.DeleteResult, error) {
	coll := m.tableCollection(c)

	ctx, cancel := timeoutCtx()
	defer cancel()
	return coll.DeleteMany(ctx, query)
}

func (m *MgoDB) DeleteOne(c string, query interface{}) (*mongo.DeleteResult, error) {
	coll := m.tableCollection(c)

	ctx, cancel := timeoutCtx()
	defer cancel()
	return coll.DeleteOne(ctx, query)
}

func (m *MgoDB) Page(c string, filter interface{}, sort bson.D, page, pageSize int, docs interface{}) (err error) {
	coll := m.tableCollection(c)
	offset := (page - 1) * pageSize
	opts := options.Find().SetLimit(int64(pageSize)).SetSkip(int64(offset))
	if len(sort) != 0 {
		opts.SetSort(sort)
	}
	ctx, cancel := timeoutCtx()
	defer cancel()
	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return errors.WithMessage(err, "Find")
	}
	ctx, cancel = timeoutCtx()
	defer cancel()
	err = cursor.All(ctx, docs)
	return errors.WithMessage(err, "All")
}

type DocTimestamp interface {
	SetUpdatedAt(int64)
	SetCreatedAt(int64)
	GetID() interface{}
	SetID(id interface{})
}

//doc 必须实现DocTimestamp接口,如果id为空，默认使用primitive.NewObjectID().Hex()作为id
func (m *MgoDB) SaveOne(c string, doc DocTimestamp) (interface{}, error) {
	now := time.Now().Unix()
	id := doc.GetID()

	doc.SetUpdatedAt(now)

	if !util.IsEmpty(id) {
		query := bson.M{
			"_id": id,
		}

		result, err := m.ReplaceOne(c, query, doc)
		if err != nil {
			return nil, errors.WithMessage(err, "updateOne")
		}
		if result.MatchedCount == 0 {
			doc.SetCreatedAt(now)
			return m.InsertOne(c, doc)
		}
		return nil, nil
	} else {
		newID := primitive.NewObjectID().Hex()
		doc.SetCreatedAt(now)
		doc.SetID(newID)
		_, err := m.InsertOne(c, doc)
		return newID, errors.WithMessage(err, "insertOne")
	}
}
