package util

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

//注意：只判断了常用类型
func IsEmpty(val interface{}) bool {
	if val == nil {
		return true
	}
	switch val.(type) {
	case string:
		return val.(string) == ""
	case int:
		return val.(int) == 0
	case uint:
		return val.(uint) == 0
	case int32:
		return val.(int32) == 0
	case uint32:
		return val.(uint32) == 0
	case int64:
		return val.(int64) == 0
	case uint64:
		return val.(uint64) == 0
	case []byte:
		return val.([]byte) == nil
	case byte:
		return val.(byte) == 0
	case primitive.ObjectID:
		return val.(primitive.ObjectID).IsZero()
	}
	panic("not support")
}

func ToStr(val interface{}) string{
	if val == nil {
		return ""
	}
	switch val.(type) {
	case string:
		return val.(string)
	case int:
		return fmt.Sprintf("%d", val.(int))
	case uint:
		return  fmt.Sprintf("%d", val.(uint))
	case int32:
		return fmt.Sprintf("%d", val.(int32))
	case uint32:
		return fmt.Sprintf("%d", val.(uint32))
	case int64:
		return fmt.Sprintf("%d", val.(int64))
	case uint64:
		return fmt.Sprintf("%d", val.(uint64))
	case primitive.ObjectID:
		return val.(primitive.ObjectID).Hex()
	}
	panic("not support")
}
