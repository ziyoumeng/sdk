package code

import (
	"fmt"
	"github.com/pkg/errors"
)

func ToErr(code int32) ErrCode {
	return ErrCode{
		code: code,
	}
}

//func IsNotFound(err error)bool{
//	err = errors.Cause(err)
//	if _,ok:= err.(NotFound);ok{
//		return true
//	}
//	return false
//}

func GetCode(err error) int32 {
	if err != nil {
		err1 := errors.Cause(err)
		if val, ok := err1.(Code); ok {
			return val.Code()
		}
	}
	return 0
}

type Code interface {
	Code() int32
}

type NotFound interface{
	NotFound()
}



type ErrCode struct {
	code int32
}

func (e ErrCode) Code() int32 {
	return e.code
}


func (e ErrCode) Error() string {
	return fmt.Sprintf("code :%d", e.code)
}
//type notFound struct{
//}
//
//func (e notFound)NotFound(){}
//
//func (e notFound)Error()string{
//	return fmt.Sprintf("not found")
//}
