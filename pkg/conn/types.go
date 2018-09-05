package conn

import (
	"fmt"
	"reflect"
)

type WireError struct {
	msg string
}

func (e *WireError) Error() string {
	return e.msg
}

type Context interface {
	Get(key string) interface{}
	Put(key string, value interface{})
	Defined(key string) bool
}

type context struct {
	scope map[string]interface{}
}

func (ctx *context) Get(key string) interface{} {
	return ctx.scope[key]
}

func (ctx *context) Put(key string, value interface{}) {
	ctx.scope[key] = value
}

func (ctx *context) Defined(key string) bool {
	_, ok := ctx.scope[key]
	return ok
}

func NewContext(population interface{}) Context {
	var ctx context
	ctx.scope = make(map[string]interface{})

	popu, ok := population.(reflect.Value)
	if !ok {
		popu = reflect.ValueOf(population)
	}
	switch popu.Kind() {
	case reflect.Struct:
		for i, n := 0, popu.Type().NumField(); i < n; i++ {
			fn := popu.Type().Field(i).Name
			fv := popu.Field(i)
			ctx.scope[fn] = fv.Interface()
		}
	case reflect.Map:
		for _, key := range popu.MapKeys() {
			ctx.scope[fmt.Sprintf("%v", key)] = popu.MapIndex(key).Interface()
		}
	default:
		panic(fmt.Sprintf("Unsupported population type: %t", population))
	}

	return &ctx
}

type HBIC interface {
	Fire(code string)
	FireCoRun(code string, data chan []byte)

	Connect() (err error)

	Notif(code string) (err error)
	NotifCoRun(code string, data chan []byte) (err error)

	CoBegin() (err error)

	CoSendCoRun(code string) (err error)

	CoGet(code string) (result interface{}, err error)

	CoSendCode(code string) (err error)
	CoSendData(data chan []byte) (err error)

	CoRecvObj() (result interface{}, err error)
	CoRecvData(data chan []byte) (err error)

	CoEnd() (err error)

	Disconnect() (err error)
}
