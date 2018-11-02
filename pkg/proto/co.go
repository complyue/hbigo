package proto

import (
	"fmt"
	"time"

	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
	"github.com/golang/glog"
)

/*
A conversation established upon a posting wire.
*/
type Conver interface {
	// be a cancellable context
	CancellableContext

	// conversation id
	Id() string

	// must NOT be nil
	Po() Posting
	// can be nil if in a send-only posting context
	Ho() Hosting

	// obtain remote execution result, in round trip (rpc) style, where `hint` can be:
	//   *) nil
	//     for simple value with no serialization, i.e.
	//     remote execution result object is formatted to a string by `%#v`;
	//     this string is sent back to local hosting context for evaluation;
	//     the evaluated value object is returned as `result`.
	//   *) a string
	//     for structured value to be serialized with BSON, i.e.
	//     remote execution result object is serialized to a `[]byte` buffer by `bson.Marshal()`;
	//     this buffer is sent back to local hosting context to be deserialized with `RecvBSON()`;
	//     where the `hint` string value will be passed as its `booter` argument, literally;
	//     the `out` value will be returned as `result`.
	//   *) <other fancy serializing schemas>
	//     <not implemented yet>
	Get(code string, hint interface{}) (result interface{}, err error)

	// send a piece of outbound script to run and yield its return value to conversation
	SendCode(code string) (err error)

	// send a bson object, which may be a map or a struct value, to remote conversation.
	// the `hint` string can be empty for remote to receive a `bson.M`,
	// or it must be a valid Go expression evaluates to a map, or a pointer to a struct,
	// whose type is either unnamed, or must be available within remote hosting context.
	SendBSON(o interface{}, hint string) error

	// send a binary data stream, the peer must understand the size and layout of this stream,
	// from previous posted scripts, actually it's expected previous scripts do trigger peer side
	// funcs to call `ho.CoRecvData()` with a chan of []byte buffers properly sized and laid out,
	// matching []bytes series posted here.
	SendData(data <-chan []byte) (err error)

	// receive an inbound data object created by landing scripts sent by peer
	// the scripts is expected to be sent from peer by `po.CoSendCode()`
	RecvObj() (result interface{}, err error)

	// receive a bson object. if `booter` is nil, `out` will be a `bson.M`, else
	// out will be `booter` value as passed in.
	// the object is expected to be sent from peer by `co.SendBSON()` or `po.CoSendBSON()`.
	RecvBSON(nBytes int, booter interface{}) (out interface{}, err error)

	// receive an inbound binary data stream sent by peer
	// actually it's expected to be sent from peer by `po.CoSendData()`, the size and layout
	// should have been deducted from previous received data objects
	RecvData(data <-chan []byte) (err error)

	// finish this conversation to release the wire for other traffic
	Close()
}

func newConver(po *PostingEndpoint) *conver {
	co := &conver{
		CancellableContext: NewCancellableContext(),
		po:                 po,
		chObj:              make(chan interface{}),
	}
	co.id = fmt.Sprintf("%p", co)
	return co
}

type conver struct {
	// embed a cancellable context
	CancellableContext

	po    *PostingEndpoint
	chObj chan interface{}
	id    string
}

func (co *conver) Get(code string, hint interface{}) (result interface{}, err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	var wireDir string
	switch hint := hint.(type) {
	case nil:
		wireDir = "coget:"
	case string:
		wireDir = fmt.Sprintf(`coget:bson:%s`, hint)
	default:
		panic(errors.NewUsageError(fmt.Sprintf("Unsupported hint (type %T): %#v", hint, hint)))
	}
	_, err = co.po.sendPacket(code, wireDir)
	if err != nil {
		return
	}
	result, err = co.recvObj()
	return
}

func (co *conver) SendCode(code string) (err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	_, err = co.po.sendPacket(code, "")
	return
}

func (co *conver) SendData(data <-chan []byte) (err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	_, err = co.po.sendData(data)
	return
}

func (co *conver) SendBSON(o interface{}, hint string) error {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	return co.po.sendBSON(o, hint)
}

func (co *conver) RecvObj() (result interface{}, err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	result, err = co.recvObj()
	return
}

func (co *conver) recvObj() (interface{}, error) {
	if glog.V(3) {
		glog.Infof("Receiving object from posting conversation %s receiver %p ...", co.id, co.chObj)
		defer func() {
			glog.Infof("Done receiving object from posting conversation %s receiver %p ...", co.id, co.chObj)
		}()
	}
	po := co.po
	// err if already disconnected due to error
	select {
	case <-po.Done():
		err := po.Err()
		if err != nil {
			return nil, errors.Wrapf(err, "Wire already disconnected due to error.")
		}
	default:
		// fall through
	}

	// block to receive
	select {
	case result := <-co.chObj:
		// most normal case, got result
		return result, nil
	case <-po.Done():
		// disconnected
		if err := po.Err(); err != nil {
			// disconnected due to error, propagate the error
			return nil, errors.Wrapf(err, "Wire already disconnected due to error.")
		}
		// disconnected normally, give a second chance to receive an object
		select {
		case result := <-co.chObj:
			return result, nil
		case <-time.After(1 * time.Millisecond):
			return nil, errors.New("Wire already disconnected.")
		}
	}
}

func (co *conver) RecvData(data <-chan []byte) (err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	_, err = co.po.ho.recvData(data)
	return
}

func (co *conver) RecvBSON(nBytes int, booter interface{}) (interface{}, error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	out, err := co.po.ho.recvBSON(nBytes, booter)
	return out, err
}

func (co *conver) Cancel(err error) {
	// make sure the done channel is closed anyway
	defer co.CancellableContext.Cancel(err)

	if co.po == nil {
		// already closed
		return
	}
	co.po.coDone(co)
	co.po = nil
}

func (co *conver) Close() {
	co.Cancel(nil)
}

func (co *conver) Id() string {
	return co.id
}

func (co *conver) Po() Posting {
	return co.po
}

func (co *conver) Ho() Hosting {
	return co.po.ho
}
