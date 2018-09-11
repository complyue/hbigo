package proto

import (
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
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

	// round trip request in rpc style
	Get(code string) (result interface{}, err error)

	// send outbound scripts
	SendCode(code string) (err error)
	// send a binary data stream, the peer must understand the size and layout of this stream,
	// from previous posted scripts, actually it's expected previous scripts do trigger peer side
	// funcs to call `ho.CoRecvData()` with a chan of []byte buffers properly sized and laid out,
	// matching []bytes series posted here.
	SendData(data <-chan []byte) (err error)

	// receive an inbound data object created by landing scripts sent by peer
	// the scripts is expected to be sent from peer by `po.CoSendCode()`
	RecvObj() (result interface{}, err error)
	// receive an inbound binary data stream sent by peer
	// actually it's expected to be sent from peer by `po.CoSendData()`, the size and layout
	// should have been deducted from previous received data objects
	RecvData(data <-chan []byte) (err error)

	// finish this conversation to release the wire for other traffic
	Close()
}

func newConver(po *PostingEndpoint) *conver {
	co := &conver{
		po: po,
	}
	co.id = fmt.Sprintf("%p", co)
	return co
}

type conver struct {
	// embed a cancellable context
	CancellableContext

	id string

	po *PostingEndpoint
}

func (co *conver) Get(code string) (result interface{}, err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	_, err = co.po.sendPacket(code, "")
	if err != nil {
		return
	}
	result, err = co.po.ho.recvObj()
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

func (co *conver) RecvObj() (result interface{}, err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	result, err = co.po.ho.recvObj()
	return
}

func (co *conver) RecvData(data <-chan []byte) (err error) {
	if co.po.co != co {
		panic(errors.NewUsageError("Conver mismatch ?!"))
	}
	_, err = co.po.ho.recvData(data)
	return
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
