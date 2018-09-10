package proto

import (
	"fmt"
	. "github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/util"
)

/*
A corun conversation
*/
type CoConv interface {
	// be a cancellable context
	CancellableContext

	// conversation id
	Id() string

	// must NOT be nil
	Po() Posting
	// can be nil if in a send-only posting context
	Ho() Hosting

	// round trip request in rpc style
	CoGet(code string) (result interface{}, err error)

	// outbound scripts, followed by data stream
	CoSendCode(code string) (err error)
	CoSendData(data <-chan []byte) (err error)

	// inbound scripts, followed by data stream
	CoRecvObj() (result interface{}, err error)
	CoRecvData(data <-chan []byte) (err error)

	Close()
}

func newCoConv(po *PostingEndpoint) *coConv {
	co := &coConv{
		po: po,
	}
	co.id = fmt.Sprintf("%p", co)
	return co
}

type coConv struct {
	// embed a cancellable context
	CancellableContext

	id string

	po *PostingEndpoint
}

func (co *coConv) CoGet(code string) (result interface{}, err error) {
	if co.po.co != co {
		panic(NewUsageError("CoConv mismatch ?!"))
	}
	_, err = co.po.sendPacket(code, "")
	if err != nil {
		return
	}
	result, err = co.po.ho.coRecvObj()
	return
}

func (co *coConv) CoSendCode(code string) (err error) {
	if co.po.co != co {
		panic(NewUsageError("CoConv mismatch ?!"))
	}
	_, err = co.po.sendPacket(code, "")
	return
}

func (co *coConv) CoSendData(data <-chan []byte) (err error) {
	if co.po.co != co {
		panic(NewUsageError("CoConv mismatch ?!"))
	}
	_, err = co.po.sendData(data)
	return
}

func (co *coConv) CoRecvObj() (result interface{}, err error) {
	if co.po.co != co {
		panic(NewUsageError("CoConv mismatch ?!"))
	}
	result, err = co.po.ho.coRecvObj()
	return
}

func (co *coConv) CoRecvData(data <-chan []byte) (err error) {
	if co.po.co != co {
		panic(NewUsageError("CoConv mismatch ?!"))
	}
	err = co.po.ho.coRecvData(data)
	return
}

func (co *coConv) Cancel(err error) {
	// make sure the done channel is closed anyway
	defer co.CancellableContext.Cancel(err)

	if co.po == nil {
		// already closed
		return
	}
	co.po.coDone(co)
	co.po = nil
}

func (co *coConv) Close() {
	co.Cancel(nil)
}

func (co *coConv) Id() string {
	return co.id
}

func (co *coConv) Po() Posting {
	return co.po
}

func (co *coConv) Ho() Hosting {
	return co.po.ho
}
