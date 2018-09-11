package svcpool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"sync"
)

func NewConsumer(
	ctxFact func() hbi.HoContext, poolAddr string,
) (consumer *Consumer, err error) {
	var connPool *hbi.TCPConn
	connPool, err = hbi.DialTCP(
		hbi.NewHoContext(), poolAddr,
	)
	if err != nil {
		return
	}
	consumer = &Consumer{
		Pool:    connPool,
		CtxFact: ctxFact,
	}
	return
}

type Consumer struct {
	sync.Mutex // embed a lock to be synced for thread safety

	// HBI connection to service pool
	Pool *hbi.TCPConn

	CtxFact func() hbi.HoContext

	// connected service connections by proc address string
	// direct access not recommended, but if so, should be synced when accessing from multiple goros
	SvcConns map[string]*hbi.TCPConn
}

// get an HBI connection to an assigned service proc by session id
func (consumer *Consumer) GetService(session string, sticky bool) (service *hbi.TCPConn, err error) {
	var ok bool
	consumer.Lock()
	defer consumer.Unlock()

	var procAddr string
	procAddr, err = consumer.AssignProc(session, sticky)
	if err != nil {
		return
	}

	if service, ok = consumer.SvcConns[procAddr]; ok {
		return
	}

	service, err = hbi.DialTCP(consumer.CtxFact(), procAddr)
	if err != nil {
		return
	}

	return
}

func (consumer *Consumer) AssignProc(session string, sticky bool) (procAddr string, err error) {
	co := consumer.Pool.PoToPeer().Co()
	defer co.Close()
	var addrStr interface{}
	addrStr, err = co.Get(fmt.Sprintf(`
AssignProc(%#v,%#v)
`, session, sticky))
	if err != nil {
		return
	}
	procAddr = addrStr.(string)
	return
}

func (consumer *Consumer) ReleaseProc(procAddr string) (err error) {
	co := consumer.Pool.PoToPeer().Co()
	defer co.Close()
	_, err = co.Get(fmt.Sprintf(`
ReleaseProc(%#v)
`, procAddr))
	if err != nil {
		return
	}
	return
}
