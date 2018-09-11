package pool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"net"
)

func newMaster4Consumer(pool *Master) hbi.HoContext {
	return &master4consumer{
		HoContext: hbi.NewHoContext(),

		pool: pool,
	}
}

type master4consumer struct {
	hbi.HoContext

	pool     *Master
	consumer *serviceConsumer
}

func (m4c *master4consumer) AssignProc(session string, sticky bool) {
	consumer := m4c.consumer
	if consumer == nil {
		// first assignment request, create consumer object
		consumer = newServiceConsumer(m4c.Ho(), session, sticky)
		m4c.consumer = consumer
	} else {
		if consumer.sticky && session != consumer.session {
			panic(errors.Errorf("Changing sticky session [%s]=>[%s] ?!", consumer.session, session))
		}
		consumer.session = session
		consumer.sticky = sticky
	}
	procPort := m4c.pool.assignProc(consumer)
	p2p := m4c.PoToPeer()
	// a conversation should have been initiated by service consumer endpoint
	p2p.CoSendCode(fmt.Sprintf(
		// use the IP via which this consumer has connected to this pool
		`"%s:%d"`, p2p.LocalAddr().(*net.IPAddr).String(), procPort,
	))
}

func (m4c *master4consumer) ReleaseProc() {
	panic(errors.New("Not impl."))
}

func newMaster4Worker() hbi.HoContext {
	return &master4worker{
		HoContext: hbi.NewHoContext(),
	}
}

type master4worker struct {
	hbi.HoContext
}

func (m4w *master4worker) WorkerOnline(pid int) {

}
