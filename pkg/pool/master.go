package pool

import (
	"fmt"
	"github.com/complyue/hbigo"
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
	consumer *ServiceConsumer
}

func (m4c *master4consumer) AssignProc(session string, sticky bool) {
	procPort := m4c.pool.assignProc(m4c.consumer)
	p2p := m4c.PoToPeer()
	co := p2p.Co()
	defer co.Close()
	co.CoSendCode(fmt.Sprintf(
		// use the IP via which this consumer has connected to this pool
		"%s:%d", p2p.LocalAddr().(*net.IPAddr).String(), procPort,
	))
}

func (m4c *master4consumer) ReleaseProc() {

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
