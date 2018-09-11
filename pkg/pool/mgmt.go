package pool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"net"
	"os"
)

type ServiceConsumer struct {
	ho      hbi.Hosting
	session string
	sticky  bool

	assignedWorker *ProcWorker
}

func NewMaster(
	poolSize int, hotBack int,
) (*Master, error) {
	if poolSize < 1 || hotBack < 0 || hotBack > poolSize {
		return nil, errors.RichError(fmt.Sprintf("Invalid pool size and hot back: %d/%d", hotBack, poolSize))
	}

	var err error
	var master = &Master{
		poolSize:         poolSize,
		hotBack:          hotBack,
		ackTimeout:       10,
		allWorkers:       make(map[*ProcWorker]struct{}),
		pendingWorkers:   make(map[*ProcWorker]struct{}),
		idleWorkers:      make(chan *ProcWorker, poolSize),
		workersByPid:     make(map[int]*ProcWorker),
		workersBySession: make(map[string]*ProcWorker),
	}

	return master, err
}

type Master struct {
	poolSize         int
	hotBack          int
	ackTimeout       float64
	teamAddr         *net.TCPAddr
	allWorkers       map[*ProcWorker]struct{}
	pendingWorkers   map[*ProcWorker]struct{}
	idleWorkers      chan *ProcWorker
	workersByPid     map[int]*ProcWorker
	workersBySession map[string]*ProcWorker
}

func (pool *Master) assignProc(consumer *ServiceConsumer) (procPort int) {
	poolQuota := pool.poolSize - len(pool.allWorkers)
	idleQuota := pool.hotBack - len(pool.pendingWorkers) - len(pool.idleWorkers)
	if idleQuota > poolQuota {
		idleQuota = poolQuota
	}
	if idleQuota > 0 {
		glog.V(1).Infof(
			"Starting %d hot backing proc workers given current numbers: (%d+%d)/%d",
			len(pool.idleWorkers), len(pool.pendingWorkers), pool.poolSize,
		)
		for ; idleQuota > 0; idleQuota-- {
			worker := newProcWorker(pool)
			pool.allWorkers[worker] = struct{}{}
			pool.pendingWorkers[worker] = struct{}{}
		}
	}

	worker := consumer.assignedWorker
	if worker != nil && worker.checkAlive() {

	}

	return
}

func (pool *Master) Serve(serviceAddr string) {

	// start a goro to face proc workers
	go func() {
		hbi.ServeTCP(newMaster4Worker, "127.0.0.1:0", func(listener *net.TCPListener) {
			glog.Infof("Pool team addr: %+v\n", listener.Addr())
			pool.teamAddr = listener.Addr().(*net.TCPAddr)

			// start hot back procs
			for i := 0; i < pool.hotBack; i++ {
				pool.allWorkers[newProcWorker(pool)] = struct{}{}
			}
		})
	}()

	// serve consumers in blocking mode
	hbi.ServeTCP(func() hbi.HoContext {
		return newMaster4Consumer(pool)
	}, serviceAddr, func(listener *net.TCPListener) {
		glog.Infof("Pool service addr: %+v\n", listener.Addr())
	})

}

func newProcWorker(pool *Master) *ProcWorker {
	worker := &ProcWorker{}
	return worker
}

type ProcWorker struct {
	proc *os.Process

	ProcPort int
}

func (w *ProcWorker) checkAlive() bool {

	return true
}
