package pool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"net"
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

func (pool *Master) Serve(serviceAddr string) {

	go func() {
		hbi.ServeTCP(newMaster4Worker, "127.0.0.1:0", func(listener *net.TCPListener) {
			glog.Infof("Pool team addr: %+v\n", listener.Addr())
			pool.teamAddr = listener.Addr().(*net.TCPAddr)

			// todo maintain proc workers
		})
	}()

	hbi.ServeTCP(newMaster4Consumer, serviceAddr, func(listener *net.TCPListener) {
		glog.Infof("Pool service addr: %+v\n", listener.Addr())
	})

}

type ProcWorker struct {
}
