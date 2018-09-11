package pool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"net"
	"os"
	"runtime"
)

func newServiceConsumer(ho hbi.Hosting, session string, sticky bool) *serviceConsumer {
	return &serviceConsumer{
		ho: ho, session: session, sticky: sticky,
	}
}

type serviceConsumer struct {
	ho      hbi.Hosting
	session string
	sticky  bool

	assignedWorker *procWorker
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
		allWorkers:       make(map[*procWorker]struct{}),
		pendingWorkers:   make(map[*procWorker]struct{}),
		idleWorkers:      make(chan *procWorker, poolSize),
		workersByPid:     make(map[int]*procWorker),
		workersBySession: make(map[string]*procWorker),
	}

	return master, err
}

type Master struct {
	poolSize         int
	hotBack          int
	ackTimeout       float64
	teamAddr         *net.TCPAddr
	allWorkers       map[*procWorker]struct{}
	pendingWorkers   map[*procWorker]struct{}
	idleWorkers      chan *procWorker
	workersByPid     map[int]*procWorker
	workersBySession map[string]*procWorker
}

func (pool *Master) assignProc(consumer *serviceConsumer) (procPort int) {
	var ok bool
	var worker *procWorker
	// prepare worker proc anyway before actual return
	defer func() {
		if worker == nil {
			panic(errors.New("No proc assigned ?!"))
		}
		if consumer.session != "" && worker.lastSession != consumer.session {
			if worker.lastSession != "" {
				delete(pool.workersBySession, worker.lastSession)
				glog.Warningf(
					"Consumer %s caused session switch [%s]=>[%s]\n",
					consumer.ho.NetIdent(), worker.lastSession, consumer.session,
				)
			}
			worker.prepareSession(consumer.session)
			pool.workersBySession[worker.lastSession] = worker
		}
		consumer.assignedWorker = worker
		procPort = worker.ProcPort // set return value
	}()

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

	worker = consumer.assignedWorker
	if worker != nil && worker.checkAlive() {
		// this consumer has had a worker assigned, and still alive, reused it
		return
	}

	if consumer.sticky && consumer.session != "" {
		// if the consumer requests sticky session, and there's already a proc worker on the specified session,
		// must assign this worker whether it is idle or not. concurrency/parallelism is offloaded to the proc impl.
		if worker, ok = pool.workersBySession[consumer.session]; ok {
			if worker.lastSession != consumer.session {
				panic(errors.New("?!"))
			}
			if worker.checkAlive() {
				return
			}
		}
	}

	// search for idle workers for this assignment
	searchedWorkers := make(map[*procWorker]struct{})
	for {
		worker := <-pool.idleWorkers
		if !worker.checkAlive() {
			continue
		}
		if consumer.session == "" {
			// session-less
			if worker.lastSession == "" {
				// ideal no-session match
				return
			}
		} else if worker.lastSession == consumer.session {
			// ideal session match
			return
		}
		if _, ok = searchedWorkers[worker]; ok {
			// reached a known idle worker, meaning we've searched all idle workers without ideal match,
			// use this worker anyway
			return
		}
		// record this idle worker as known, and continue searching for an ideal match
		searchedWorkers[worker] = struct{}{}
		// put it back to ideal cha
		pool.idleWorkers <- worker
	}

	return
}

func (pool *Master) Serve(serviceAddr string) {

	// run pool master with parallelism of 1
	runtime.GOMAXPROCS(1)

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

func newProcWorker(pool *Master) *procWorker {
	worker := &procWorker{}
	return worker
}

type procWorker struct {
	proc *os.Process

	ProcPort int

	lastSession string
}

func (w *procWorker) checkAlive() bool {

	return true
}

func (w *procWorker) prepareSession(session string) {

	w.lastSession = session
}
