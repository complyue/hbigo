package pool

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/complyue/hbigo/pkg/errors"
	"github.com/golang/glog"
	"net"
	"os"
	"runtime"
	"syscall"
	"time"
)

func NewMaster(
	poolSize int, hotBack int, processStartTimeout time.Duration,
) (*Master, error) {
	if poolSize < 1 || hotBack < 0 || hotBack > poolSize {
		return nil, errors.RichError(fmt.Sprintf("Invalid pool size and hot back: %d/%d", hotBack, poolSize))
	}

	var err error
	var master = &Master{
		poolSize:            poolSize,
		hotBack:             hotBack,
		processStartTimeout: processStartTimeout,
		allWorkers:          make(map[*procWorker]struct{}),
		pendingWorkers:      make(map[*procWorker]time.Time),
		idleWorkers:         make(chan *procWorker, poolSize),
		workersByPid:        make(map[int]*procWorker),
		workersBySession:    make(map[string]*procWorker),
	}

	return master, err
}

type Master struct {
	poolSize            int
	hotBack             int
	processStartTimeout time.Duration
	teamAddr            *net.TCPAddr
	allWorkers          map[*procWorker]struct{}
	// map worker to its last process start time, removed after successfully registered
	pendingWorkers   map[*procWorker]time.Time
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
			newProcWorker(pool)
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

	// run pool master with parallelism of 1.
	// if parallelism is to be increased, before applying a bigger number than 1:
	// 	*) access to pool master data structures must be properly synced for thread safety
	//  *) race condition for sticky session assignment needs to be prevented by proper syncing
	runtime.GOMAXPROCS(1)

	// start a goro to face proc workers
	go func() {
		hbi.ServeTCP(func() hbi.HoContext {
			return newMaster4Worker(pool)
		}, "127.0.0.1:0", func(listener *net.TCPListener) {
			glog.Infof("HBI Service team addr: %+v\n", listener.Addr())
			pool.teamAddr = listener.Addr().(*net.TCPAddr)

			// start hot back processes
			for i := 0; i < pool.hotBack; i++ {
				newProcWorker(pool)
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

func (pool *Master) registerWorker(pid int, procPort int, ho hbi.Hosting) (w *procWorker) {
	var ok bool

	w, ok = pool.workersByPid[pid]
	if !ok {
		ho.Cancel(errors.Errorf("Your pid [%d] is not expected!", pid))
		return
	}
	if w.proc.Pid != pid {
		panic(errors.Errorf("?! pid %d vs %d", pid, w.proc.Pid))
	}
	w.ho = ho
	w.ProcPort = procPort
	w.lastSession = ""

	delete(pool.pendingWorkers, w)
	pool.idleWorkers <- w

	// restart a new process after disconnected
	go func() {
		<-ho.Done()
		// don't restart too quickly, or continuous os failures
		// could create extreme system pressure
		time.Sleep(10 * time.Second)
		w.restartProcess(ho.Err())
	}()

	return
}

func newProcWorker(pool *Master) *procWorker {
	worker := &procWorker{
		pool: pool,
	}
	pool.allWorkers[worker] = struct{}{}
	worker.startProcess()
	return worker
}

type procWorker struct {
	pool *Master

	proc *os.Process

	lastAct time.Time

	ho hbi.HoContext

	ProcPort int

	lastSession string
}

func (w *procWorker) startProcess() (err error) {
	// todo better to clear workersByPid or not ?
	if w.lastSession != "" {
		delete(w.pool.workersBySession, w.lastSession)
		w.lastSession = ""
	}
	w.pool.pendingWorkers[w] = time.Now()
	var exePath string
	exePath, err = os.Executable()
	if err != nil {
		return
	}
	w.proc, err = os.StartProcess(
		exePath, []string{
			"-team", w.pool.teamAddr.String(),
		}, &os.ProcAttr{},
	)
	if err != nil {
		return
	}
	w.pool.workersByPid[w.proc.Pid] = w
	return
}

func (w *procWorker) restartProcess(reason error) {
	workerInfo := ""
	if w.proc != nil {
		workerInfo = fmt.Sprintf("ptr=%p,pid=%v", w, w.proc.Pid)
	} else {
		workerInfo = fmt.Sprintf("ptr=%p", w)
	}
	if reason != nil {
		glog.Errorf("Restarting proc worker [%s] due to error: %+v", workerInfo, reason)
	}
	if startTime, ok := w.pool.pendingWorkers[w]; ok {
		if time.Now().Sub(startTime) < w.pool.processStartTimeout {
			// recently restarted
			glog.Warningf("Repeated restarting of proc worker [%s] ignored.", workerInfo)
			return
		} else {
			// start timed out,
			glog.Warningf("Service proc worker [%s] not registered within %v, attempt restarting ...",
				workerInfo, w.pool.processStartTimeout,
			)
		}
	}
	if w.ho != nil {
		w.ho.Cancel(reason)
		w.ho = nil
	}
	if w.proc != nil {
		_ = w.proc.Kill() // ignore err
		w.proc = nil
	}
	w.startProcess()
}

func (w *procWorker) checkAlive() (alive bool) {
	var err error
	defer func() {
		if !alive {
			w.restartProcess(err)
		}
	}()
	if w.proc == nil || w.ho == nil {
		// not started
		return
	}
	if time.Now().Sub(w.lastAct) < (5 * time.Second) {
		// affirmative recently
		alive = true
		return
	}
	// verify wire is functioning for sending
	err = w.ho.PoToPeer().Notif("ping()")
	if err != nil {
		return
	}
	// check pid exists
	err = w.proc.Signal(syscall.Signal(0))
	if err != nil {
		return
	}
	// aliveness confirmed
	w.lastAct = time.Now()
	alive = true
	return
}

func (w *procWorker) prepareSession(session string) {
	p2p := w.ho.PoToPeer()
	co := p2p.Co()
	defer co.Close()
	workerSession, err := co.Get(fmt.Sprintf(`
PrepareSession(%#v)
`, session))
	if err != nil {
		defer w.restartProcess(errors.RichError(err))
		return
	}
	if workerSession != session {
		defer w.restartProcess(errors.Errorf(
			"Unexpected session preparation result [%s] vs [%s]",
			workerSession, session,
		))
	}
	w.lastSession = workerSession.(string)
}

func (w *procWorker) retired() {
	// start a new process to replace the retired one
	w.restartProcess(nil)
}

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
