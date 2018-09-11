package conn

import (
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/proto"
	"github.com/complyue/hbigo/pkg/util"
	"github.com/golang/glog"
	"io"
	"log"
	"net"
	"strings"
)

type TCPConn struct {
	Hosting
	Posting
}

func (hbic *TCPConn) Cancel(err error) {
	defer hbic.Hosting.Cancel(err)
	defer hbic.Posting.Cancel(err)
}

func (hbic *TCPConn) Close() {
	hbic.Cancel(nil)
}

/*
Serve with a hosting context factory, at specified local address (host:port)

`cb` will be called with the created `*net.TCPListener`, it's handful to specify port as 0,
and receive the actual port from the cb.

This func won't return until the listener is closed.

*/
func ServeTCP(ctxFact func() HoContext, addr string, cb func(*net.TCPListener)) (err error) {
	var raddr *net.TCPAddr
	raddr, err = net.ResolveTCPAddr("tcp", addr)
	if nil != err {
		log.Fatal("addr error", err)
		return
	}
	var listener *net.TCPListener
	listener, err = net.ListenTCP("tcp", raddr)
	if cb != nil {
		cb(listener)
	}

	for {
		var conn *net.TCPConn
		conn, err = listener.AcceptTCP()
		if nil != err {
			log.Fatal("accept error", err)
			return
		}
		netIdent := fmt.Sprintf("%s<->%s", conn.LocalAddr(), conn.RemoteAddr())
		glog.V(1).Infof("New HBI connection accepted: %s", netIdent)

		// todo DoS react
		ctx := ctxFact()

		ho := NewHostingEndpoint(ctx)
		hoWire := tcpWire{
			CancellableContext: ho,
			conn:               conn,
		}
		ho.PlugWire(
			netIdent, conn.LocalAddr(), conn.RemoteAddr(),
			hoWire.recvPacket, hoWire.recvData, conn.CloseRead,
		)
		ho.SetHo(ho) // ctx can intercept this

		po := NewPostingEndpoint()
		poWire := tcpWire{
			CancellableContext: po,
			conn:               conn,
		}
		po.PlugWire(
			netIdent, conn.LocalAddr(), conn.RemoteAddr(),
			poWire.sendPacket, poWire.sendData, conn.CloseWrite, ho,
		)
		ho.SetPoToPeer(po) // ctx can intercept this

		ho.StartLandingLoop()
	}
}

/*
Connect to specified remote address (host+port) with a hosting context.

The returned `*hbi.TCPConn` embeds an `hbi.Hosting` interface and an `hbi.Posting` interface.

*/
func DialTCP(ctx HoContext, addr string) (hbic *TCPConn, err error) {
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if nil != err {
		log.Fatal("addr error", err)
		return
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if nil != err {
		log.Fatal("conn error", err)
		return
	}
	netIdent := fmt.Sprintf("%s<->%s", conn.LocalAddr(), conn.RemoteAddr())
	glog.V(1).Infof("New HBI connection established: %s", netIdent)

	ho := NewHostingEndpoint(ctx)
	hoWire := tcpWire{
		CancellableContext: ho,
		conn:               conn,
	}
	ho.PlugWire(
		netIdent, conn.LocalAddr(), conn.RemoteAddr(),
		hoWire.recvPacket, hoWire.recvData, conn.CloseRead,
	)

	po := NewPostingEndpoint()
	poWire := tcpWire{
		CancellableContext: po,
		conn:               conn,
	}
	po.PlugWire(
		netIdent, conn.LocalAddr(), conn.RemoteAddr(),
		poWire.sendPacket, poWire.sendData, conn.CloseWrite,
		ho,
	)

	ho.SetPoToPeer(po)

	ho.StartLandingLoop()

	hbic = &TCPConn{
		Hosting: ho, Posting: po,
	}

	return
}

type tcpWire struct {
	util.CancellableContext
	conn *net.TCPConn
}

func (wire tcpWire) sendPacket(payload, wireDir string) (n int64, err error) {
	header := fmt.Sprintf("[%v#%s]", len(payload), wireDir)
	bufs := net.Buffers{
		[]byte(header), []byte(payload),
	}
	n, err = bufs.WriteTo(wire.conn)
	return
}

func (wire tcpWire) recvPacket() (packet *Packet, err error) {
	const MaxHeaderLen = 60
	var (
		wireDir, payload string
		n, start, newLen int
		hdrBuf           = make([]byte, 0, MaxHeaderLen)
		payloadBuf       []byte
	)

	// read header
	for {
		if wire.Cancelled() {
			return
		}
		start = len(hdrBuf)
		n, err = wire.conn.Read(hdrBuf[start:cap(hdrBuf)])
		if err == io.EOF {
			if start+n <= 0 {
				// normal EOF after full packet, return nil + EOF
				return
			}
			// fall through to receive this last packet, it's possible we already got the full data in hdrBuf
		} else if err != nil {
			// other error occurred
			return
		}
		newLen = start + n
		hdrBuf = hdrBuf[:newLen]
		for i, c := range hdrBuf[start:newLen] {
			if ']' == c {
				header := string(hdrBuf[0 : start+i+1])
				if '[' != header[0] {
					err = errors.NewWireError(fmt.Sprintf("Invalid header: %#v", header))
					return
				}
				lenEnd := strings.Index(header, "#")
				if -1 == lenEnd {
					err = errors.NewWireError("No # in header!")
					return
				}
				wireDir = string(header[lenEnd+1 : start+i])
				var payloadLen int
				fmt.Sscan(header[1:lenEnd], &payloadLen)
				if payloadLen < 0 {
					err = errors.NewWireError("Negative payload length!")
					return
				}
				chunkLen := newLen - i - 1
				func() { // mysterious cap range problem, reproducible ?
					defer func() {
						if e := recover(); e != nil {
							panic(errors.Wrapf(errors.RichError(e), "%v/%v", chunkLen, payloadLen))
						}
					}()
					payloadBuf = make([]byte, chunkLen, payloadLen)
				}()
				if chunkLen > 0 {
					copy(payloadBuf[:chunkLen], hdrBuf[i+1:newLen])
				}
				break
			}
		}
		if payloadBuf != nil {
			break
		}
		if newLen >= MaxHeaderLen {
			err = errors.NewWireError(fmt.Sprintf("No header within first %v bytes!", MaxHeaderLen))
			return
		}
		if err == io.EOF {
			// reached EOF without full header
			err = errors.NewWireError("Incomplete header at EOF!")
			return
		}
	}

	// read payload
	for len(payloadBuf) < cap(payloadBuf) {
		if wire.Cancelled() {
			return
		}
		if err == io.EOF {
			err = errors.NewWireError("Premature packet at EOF.")
			return
		}
		start = len(payloadBuf)
		n, err = wire.conn.Read(payloadBuf[start:cap(payloadBuf)])
		newLen = start + n
		payloadBuf = payloadBuf[:newLen]
		if newLen >= cap(payloadBuf) {
			break
		}
	}
	payload = string(payloadBuf)

	packet = &Packet{wireDir, payload}
	if err == io.EOF {
		// clear EOF if got a complete packet.
		// todo what if the underlying Reader not tolerating our next read passing EOF
		err = nil
	}
	return
}

// each []byte will have its len() of data sent, regardless of it cap()
func (wire tcpWire) sendData(data <-chan []byte) (n int64, err error) {
	var bufs net.Buffers
	var nb int64
	for {
		select {
		case <-wire.Done():
			// context cancelled
			return
		case buf, ok := <-data:
			if !ok {
				// no more buf to send
				break
			}
			if len(buf) <= 0 {
				// zero buf, ignore it
				break
			}
			bufs = append(bufs, buf)
		}
		if len(bufs) <= 0 {
			// all data sent
			return
		}
		nb, err = bufs.WriteTo(wire.conn)
		if err != nil {
			return
		}
		n += nb
	}
	return
}

// each []byte will be filled up to its full cap
func (wire tcpWire) recvData(data <-chan []byte) (n int64, err error) {
	var nb int
	for {
		select {
		case <-wire.Done():
			// context cancelled
			return
		case buf, ok := <-data:
			if !ok {
				// no more buf to send
				break
			}
			if len(buf) <= 0 {
				// zero buf, ignore it
				break
			}
			for {
				nb, err = wire.conn.Read(buf[:cap(buf)])
				if err != nil {
					return
				}
				if nb >= cap(buf) {
					// this buf fully filled
					break
				}
				n += int64(nb)
				// read into rest space
				buf = buf[nb:cap(buf)]
			}
		}
	}
	return
}
