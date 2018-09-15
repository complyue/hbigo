package conn

import (
	"fmt"
	"github.com/complyue/hbigo/pkg/errors"
	. "github.com/complyue/hbigo/pkg/proto"
	"github.com/complyue/hbigo/pkg/util"
	"github.com/golang/glog"
	"io"
	"net"
	"strings"
)

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
		glog.Error("addr error", err)
		return
	}
	var listener *net.TCPListener
	listener, err = net.ListenTCP("tcp", raddr)
	if err != nil {
		glog.Error(err)
		return
	}
	if cb != nil {
		cb(listener)
	}

	for {
		var conn *net.TCPConn
		conn, err = listener.AcceptTCP()
		if nil != err {
			glog.Error("accept error", err)
			return
		}
		netIdent := fmt.Sprintf("%s<->%s", conn.LocalAddr(), conn.RemoteAddr())
		glog.V(1).Infof("New HBI connection accepted: %s", netIdent)

		// todo DoS react
		ctx := ctxFact()

		ho := NewHostingEndpoint(ctx)
		hoWire := &tcpWire{
			CancellableContext: ho,
			netIdent:           netIdent,
			conn:               conn,
		}
		ho.PlugWire(
			netIdent, conn.LocalAddr(), conn.RemoteAddr(),
			hoWire.recvPacket, hoWire.recvData, conn.CloseRead,
		)
		ho.SetHo(ho)

		po := NewPostingEndpoint()
		poWire := &tcpWire{
			CancellableContext: po,
			netIdent:           netIdent,
			conn:               conn,
		}
		po.PlugWire(
			netIdent, conn.LocalAddr(), conn.RemoteAddr(),
			poWire.sendPacket, poWire.sendData, conn.CloseWrite, ho,
		)
		ho.SetPoToPeer(po)

		ho.StartLandingLoop()
	}
}

type TCPConn struct {
	Hosting
	Posting
	Conn *net.TCPConn
}

func (hbic *TCPConn) Cancel(err error) {
	defer hbic.Hosting.Cancel(err)
	defer hbic.Posting.Cancel(err)
}

func (hbic *TCPConn) Close() {
	hbic.Cancel(nil)
}

/*
Connect to specified remote address (host+port) with a hosting context.

The returned `*hbi.TCPConn` embeds an `hbi.Hosting` interface and an `hbi.Posting` interface.

*/
func DialTCP(ctx HoContext, addr string) (hbic *TCPConn, err error) {
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if nil != err {
		glog.Error("addr error", err)
		return
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if nil != err {
		glog.Error("conn error", err)
		return
	}
	netIdent := fmt.Sprintf("%s<->%s", conn.LocalAddr(), conn.RemoteAddr())
	glog.V(1).Infof("New HBI connection established: %s", netIdent)

	ho := NewHostingEndpoint(ctx)
	hoWire := &tcpWire{
		CancellableContext: ho,
		netIdent:           netIdent,
		conn:               conn,
	}
	ho.PlugWire(
		netIdent, conn.LocalAddr(), conn.RemoteAddr(),
		hoWire.recvPacket, hoWire.recvData, conn.CloseRead,
	)
	ho.SetHo(ho)

	po := NewPostingEndpoint()
	poWire := &tcpWire{
		CancellableContext: po,
		netIdent:           netIdent,
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
		Conn: conn,
	}

	return
}

type tcpWire struct {
	util.CancellableContext

	netIdent string
	conn     *net.TCPConn

	readahead []byte
}

func (wire *tcpWire) sendPacket(payload, wireDir string) (n int64, err error) {
	if glog.V(3) {
		defer func() {
			if err != nil {
				glog.Error(errors.RichError(err))
			} else {
				glog.Infof("HBI wire %s sent pkt %d:\n%s\n", wire.netIdent, n,
					Packet{WireDir: wireDir, Payload: payload}.String())
			}
		}()
	}
	header := fmt.Sprintf("[%v#%s]", len(payload), wireDir)
	bufs := net.Buffers{
		[]byte(header), []byte(payload),
	}
	n, err = bufs.WriteTo(wire.conn)
	return
}

// each []byte will have its len() of data sent, regardless of it cap()
func (wire *tcpWire) sendData(data <-chan []byte) (n int64, err error) {
	if glog.V(1) {
		defer func() {
			glog.Infof("HBI wire %s sent binary data of %d bytes.",
				wire.netIdent, n)
		}()
	}
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
				return
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

func (wire *tcpWire) recvPacket() (packet *Packet, err error) {
	if glog.V(3) {
		defer func() {
			if packet != nil {
				glog.Infof("HBI wire %s got pkt:\n%s\n", wire.netIdent, packet.String())
			}
		}()
	}
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
		readInto := hdrBuf[start:cap(hdrBuf)]
		if wire.readahead != nil {
			// consume readahead first
			if len(wire.readahead) > len(readInto) {
				n = len(readInto)
				copy(readInto, wire.readahead[:n])
				wire.readahead = wire.readahead[n:]
			} else {
				n = len(wire.readahead)
				copy(readInto[:n], wire.readahead)
				wire.readahead = nil
			}
		} else {
			// no readahead, read wire
			n, err = wire.conn.Read(readInto)
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
				plBegin := start + i + 1
				extraLen := newLen - plBegin
				payloadBuf = make([]byte, 0, payloadLen)
				if extraLen > payloadLen {
					// got more data than this packet's payload
					payloadBuf = payloadBuf[:payloadLen]
					plEnd := plBegin + payloadLen
					copy(payloadBuf, hdrBuf[plBegin:plEnd])
					if wire.readahead == nil {
						wire.readahead = make([]byte, newLen-plEnd)
						copy(wire.readahead, hdrBuf[plEnd:newLen])
					} else {
						readahead := make([]byte, newLen-plEnd+len(wire.readahead))
						copy(readahead[:newLen-plEnd], hdrBuf[plEnd:newLen])
						readahead = append(readahead, wire.readahead...)
						wire.readahead = readahead
					}
				} else if extraLen > 0 {
					// got some data but no more than this packet's payload
					payloadBuf = payloadBuf[:extraLen]
					copy(payloadBuf, hdrBuf[i+1:newLen])
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

	packet = &Packet{WireDir: wireDir, Payload: payload}
	if err == io.EOF {
		// clear EOF if got a complete packet.
		// todo what if the underlying Reader not tolerating our next read passing EOF
		err = nil
	}
	return
}

// each []byte will be filled up to its len
func (wire *tcpWire) recvData(data <-chan []byte) (n int64, err error) {
	if glog.V(3) {
		defer func() {
			glog.Infof("HBI wire %s received binary data of %d bytes.",
				wire.netIdent, n)
		}()
	}
	var nb int
	for {
		select {
		case <-wire.Done():
			// context cancelled
			return
		case buf, ok := <-data:
			if !ok {
				// no more buf to send
				return
			}
			if len(buf) <= 0 {
				// zero buf, ignore it
				break
			}
			for {
				if wire.readahead != nil {
					if len(buf) <= len(wire.readahead) {
						copy(buf, wire.readahead[:len(buf)])
						if len(buf) == len(wire.readahead) {
							wire.readahead = nil
						} else {
							wire.readahead = wire.readahead[len(buf):]
						}
						// this buf fully filed by readahead data
						break
					} else {
						copy(buf[:len(wire.readahead)], wire.readahead)
						// this buf only partial filled by readahead data,
						// read rest from wire
						buf = buf[len(wire.readahead):]
						wire.readahead = nil
					}
				}
				nb, err = wire.conn.Read(buf)
				if err != nil {
					return
				}
				n += int64(nb)
				if nb >= len(buf) {
					// this buf fully filled
					break
				}
				// read into rest space
				buf = buf[nb:]
			}
		}
	}
	return
}
