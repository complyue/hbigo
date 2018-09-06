package conn

import (
	"encoding/json"
	"fmt"
	"github.com/cosmos72/gomacro/fast"
	"log"
	"net"
	"strings"
)

type tcpHBIC struct {
	ctxPkgPath string
	addr       string
	conn       *net.TCPConn
}

func ListenTCP(ctxPkgPath string, addr string) (listener *net.TCPListener, err error) {
	interp := fast.New()
	_, _ = interp.Eval(fmt.Sprintf("import . \"%s\"", ctxPkgPath))

	var raddr *net.TCPAddr
	raddr, err = net.ResolveTCPAddr("tcp", addr)
	if nil != err {
		log.Fatal("addr error", err)
		return
	}
	listener, err = net.ListenTCP("tcp", raddr)
	for {
		var conn *net.TCPConn
		conn, err = listener.AcceptTCP()
		if nil != err {
			log.Fatal("accept error", err)
			return
		}
		// todo DoS react
		go func(hbic *tcpHBIC) {

		}(&tcpHBIC{ctxPkgPath, conn.LocalAddr().String(), conn})
	}
}

// Make instead of Connect as it can be disconnected & connected again and again
func MakeTCP(ctxPkgPath string, addr string) (hbic *tcpHBIC, err error) {
	hbic = &tcpHBIC{ctxPkgPath: ctxPkgPath, addr: addr}
	err = hbic.Connect()
	return
}

func (hbic *tcpHBIC) Connect() (err error) {
	raddr, err := net.ResolveTCPAddr("tcp", hbic.addr)
	if nil != err {
		log.Fatal("addr error", err)
		return
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if nil != err {
		log.Fatal("conn error", err)
		return
	}
	hbic.conn = conn
	return
}

func (hbic *tcpHBIC) Disconnect() (err error) {
	err = hbic.conn.Close()
	return
}

func (hbic *tcpHBIC) GetText(request string) (code string, err error) {

	raw, dir, err := hbic.GetRaw(request)
	if err != nil {
		return
	}
	if dir != "" {
		err = &WireError{fmt.Sprintf("Unexpected wire directive: [%s]", dir)}
	}

	code = raw

	return
}

func (hbic *tcpHBIC) GetJSON(request string, result interface{}) (err error) {

	raw, dir, err := hbic.GetRaw(request)
	if err != nil {
		return
	}
	if dir != "" {
		err = &WireError{fmt.Sprintf("Unexpected wire directive: [%s]", dir)}
	}

	err = json.Unmarshal([]byte(raw), result)
	if err != nil {
		return
	}

	return
}

func (hbic *tcpHBIC) GetRaw(request string) (response string, dir string, err error) {
	const LocalCoId = "1"

	if _, err = hbic.SendPacket(LocalCoId, "co_begin"); err != nil {
		log.Fatal("send error", err)
		return
	}

	response, dir, err = hbic.RecvPacket()
	if err != nil {
		log.Fatal("recv error", err)
		return
	}

	if "co_ack" != dir || LocalCoId != response {
		err = &WireError{fmt.Sprintf("Unexpected co response [#%s]%s", dir, response)}
		return
	}

	if _, err = hbic.SendPacket(request, "coget"); err != nil {
		log.Fatal("send error", err)
		return
	}

	if _, err = hbic.SendPacket(LocalCoId, "co_end"); err != nil {
		log.Fatal("send error", err)
		return
	}

	response, dir, err = hbic.RecvPacket()
	if err != nil {
		log.Fatal("recv error", err)
		return
	}
	if "" != dir {
		err = &WireError{fmt.Sprintf("Unexpected response [#%s]%s", dir, response)}
	}

	return
}

func (hbic *tcpHBIC) SendPacket(payload string, wireDir string) (n int64, err error) {
	header := fmt.Sprintf("[%v#%s]", len(payload), wireDir)
	bufs := net.Buffers{
		[]byte(header), []byte(payload),
	}
	n, err = bufs.WriteTo(hbic.conn)
	return
}

func (hbic *tcpHBIC) RecvPacket() (payload string, wireDir string, err error) {
	const MaxHeaderLen = 60
	var (
		n, start, newLen int
		hdrBuf           = make([]byte, 0, MaxHeaderLen)
		payloadBuf       []byte
	)

	// read header
	for {
		start = len(hdrBuf)
		n, err = hbic.conn.Read(hdrBuf[start:cap(hdrBuf)])
		if err != nil {
			return
		}
		newLen = start + n
		hdrBuf = hdrBuf[:newLen]
		for i, c := range hdrBuf[start:newLen] {
			if ']' == c {
				header := string(hdrBuf[0 : start+i+1])
				if '[' != header[0] {
					err = &WireError{fmt.Sprintf("Invalid header: %s", header)}
					return
				}
				lenEnd := strings.Index(header, "#")
				if -1 == lenEnd {
					err = &WireError{"No # in header!"}
					return
				}
				wireDir = string(header[lenEnd+1 : start+i])
				var payloadLen int
				fmt.Sscan(header[1:lenEnd], &payloadLen)
				if payloadLen < 0 {
					err = &WireError{"Negative payload length!"}
					return
				}
				chunkLen := newLen - i - 1
				payloadBuf = make([]byte, chunkLen, payloadLen)
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
			err = &WireError{fmt.Sprintf("No header within first %v bytes!", MaxHeaderLen)}
			return
		}
	}

	// read payload
	for len(payloadBuf) < cap(payloadBuf) {
		start = len(payloadBuf)
		n, err = hbic.conn.Read(payloadBuf[start:cap(payloadBuf)])
		newLen = start + n
		payloadBuf = payloadBuf[:newLen]
		if newLen >= cap(payloadBuf) {
			break
		}
	}
	payload = string(payloadBuf)

	return
}
