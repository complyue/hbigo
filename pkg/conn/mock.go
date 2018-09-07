package conn

import (
	"encoding/json"
	"fmt"
	"log"
)

func (hbic *tcpHBIC) GetText(request string) (code string, err error) {

	raw, err := hbic.GetRaw(request)
	if err != nil {
		return
	}

	code = raw

	return
}

func (hbic *tcpHBIC) GetJSON(request string, result interface{}) (err error) {

	raw, err := hbic.GetRaw(request)
	if err != nil {
		return
	}

	err = json.Unmarshal([]byte(raw), result)
	if err != nil {
		return
	}

	return
}

func (hbic *tcpHBIC) GetRaw(request string) (response string, err error) {
	const LocalCoId = "1"
	var pkt *Packet

	if _, err = hbic.SendPacket(LocalCoId, "co_begin"); err != nil {
		log.Fatal("send error", err)
		return
	}

	pkt, err = hbic.RecvPacket()
	if err != nil {
		log.Fatal("recv error", err)
		return
	}

	if "co_ack" != pkt.WireDir || LocalCoId != response {
		err = &WireError{fmt.Sprintf("Unexpected co response %s", pkt)}
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

	pkt, err = hbic.RecvPacket()
	if err != nil {
		log.Fatal("recv error", err)
		return
	}
	if "" != pkt.WireDir {
		err = &WireError{fmt.Sprintf("Unexpected response %s", pkt)}
	}

	response = pkt.Payload
	return
}
