/*
Chat server for demonstration & diagnostics purpose
*/
package chat

import (
	"fmt"
	"github.com/complyue/hbigo"
	"github.com/golang/glog"
	"sync"
	"time"
)

const (
	MaxHist = 10
)

type Msg struct {
	Content    string
	Time       time.Time
	Prev, Next *Msg
}

func (msg *Msg) String() string {
	return fmt.Sprintf("[%s] %s", msg.Time.Format("2006-01-02T15:04:05"), msg.Content)
}

type MsgHist struct {
	mu                sync.Mutex
	firstMsg, lastMsg *Msg
	numMsgs           int
	observers         map[hbi.Context]struct{}
}

func (room *MsgHist) Chat(content string) {
	room.mu.Lock()
	defer room.mu.Unlock()

	now := time.Now()
	if room.numMsgs <= 0 {
		if room.firstMsg != nil || room.lastMsg != nil {
			panic("?!")
		}
		room.firstMsg = &Msg{content, now, nil, nil}
		room.lastMsg = room.firstMsg
	} else {
		if room.lastMsg == nil {
			panic("?!")
		}
		for ; room.numMsgs >= MaxHist; room.numMsgs-- {
			room.firstMsg = room.firstMsg.Next
			room.firstMsg.Prev = nil
		}
		room.lastMsg.Next = &Msg{content, now, room.lastMsg, nil}
		room.lastMsg = room.lastMsg.Next
	}
	room.numMsgs++
}

func (room *MsgHist) MsgLog() []string {
	msgs := make([]string, room.numMsgs)
	for msg := room.firstMsg; nil != msg; msg = msg.Next {
		msgs = append(msgs, msg.String())
	}
	return msgs
}

var (
	lobby = &MsgHist{}
	rooms map[string]*MsgHist
	mu    sync.Mutex
)

func getRoom(session string) (room *MsgHist) {
	room = lobby
	if "" != session {
		mu.Lock()
		defer mu.Unlock()
		room, ok := rooms[session]
		if !ok {
			room = &MsgHist{}
			rooms[session] = room
		}
	}
	return
}

func Chat(hbic hbi.Context, content string, session string) {
	glog.V(1).Infof(" ** Chat ** msg to room [%s] content: %s", session, content)
	room := getRoom(session)
	room.Chat(content)

	hbic.Peer().Fire(`
println(` + "`" +
		fmt.Sprintf("sent to %v observer(s)", len(room.observers)) +
		"`" + `)
`)
}

func MsgLog(session string) []string {
	room := getRoom(session)
	return room.MsgLog()
}

func LastMsg(session string) string {
	room := getRoom(session)
	if nil == room.lastMsg {
		return ""
	}
	return room.lastMsg.String()
}
