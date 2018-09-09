/*
Chat server for demonstration & diagnostics purpose
*/
package chat

import (
	"fmt"
	"github.com/complyue/hbigo"
	"strings"
	"sync"
	"time"
)

const (
	MaxHist = 10
)

type Msg struct {
	From       string
	Content    string
	Time       time.Time
	Prev, Next *Msg
}

func (msg *Msg) String() string {
	return fmt.Sprintf(
		"[%s]%s: %s",
		msg.Time.Format("2006-01-02T15:04:05"),
		msg.From, msg.Content,
	)
}

type Room struct {
	sync.Mutex        // embed a mutex
	name              string
	firstMsg, lastMsg *Msg
	numMsgs           int
	stayers           map[*ChatContext]struct{}
}

func NewRoom(id string) *Room {
	return &Room{
		name:    "Room#" + id,
		stayers: make(map[*ChatContext]struct{}),
	}
}

func (room *Room) Post(from, content string) {
	room.Lock()
	defer room.Unlock()

	now := time.Now()
	msg := &Msg{from, content, now, room.lastMsg, nil}
	if room.numMsgs <= 0 {
		if room.firstMsg != nil || room.lastMsg != nil {
			panic("?!")
		}
		room.firstMsg = msg
		room.lastMsg = room.firstMsg
	} else {
		if room.lastMsg == nil {
			panic("?!")
		}
		for ; room.numMsgs >= MaxHist; room.numMsgs-- {
			room.firstMsg = room.firstMsg.Next
			room.firstMsg.Prev = nil
		}
		room.lastMsg.Next = msg
		room.lastMsg = room.lastMsg.Next
	}
	room.numMsgs++

	for stayer := range room.stayers {
		if stayer.inRoom != room {
			// went to another room, forget it
			delete(room.stayers, stayer)
		} else if p2p := stayer.PoToPeer(); p2p != nil {
			// concurrently push new msg to it, forget it if err in notifying
			go func() {
				defer func() {
					if err := recover(); err != nil {
						room.Lock()
						defer room.Unlock()
						delete(room.stayers, stayer)
					}
				}()
				p2p.Notif(fmt.Sprintf(`
println(%#v, " ", %#v)
`, room.name, msg.String()))
			}()
		} else {
			// disconnected or so, forget it
			delete(room.stayers, stayer)
		}
	}
}

func (room *Room) MsgLog() []string {
	msgs := make([]string, room.numMsgs)
	for msg := room.firstMsg; nil != msg; msg = msg.Next {
		msgs = append(msgs, msg.String())
	}
	return msgs
}

// server globals
var (
	mu    sync.Mutex
	lobby = NewRoom("Loggy")
	rooms = make(map[string]*Room)
)

func init() {
	rooms[lobby.name] = lobby
}

func prepareRoom(roomId string) (room *Room) {
	room = lobby
	if "" != roomId {
		mu.Lock()
		defer mu.Unlock()
		room, ok := rooms[roomId]
		if !ok {
			room = NewRoom(roomId)
			rooms[roomId] = room
		}
	}
	return
}

// one hosting context for each and every client connections
type ChatContext struct {
	hbi.HoContext

	nick   string
	inRoom *Room
}

func NewChatContext() hbi.HoContext {
	return &ChatContext{
		HoContext: hbi.NewHoContext(),
		nick:      "stranger",
		inRoom:    lobby,
	}
}

// intercept p2p to initialize nick name from network identity, and show this
// newly connected client to lobby
func (ctx *ChatContext) SetPoToPeer(p2p hbi.Posting) {
	ctx.HoContext.SetPoToPeer(p2p)
	if p2p != nil {
		ctx.nick = p2p.NetIdent()
		ctx.Goto("") // goto lobby
	}
}

func (ctx *ChatContext) Goto(roomId string) {
	room := prepareRoom(roomId)
	ctx.inRoom = room
	if p2p := ctx.PoToPeer(); p2p != nil {
		// post recent msgs and confirm msg in one packet,
		// it will land atomically in peer's hosting context.
		p2p.Notif(fmt.Sprintf(`
println("Welcome %s, you are in %s now.");
println(%#v)
println("%d other(s) in this room now.")
`,
			ctx.nick, room.name,
			strings.Join(room.MsgLog(), "\n"),
			len(room.stayers),
		))
		room.stayers[ctx] = struct{}{}
	}
}

func (ctx *ChatContext) Say(msg string) {
	ctx.inRoom.Post(ctx.nick, msg)
}
