package mux

import "time"

type Evt struct {
	Ts    int64   `json:"ts,omitempty" bson:"ts,omitempty"`
	Name  string  `json:"name,omitempty" bson:"name,omitempty"`
	Value float64 `json:"value,omitempty" bson:"value,omitempty"`
}

func NewEvt(
	name string, value float64,
) *Evt {
	return &Evt{
		Ts:   int64(time.Now().Unix()),
		Name: name, Value: value,
	}
}
