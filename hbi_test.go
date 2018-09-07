package hbi

import (
	"os"
	"testing"
	"text/template"
)

func TestMain(m *testing.M) {

	ListenTCP(func() Context {
		return NewContext()
	}, "127.0.0.1:3232")

	os.Exit(m.Run())
}

func TestGreeting(t *testing.T) {
	hbic, err := MakeTCP(NewContext(), "127.0.0.1:3232")
	defer CoDone(hbic.Co())

	type Inventory struct {
		Material string
		Count    uint
	}
	sweaters := Inventory{"wool", 17}
	tmpl, err := template.New("test").Parse("{{.Count}} items are made of {{.Material}}")
	if err != nil {
		panic(err)
	}
	err = tmpl.Execute(os.Stdout, sweaters)
	if err != nil {
		panic(err)
	}

	t.Log("Hello")
}
