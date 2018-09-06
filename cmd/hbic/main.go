package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	img, err := os.Executable()
	if nil != err {
		log.Fatal("No executable?!")
	}
	cwd, err := os.Getwd()
	if nil != err {
		log.Fatal("No cwd?!")
	}

	fmt.Printf("CWD: %v\nARGS: %v\nIMG: %v\n", cwd, os.Args, img)
}
