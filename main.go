package main

import (
	"log"
	"net"
	"time"
)

func main() {
	l, err := net.Listen("tcp", ":1883")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
			time.Sleep(time.Millisecond * 5)
			continue
		}
		go HandConn(conn)
	}
}
