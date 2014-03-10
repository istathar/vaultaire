package main

import (
	zmq "github.com/pebbe/zmq4"
	"flag"
	"os"
	"fmt"
)

func Exitf(code int, format string, v... interface{}) {
	fmt.Fprintf(os.Stderr, "Fatal: ")
	fmt.Fprintf(os.Stderr, format, v...)
	os.Exit(code)
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s <origin> [opts]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}
	uri := flag.String("endpoint", "tcp://127.0.0.1:5572", "ZMQ URI of contents endpoint")
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}
	origin := flag.Arg(0)
	if len(origin) != 6 {
		Exitf(1, "Invalid origin.")
	}
	sock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		Exitf(2, "Could not create ZMQ socket: %v", err)
	}
	err = sock.Connect(*uri)
	if err != nil {
		Exitf(2, "Could not connect to %s: %v", uri, err)
	}
	_, err = sock.Send(origin, 0)
	if err != nil {
		Exitf(2, "Could not send: %v", uri, err)
	}
	contents, err := sock.RecvBytes(0)
	if err != nil {
		fmt.Printf("Error reading contents: %v", err)
	}
	os.Stdout.Write(contents)
}
