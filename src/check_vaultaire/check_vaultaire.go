/*
check_vaultaire connects to an ingestd monitoring socket and requests
metrics. 
*/
package main

import (
	"flag"
	"strings"
	"github.com/fractalcat/nagiosplugin"
	"math"
	"strconv"
	zmq "github.com/pebbe/zmq4"
)


func main() {
	check := nagiosplugin.NewCheck()
	defer check.Finish()
	ingestdEndpoint := flag.String("ingestd", "tcp://localhost:5571", "ingestd monitoring endpoint")
	flag.Parse()
	sock, err := zmq.NewSocket(zmq.REQ)
	if err != nil {
		check.Criticalf("could not create req socket: %v", err)
	}
	sock.Connect(*ingestdEndpoint)
	_, err = sock.Send("", 0)
	if err != nil {
		check.Criticalf("Could not send to monitoring socket")
	}
	msg, err := sock.RecvMessage(0)
	if err != nil {
		check.AddResult(nagiosplugin.UNKNOWN, "didn't get a reply from ingestd")
	}
	for _, line := range msg {
		pair := strings.Split(line, ":")
		key := pair[0]
		value, err := strconv.ParseFloat(pair[1], 64)
		if err != nil {
			check.AddResult(nagiosplugin.WARNING, "could not parse metrics")
			continue
		}
		check.AddPerfDatum(key, "c", value, 0.0, math.Inf(1))
	}
	check.AddResult(nagiosplugin.OK, "ingestd seems happy")
}
