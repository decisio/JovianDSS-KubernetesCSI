package main

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"

	"JovianDSS-KubernetesCSI/pkg/rest"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	//log.SetLevel(log.WarnLevel)
}

func main() {
	var version = "testing"
	var logger = log.New()
	logger.SetLevel(log.DebugLevel)

	logger.Out = os.Stdout
	l := logger.WithFields(log.Fields{
		"version": version,
	})
	opt := rest.RestProxyCfg{
		Addr:  "85.14.118.246",
		Port:  11582,
		Prot:  "https",
		User:  "admin",
		Pass:  "Rtew2456Xcp5#",
		Tries: 3,
	}
	rproxy, _ := rest.NewRestProxy(opt, l)
	fmt.Println("Opt %+v", opt)
	fmt.Println("Rproxy %+v", rproxy)

	status, body, err := rproxy.Send("GET", "api/v3/pools", nil, rest.GetPoolsRCode)

	var gprsp = &rest.GetPoolsData{}
	if err := json.Unmarshal(body, &gprsp); err != nil {
		panic(err)
	}
	for poolN := range gprsp.Data {
		fmt.Println("JSon %+v", gprsp.Data[poolN].Name)
	}
	fmt.Println("Status %+v", status)

	if err != nil {
		fmt.Println("Erro %+v", err)
	}
}
