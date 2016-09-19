// Copyright © 2016 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	r "github.com/dancannon/gorethink"
	"github.com/go-steem/rpc"
	d "github.com/go-steem/rpc/apis/database"
	"github.com/go-steem/rpc/transports/websocket"
	_ "github.com/herenow/go-crate"
	"github.com/spf13/cobra"
)

var getitallCmd = &cobra.Command{
	Use:   "getitall",
	Short: "This will get the whole blockchain and write it to rethinkdb",
	Long:  `cooter!`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Hugo Static Site Generator v0.9 -- HEAD")
	},
}

func init() {

	RootCmd.AddCommand(getitallCmd)

	Rsession, err := r.Connect(r.ConnectOpts{
		Addresses: []string{"138.201.198.167:28015", "138.201.198.169:28015", "138.201.198.173:28015", "138.201.198.175:28015"},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Create a table in the DB
	var rethinkdbname string = "steem"
	_, err = r.DBCreate(rethinkdbname).RunWrite(Rsession)
	Rsession.Use(rethinkdbname)
	if err != nil {
		fmt.Println("rethindb DB already made")
	}

	_, err = r.DB(rethinkdbname).TableCreate("transactions").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for transactions")

	}

	_, err = r.DB(rethinkdbname).TableCreate("blocks").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for flat blocks")

	}

	// Process flags.
	flagAddress := flag.String("rpc_endpoint", "ws://138.201.198.169:8090", "steemd RPC endpoint address")
	flagReconnect := flag.Bool("reconnect", false, "enable auto-reconnect mode")
	flag.Parse()

	var (
		url       = *flagAddress
		reconnect = *flagReconnect
	)

	// Start catching signals.
	var interrupted bool
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Drop the error in case it is a request being interrupted.
	defer func() {
		if err == websocket.ErrClosing && interrupted {
			err = nil
		}
	}()
	// This allows you to tell the app which block to start on.
	// TODO: Make all of the vars into a config file and package the binaries
	// Start the connection monitor.
	monitorChan := make(chan interface{}, 1)
	if reconnect {
		go func() {
			for {
				event, ok := <-monitorChan
				if ok {
					log.Println(event)
				}
			}
		}()
	}

	// Instantiate the WebSocket transport.
	log.Printf("---> Dial(\"%v\")\n", url)
	t, err := websocket.NewTransport(url,
		websocket.SetAutoReconnectEnabled(reconnect),
		websocket.SetAutoReconnectMaxDelay(30*time.Second),
		websocket.SetMonitor(monitorChan))

	// Use the transport to get an RPC client.
	Client, err := rpc.NewClient(t)

	defer func() {
		if !interrupted {
			Client.Close()
		}
	}()

	// Start processing signals.
	go func() {
		<-signalCh
		fmt.Println()
		log.Println("Signal received, exiting...")
		signal.Stop(signalCh)
		interrupted = true
		Client.Close()
	}()

	// Keep processing incoming blocks forever.
	fmt.Println("---> Entering the block processing loop")
	for {
		// Get current properties.
		props, err := Client.Database.GetDynamicGlobalProperties()
		if err != nil {
			fmt.Println(err)
		}

		// Process blocks.
		for I := uint32(1); I <= props.LastIrreversibleBlockNum; I++ {
			go getblock(I, Client, Rsession)
		}
		if err != nil {
			fmt.Println(err)
		}

	}

}

func getblock(I uint32, Client *rpc.Client, Rsession *r.Session) {
	block, err := Client.Database.GetBlock(I)
	fmt.Println(I)
	writeBlock(block, Rsession)
	if err != nil {
		fmt.Println(err)
	}
}

func writeBlock(block *d.Block, Rsession *r.Session) {
	//rethinkdb writes
	r.Table("transactions").
		Insert(block.Transactions).
		Exec(Rsession)
	r.Table("blocks").
		Insert(block).
		Exec(Rsession)
}
