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
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/astaxie/flatmap"
	r "github.com/dancannon/gorethink"
	"github.com/go-steem/rpc"
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

func hello(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello!"))
}

func steemServer(func serverConnect() []s string) []clni *rpc.Client {
	// Process flags.
	s := make([]string, 4)
	s[0] = "ws://138.201.198.167:8090"
	s[1] = "ws://138.201.198.169:8090"
	s[2] = "ws://138.201.198.173:8090"
	s[4] = "ws://138.201.198.175:8090"

	clnd := make([]*rpc.Client, 4)
	
	for _, s := range vs {
		vsm[i] = s(f)
	}
	

func serverConnect(s []string) clni []*rpc.Client {
		flagAddress := flag.String("rpc_endpoint", s, "steemd RPC endpoint address")
		flagReconnect := flag.Bool("reconnect", true, "enable auto-reconnect mode")
		flag.Parse()

		var (
			url       = *flagAddress
			reconnect = *flagReconnect
		)

		// Start catching signals.
		var interrupted bool
		var err error
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
		Startblock := 1
		U := uint32(Startblock)
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
		cln[i], err = rpc.NewClient(t)

		defer func() {
			if !interrupted {
				cln[i].Close()
			}
		}()

		// Start processing signals.
		go func() {
			<-signalCh
			fmt.Println()
			log.Println("Signal received, exiting...")
			signal.Stop(signalCh)
			interrupted = true
			cln[i].Close()
		}()
		return clni
	}
	

}

func init() {
	// Adds this file to Cobra's command list
	RootCmd.AddCommand(getitallCmd)

	http.HandleFunc("/", hello)
	http.ListenAndServe(":8080", nil)

	steemServer()([]clni *rpc.Client)

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

	_, err = r.DB(rethinkdbname).TableCreate("flatblocks").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for flat blocks")

	}

	_, err = r.DB(rethinkdbname).TableCreate("operations").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for flat blocks")

	}

	// Keep processing incoming blocks forever.
	fmt.Println("---> Entering the block processing loop")
	for {
		// Get current properties.
		props, err := client.Database.GetDynamicGlobalProperties()

		// Process new blocks.
		// This now explodes the JSON for each block.  This will flatten the nested arrays in the JSON.  Unsure if this will yeild the right result but it will be better.
		for _, servers := range Billy {
			for props.LastIrreversibleBlockNum > U {
				block, err := client.Database.GetBlock(U)
				blockraw, err := client.Database.GetBlockRaw(U)
				lastblock := props.LastIrreversibleBlockNum
				var data = blockraw
				var mp map[string]interface{}
				if err := json.Unmarshal([]byte(*data), &mp); err != nil {
					log.Fatal(err)
				}
				fm, err := flatmap.Flatten(mp)
				if err != nil {
					log.Fatal(err)
				}
				var ks []string
				for k := range fm {
					ks = append(ks, k)
				}
				sort.Strings(ks)
				for _, k := range ks {
					fmt.Println(k, ":", fm[k])
				}
				fmt.Println(U)
				//rethinkdb writes
				r.Table("transactions").
					Insert(block.Transactions).
					Exec(Rsession)
				r.Table("flatblocks").
					Insert(fm).
					Exec(Rsession)
				r.Table("nestedblocks").
					Insert(blockraw).
					Exec(Rsession)
				r.Table("blocks").
					Insert(block).
					Exec(Rsession)
				//crate.io writes
				db.Exec("create table transactions")
				db.Exec("insert into trasactions", block.Transactions)
				// Iterator:  This should work for doing a full dump, and then waiting 3 seconds each time.  We shall see.
				if U != lastblock {
					U++
				}
				if err != nil {
					fmt.Println(err)
				}

			}
			if err != nil {
				fmt.Println(err)
			}

		}

	}
}
