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
	"time"

	"github.com/faddat/steemjson/cmd/lib/rethinkdb"
	"github.com/faddat/steemjson/cmd/lib/websocket"

	r "github.com/dancannon/gorethink"
	"github.com/go-steem/rpc"

	"github.com/spf13/cobra"
)

//My alpha of what a full freakin block is like.  OMFG, the generator, dudes.... Swagger or gRPC.....
type BlockStruct struct {
	Result struct {
		BlockID               uint32
		Previous              string        `json:"previous,omitifempty"`
		Timestamp             string        `json:"timestamp,omitifempty"`
		Witness               string        `json:"witness,omitifempty"`
		TransactionMerkleRoot string        `json:"transaction_merkle_root,omitifempty"`
		Extensions            []interface{} `json:"extensions,omitifempty"`
		WitnessSignature      string        `json:"witness_signature,omitifempty"`
		Transactions          struct {
			Operations struct {
				OpType          string   `json:"operations.0,omitifempty"`
				Author          string   `json:"weenis,omitifempty"`
				Body            string   `json:"body,omitifempty"`
				PermLink        string   `json:"permlink,omitifempty"`
				Voter           string   `json:"voter,omitifempty"`
				Weight          string   `json:"weight,omitifempty"`
				JsonMetadata    string   `json:"json_metadata,omitifempty"`
				ParentAuthor    string   `json:"parent_author,omitifempty"`
				Permlink        string   `json:"permlink,omitifempty"`
				Title           string   `json:"title,omitifempty"`
				Owner           string   `json:"owner,omitifempty"`
				OpUrl           string   `json:"url,omitifempty"`
				BlockSigningKey string   `json:"block_signing_key,omitifempty"`
				KeyAuths        []string `json:"key_auths,omitifempty"`
				MemoKey         string   `json:"memo_key,omitifempty"`
				Fee             string   `json:"fee,omitifempty"`
				NewAccountName  string   `json:"new_account_name,omitifempty"`
				AccountAuths    []string `json:"account_auths,omitifempty"`
				WeightThreshold int      `json:"weight_threshold,omitifempty"`
				WorkerAccount   string   `json:"worker_account,omitifempty"`
				AccountUpdate   string   `json:"account_update,omitifempty"`
				Work            struct {
					Signature string `json:"signature,omitifempty"`
					Input     string `json:"input,omitifempty"`
					WorkWork  string `json:"work.work,omitifempty"`
					Worker    string `json:"worker,omitifempty"`
				} `json:"work,omitifempty"`
				OperationProps struct {
					AccountCreationFee string `json:"account_creation_fee,omitifempty"`
					MaximumBlockSize   uint32 `json:"maximum_block_size,omitifempty"`
					SbdInterestRate    uint16 `json:"sbd_interest_rate,omitifempty"`
				} `json:"props,omitifempty"`
			}
			Signatures []string `json:"signatures,omitifempty"`
		}
		SigningKey string `json:"signing_key,omitifempty"`
	}
}

var getitallCmd = &cobra.Command{
	Use:   "getitall",
	Short: "This will get the whole blockchain and write it to rethinkdb",
	Long:  `cooter!`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("get it all")
	},
}

func init() {
	var err error
	//Adds getitall to cobra command list
	RootCmd.AddCommand(getitallCmd)
	//vars for the strange as hell flag parse-thing going on here for the overall strange as hell websocket situation
	//No idea why this i sset up the way it is, but the middle variable is a steemd server address
	flagAddress := flag.String("rpc_endpoint", "ws://138.201.198.167:8090", "steemd RPC endpoint address")
	//I haven't played with the reconect setting because I hven't had disconnect issues
	flagReconnect := flag.Bool("reconnect", false, "enable auto-reconnect mode")
	flag.Parse()
	var (
		url       = *flagAddress
		reconnect = *flagReconnect
	)
	//gets the websockets over http client
	//below is a slice of strings containing rethinkdb server addresses
	RethinkAddresses := []string{"138.201.198.167:28015", "138.201.198.169:28015", "138.201.198.173:28015"}
	Rsession := rethinkconnect.RethinkConnect(RethinkAddresses)
	Client := steemrpc.SteemSocket(url, reconnect, err)
	//connects us to rethinkdb
	fmt.Println("all finished time to start processing blocks!")

	//Infinitely loops at the getblock function.  Once the iterator reaches the last irreversible block number introduce one second pauses.
	I := uint32(1)
	props, _ := Client.Database.GetDynamicGlobalProperties()
	for {
		block, err := Client.Database.GetBlockRaw(I)
		var knockers BlockStruct
		json.Unmarshal(*block, knockers)
		knockers.Result.BlockID = I
		fmt.Println(I)
		fmt.Println(&knockers)
		writeBlock(knockers, Rsession)
		if err != nil {
			fmt.Println(err)
		}
		if I < props.LastIrreversibleBlockNum {
			I++
		} else {
			time.Sleep(3000 * time.Millisecond)
		}

	}
}

//Accepts the block structure, and writes that to rethinkdb.
func writeBlock(knockers BlockStruct, Rsession *r.Session) {
	//rethinkdb writes
	r.Table("blocks").
		Insert(knockers).
		Exec(Rsession)
}
