package rethinkdb

import (
	"fmt"
	"log"

	r "github.com/dancannon/gorethink"
)

func RethinkConnect(RethinkAddresses []string) (Rsession *r.Session) {
	Rsession, err := r.Connect(r.ConnectOpts{
		Addresses: RethinkAddresses,
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

	_, err = r.DB(rethinkdbname).TableCreate("accounts").RunWrite(Rsession)
	if err != nil {
		fmt.Println("probably already have a table of accounts")
	}

	_, err = r.DB(rethinkdbname).TableCreate("blocks").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for flat blocks")
	}
	return Rsession, err
}
