package websocket

import (
  "flag"
  "github.com/go-steem/rpc"
  "github.com/go-steem/rpc/transports/websocket"
)

func WebSocket(flagAddress *flag.String, flagReconnect *flag.Bool)(Client *rpc.NewClient) {

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
  go func() {
		<-signalCh
		fmt.Println()
		log.Println("Signal received, exiting...")
		signal.Stop(signalCh)
		interrupted = true
		Client.Close()
	}()
  return Client

}
}
