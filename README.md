# KubeMQvsnats
A benchmark test forked from go-nats(https://github.com/nats-io/go-nats/tree/master/examples/nats-bench) to compare KubeMQ performance and pure GRPC performance

## Use cases
KubeMQvsnats can be runned in 2 main options:
* [KubeMQ] benchmark pub sub patterens on a KubeMQ server 
* [grpc] using identical proto of kubemq will becnhmark pub sub messages on local grpc server

KubeMQvsnats running varialbes 
* [s]  The KubeMQ or grpc server address (separated by comma)")
* [np] Number of Concurrent Publishers)
* [ns] Number of Concurrent Subscribers
* [n] Number of Messages to Publish
* [ms] Size of the message.
* [csv] Save bench data to csv file.
* [ch] pubsub channel name.
* [client] benchamrk client name
* [type] benchamrk running mode:
  * [grpc]->event with emulated server
  * [e]	->pub sub event
  * [es]	->pubsub event with stream sender
  * [est]	->pub sub event with presistance
  * [esst]	->pub sub event with stream sender with presistance
	
### grpc example
running with the default values of tester (	DefaultNumMsgs     = 10000,	
                                            DefaultNumPubs     = 1,	
                                            DefaultNumSubs     = 1,	
                                            DefaultMessageSize = 128,	
                                            DefaultChannelName = "ido",	
                                            DefaultKubeAddres  = "localhost:50000",	
                                            DefaultClientName  = "newClient"
```
KubeMQvsnats.exe -grpc 
```
