# KubeMQvsnats
A benchmark test forked from go-nats(https://github.com/nats-io/go-nats/tree/master/examples/nats-bench) to compare pub sub KubeMQ to nats performance and pure GRPC performance

## Use cases
KubeMQvsnats can be runned in 2 main options:
* [KubeMQ] benchmark pub sub patterens using GRPC to KubeMQ server 
* [grpc] using identical proto of kubemq will becnhmark pub sub messages on local grpc server

### grpc example
Running Grpc server +pub +sub
```
KubeMQvsnats.exe -grpc 
```
Running Grpc server only
```
KubeMQvsnats.exe -grpc -np 0 -ns =0
```
### KubeMQ example
* make sure KubeMQ is running. (default address for grpc on KubeMQ is localhost:5000)
```
KubeMQvsnats.exe  
```
Running presistance
```
KubeMQvsnats.exe  -type est
```

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
  * [grpc]->stream event with emulated server
  * [grpcst]->event with emulated server
  * [e]	->pub sub event
  * [es] ->pubsub event with stream sender
  * [est] ->pub sub event with presistance
  * [esst] ->pub sub event with stream sender with presistance
	

