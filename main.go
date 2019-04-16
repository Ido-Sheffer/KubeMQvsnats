package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Ido-Sheffer/KubeMQvsnats/bench"
	kubemq "github.com/kubemq-io/kubemq-go"
)

// Some sane defaults
const (
	DefaultNumMsgs     = 10
	DefaultNumPubs     = 1
	DefaultNumSubs     = 1
	DefaultMessageSize = 1024
	DefaultChannelName = "ido"
	DefaultKubeAddres  = "qarancher:31515"
	DefaultClientName  = "newClient"
	DefaulType         = "est"
)

var benchmark *bench.Benchmark

func main() {
	var kubeaAddress = flag.String("s", DefaultKubeAddres, "The KubeMQ server address (separated by comma)")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of Concurrent Publishers")
	var numSubs = flag.Int("ns", DefaultNumSubs, "Number of Concurrent Subscribers")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of Messages to Publish")
	var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message.")
	var csvFile = flag.String("csv", "", "Save bench data to csv file.")
	var channelName = flag.String("ch", DefaultChannelName+strconv.FormatInt(time.Now().Unix(), 10), "pubsub channel name")
	var clientName = flag.String("client", DefaultClientName+strconv.FormatInt(time.Now().Unix(), 10), "client name")
	var testpattern = flag.String("type", DefaulType, "e = pubsub, es = pubsub presistnace")

	log.SetFlags(0)
	flag.Parse()

	if *numMsgs <= 0 {
		log.Fatal("Number of messages should be greater than zero.")
	}

	//TODO validate testpattern

	benchmark = bench.NewBenchmark("KubeMQ", *numSubs, *numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs + *numSubs)

	// Run Subscribers first
	startwg.Add(*numSubs)

	/*patterens can be:
	e	->pub sub event
	est	->pubsub event with stream sender
	es	->pub sub event with presistance
	esst	->pub sub event with stream sender with presistance
	*/

	//get kube client or grpc client
	client, err := getClient(*kubeaAddress, *clientName, *testpattern)
	if err != nil {
		log.Printf("getClient %s", err.Error())
		return
	}
	for i := 0; i < *numSubs; i++ {
		go runSubscriber(client, *channelName, "", &startwg, &donewg, *numMsgs, *msgSize, *testpattern)
	}
	startwg.Wait()

	// Now Publishers
	startwg.Add(*numPubs)
	pubCounts := bench.MsgsPerClient(*numMsgs, *numPubs)

	for i := 0; i < *numPubs; i++ {
		go runPublisher(client, *channelName, *clientName, &startwg, &donewg, pubCounts[i], *msgSize, *testpattern)
	}

	log.Printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d testpattern=%s channel=%s client=%s]\n", *numMsgs, *msgSize, *numPubs, *numSubs, *testpattern, *channelName, *clientName)

	startwg.Wait()
	donewg.Wait()

	benchmark.Close()

	fmt.Printf("\n")
	fmt.Print(benchmark.Report())

	if len(*csvFile) > 0 {
		csv := benchmark.CSV()
		ioutil.WriteFile(*csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", *csvFile)
	}

}

func runPublisher(client *kubemq.Client, channel string, clientName string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string) {

	startwg.Done()
	var body string
	if msgSize > 0 {
		body = randomString(msgSize)
	}
	var a int64

	start := time.Now()
	end := time.Now()
	//Event KubeMQ inmemory

	finish := make(chan bool)

	if pattern == "e" {
		for i := 0; i < numMsgs; i++ {
			err := client.E().
				SetId(clientName).
				SetChannel(channel).
				SetMetadata(strconv.Itoa(i)).
				SetBody([]byte(body)).Send(context.Background())
			if err != nil {
				fmt.Printf("Error innerSubscribeToEvents , %v", err)
			}

		}
		end = time.Now()
	}
	//EventStore KubeMQ persistence
	if pattern == "es" {

		for i := 0; i < numMsgs; i++ {
			_, err := client.ES().
				SetId(clientName).
				SetChannel(channel).
				SetMetadata(strconv.Itoa(i)).
				SetBody([]byte(body)).
				Send(context.Background())
			if err != nil {
				fmt.Printf("Error innerSubscribeToEvents , %v", err)
			}

		}
		end = time.Now()
	}

	//Event stream  KubeMQ
	if pattern == "est" {

		eventStreamCh := make(chan *kubemq.Event, 1)
		errStreamCh := make(chan error, 1)

		go client.StreamEvents(context.Background(), eventStreamCh, errStreamCh)

		go func(numMsgs int64) {
			for {

				event := client.E().SetId(clientName).
					SetChannel(channel).
					SetMetadata("").
					SetBody([]byte(body))
				select {
				case eventStreamCh <- event:
					cnt := atomic.AddInt64(&a, 1)

					if cnt >= numMsgs {
						finish <- true
						return
					}
				case err := <-errStreamCh:
					fmt.Printf("Error innerSubscribeToEvents , %v", err)
				}
			}

		}(int64(numMsgs))

	}

	//EventStoreStream KubeMQ persistence
	if pattern == "esst" {
		eventStreamCh := make(chan *kubemq.EventStore, 1)
		eventStoreResmCh := make(chan *kubemq.EventStoreResult, 1)
		errStreamCh := make(chan error, 1)

		go client.StreamEventsStore(context.Background(), eventStreamCh, eventStoreResmCh, errStreamCh)

		eventStore := client.ES().
			SetId(clientName).
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetBody([]byte(body))

		for r := 0; r < numMsgs; r++ {

			select {

			case eventStreamCh <- eventStore:

			case err := <-errStreamCh:
				fmt.Printf("Error innerSubscribeToEvents , %v", err)

			}

		}

		end = time.Now()

	}
	<-finish
	end = time.Now()
	cnt := atomic.LoadInt64(&a)
	log.Printf("\nfinished publish %d", cnt)

	benchmark.AddPubSample(bench.NewSample(int(cnt), msgSize, start, end))

	donewg.Done()

}

func runSubscriber(client *kubemq.Client, channel string, group string, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string) {

	errCh := make(chan error)
	ch := make(chan time.Time, 2)
	var b int64
	//events and event stream
	if pattern == "e" || pattern == "est" {
		eventCh, err := client.SubscribeToEvents(context.Background(), channel, group, errCh)
		if err != nil {
			fmt.Printf("Error SubscribeToEvents , %s", err.Error())
		}

		go func(numMsgs int64, ch chan time.Time) {
			var cnt int64
			for {
				select {
				case <-eventCh:
					cnt = atomic.AddInt64(&b, 1)
					if cnt%10000 == 0 {
						fmt.Printf("perc %.f Messages %d\r", (float32(cnt) / float32(numMsgs) * 100), cnt)
					}
					if handleCounterAndStop(cnt, numMsgs, ch) {
						break
					}
				case err := <-errCh:
					fmt.Printf("\nError lastMessages  %s", err.Error())

				}
			}

		}(int64(numMsgs), ch)
	}

	//eventsStore and event stream Store
	if pattern == "es" || pattern == "esst" {
		eventSCh, err := client.SubscribeToEventsStore(context.Background(), channel, group, errCh, kubemq.StartFromNewEvents())

		if err != nil {
			fmt.Printf("Error SubscribeToEventsStore , %s", err.Error())
		}
		go func(numMsgs int64, ch chan time.Time) {
			for {
				select {
				case err := <-errCh:
					fmt.Printf("Error lastMessages  %s", err.Error())
					ch <- time.Now()
					return
				case <-eventSCh:
					cnt := atomic.AddInt64(&b, 1)

					if handleCounterAndStop(cnt, numMsgs, ch) {
						return
					}
				}
			}
		}(int64(numMsgs), ch)
	}

	startwg.Done()
	start := <-ch
	end := <-ch
	benchmark.AddSubSample(bench.NewSample(int(atomic.LoadInt64(&b)), msgSize, start, end))

	donewg.Done()

}

func handleCounterAndStop(cnt int64, numMsgs int64, ch chan time.Time) bool {

	if cnt >= numMsgs {
		ch <- time.Now()
		return true
	} else if cnt == 1 {
		ch <- time.Now()
		return false
	}

	// if cnt%100 == 0 || cnt >= (numMsgs-100) {
	// 	fmt.Printf("perc %.f Messages %d\r", (float32(cnt) / float32(numMsgs) * 100), cnt)
	// }
	if cnt == (numMsgs - 100) {
		go func() {
			timer1 := time.NewTimer(10 * time.Second)
			<-timer1.C
			fmt.Printf("finish publisher intetinally")
			ch <- time.Now()
		}()
	}
	return false

}

//Receive address and split it
func splitServerCred(a string) (server string, port int, err error) {
	fullAddress := strings.Split(a, ":")
	if len(fullAddress) != 2 {
		err = errors.New("Please make sure the format of the server name is {serverName}:{port} , localhost:50000")
		return "", 0, err
	}
	server = fullAddress[0]
	port, err = strconv.Atoi(fullAddress[1])
	if err != nil {
		return "", 0, err
	}
	return server, port, err
}

func randomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(65 + rand.Intn(25)) //A=65 and Z = 65+25
	}
	return string(bytes)
}

func getClient(address string, clientName string, testpattern string) (*kubemq.Client, error) {

	address, port, err := splitServerCred(address)
	if err != nil {
		log.Printf(err.Error())
		return nil, err
	}
	client, err := kubemq.NewClient(context.Background(),
		kubemq.WithAddress(address, port),
		kubemq.WithClientId(clientName),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Printf("kubemq.NewClient %s", err.Error())
		return nil, err
	}
	return client, nil

}
