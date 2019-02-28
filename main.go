// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	"time"

	"bitbucket.org/tradency_team/KubeVSnast/KubeMQvsnats/bench"
	kubemq "github.com/kubemq-io/kubemq-go"
)

// Some sane defaults
const (
	DefaultNumMsgs     = 100
	DefaultNumPubs     = 1
	DefaultNumSubs     = 1
	DefaultMessageSize = 128
	DefaultChannelName = "ido"
	DefaultKubeAddres  = "localhost:50000"
	DefaultClientName  = "newClient"
	DefaulType         = "e"
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

	benchmark = bench.NewBenchmark("KubeMQ", *numSubs, *numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs + *numSubs)

	// Run Subscribers first
	startwg.Add(*numSubs)
	address, port, err := splitServerCred(*kubeaAddress)
	if err != nil {
		log.Printf(err.Error())
		return
	}

	client, err := kubemq.NewClient(context.Background(),
		kubemq.WithAddress(address, port),
		kubemq.WithClientId(*clientName),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Printf(err.Error())
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

		go runPublisher(client, *channelName, &startwg, &donewg, pubCounts[i], *msgSize, *testpattern, *clientName)
	}

	log.Printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d testpattern=%s channel=%s client=%s]\n", *numMsgs, *msgSize, *numPubs, *numSubs, *testpattern, *channelName, *clientName)

	startwg.Wait()
	donewg.Wait()
	if *numSubs == 0 {
		time.Sleep(60 * time.Second)
	}

	benchmark.Close()

	fmt.Printf("\n")
	fmt.Print(benchmark.Report())

	if len(*csvFile) > 0 {
		csv := benchmark.CSV()
		ioutil.WriteFile(*csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", *csvFile)
	}
}

func runPublisher(client *kubemq.Client, channel string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string, clientName string) {

	startwg.Done()
	var body string
	if msgSize > 0 {
		body = randomString(msgSize)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	var goSend sync.WaitGroup
	goSend.Add(numMsgs)
	start := time.Now()
	//Event KubeMQ inmemory
	if pattern == "e" {
		for i := 0; i < numMsgs; i++ {

			go func(i int) {
				defer goSend.Done()
				err := client.E().
					SetId(clientName).
					SetChannel(channel).
					SetMetadata(strconv.Itoa(i)).
					SetBody([]byte(body)).Send(context.Background())
				if err != nil {
					fmt.Printf("Error innerSubscribeToEvents , %v", err)
				}

			}(i)
		}
	}
	//EventStore KubeMQ persistence
	if pattern == "es" {
		for i := 0; i < numMsgs; i++ {
			go func(i int) {
				defer goSend.Done()
				_, err := client.ES().
					SetId(clientName).
					SetChannel(channel).
					SetMetadata(strconv.Itoa(i)).
					SetBody([]byte(body)).
					Send(context.Background())
				if err != nil {
					fmt.Printf("Error innerSubscribeToEvents , %v", err)
				}

			}(i)

		}
	}

	//EventStoreStream KubeMQ inmemory stream
	if pattern == "est" {
		eventStreamCh := make(chan *kubemq.Event, 1)
		errStreamCh := make(chan error, 1)

		go client.StreamEvents(context.Background(), eventStreamCh, errStreamCh)

		event := client.E().SetId(clientName).
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetBody([]byte(body))

		go func() {
			for r := 0; r < numMsgs; r++ {
				select {
				case eventStreamCh <- event:
					goSend.Done()
				}
			}
		}()

	}

	//EventStoreStream KubeMQ persistence
	if pattern == "esst" {
		eventStreamCh := make(chan *kubemq.EventStore)
		eventStoreResmCh := make(chan *kubemq.EventStoreResult)
		errStreamCh := make(chan error, 1)

		go client.StreamEventsStore(context.Background(), eventStreamCh, eventStoreResmCh, errStreamCh)

		eventStore := client.ES().
			SetId(clientName).
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetBody([]byte(body))

		go func() {
			for r := 0; r < numMsgs; r++ {

				select {

				case eventStreamCh <- eventStore:
					goSend.Done()
				case err := <-errStreamCh:
					fmt.Printf("Error innerSubscribeToEvents , %v", err)

				}

			}
		}()

	}
	go func() {
		goSend.Wait()
		fmt.Printf("\n!!!!!!!!!!FinishWait!!!!!!!!!!!\n")
		benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now()))

		donewg.Done()
	}()
}

// type counter struct {
// 	value int64
// }

// func (c *counter) add(v int) {
// 	if v < 0 {
// 		panic(errors.New("counter cannot decrease in value"))
// 	}
// 	atomic.AddInt64(&c.value, int64(v))
// }

// func (c *counter) Write(perc int, numMsgs int, ch chan time.Time) {
// 	v := atomic.LoadInt64(&c.value)
// 	if v >= int64(numMsgs) || v == 1 {
// 		ch <- time.Now()
// 	}
// 	if v%int64(perc) == 0 {
// 		fmt.Printf("perc %d\r", v/int64(perc)*10)
// 	}
// 	if v > int64(numMsgs)-5 {
// 		fmt.Printf("lastMessages %d\r", v)
// 	}
// }

func runSubscriber(client *kubemq.Client, channelName string, group string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string) {

	counter := 0
	errCh := make(chan error)
	ch := make(chan time.Time, 2)
	mmperc := numMsgs / 10

	//events and event stream
	if pattern == "e" || pattern == "est" {
		eventCh, err := client.SubscribeToEvents(context.Background(), channelName, group, errCh)
		if err != nil {
			fmt.Printf("Error innerSubscribeToEvents , %v", err)
		}

		go func() {
			for {
				select {
				case err := <-errCh:
					fmt.Printf("Error lastMessages  %v", err)
				case <-eventCh:

					if counter == 0 || counter == numMsgs-1 {
						ch <- time.Now()
					}
					counter++
					if (counter % mmperc) == 0 {
						fmt.Printf("perc %d\r", counter/mmperc*10)

					}
					if counter > numMsgs-10 {
						fmt.Printf("lastMessages %d\r", counter)
					}

				}
			}
		}()
	}

	//eventsStore and event stream Store
	if pattern == "es" || pattern == "esst" {
		eventSCh, err := client.SubscribeToEventsStore(context.Background(), channelName, group, errCh, kubemq.StartFromNewEvents())

		if err != nil {
			fmt.Printf("Error innerSubscribeToEventsStore , %v", err)
		}
		go func() {
			for {
				select {
				case err := <-errCh:
					fmt.Printf("Error lastMessages  %v", err)
				case <-eventSCh:

					if counter == 0 || counter == numMsgs-1 {
						ch <- time.Now()
					}

					counter++

					if (counter % mmperc) == 0 {
						fmt.Printf("perc %d\r", counter/mmperc*10)

					}
					if counter > numMsgs-10 {
						fmt.Printf("lastMessages %d\r", counter)
					}

				}
			}
		}()
	}

	startwg.Done()
	start := <-ch
	end := <-ch

	benchmark.AddSubSample(bench.NewSample(numMsgs, msgSize, start, end))

	donewg.Done()

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
