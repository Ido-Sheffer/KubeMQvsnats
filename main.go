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
	DefaultChannelName = "newChannel"
	DefaultKubeAddres  = "localhost:50000"
	DefaultClientName  = "newClient"
	DefaulType         = "es"
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
	client, err := createKubmeMQClient(address, port, *clientName)
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

		go runPublisher(client, *channelName, &startwg, &donewg, pubCounts[i], *msgSize, *testpattern, *channelName)
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

func runPublisher(client *kubemq.Client, channel string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string, clientName string) {
	startwg.Done()

	var body string
	if msgSize > 0 {
		body = randomString(msgSize)
	}

	start := time.Now()
	//Event KubeMQ inmemory
	if pattern == "e" {
		for i := 0; i < numMsgs; i++ {

			go func(i int) {
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
			go func(r int) {
				_, err := client.ES().
					SetId(clientName).
					SetChannel(channel).
					SetMetadata(strconv.Itoa(r)).
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

				}
			}
		}()
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

		go func() {
			for r := 0; r < numMsgs; r++ {
				select {
				case eventStreamCh <- eventStore:
				case err := <-errStreamCh:
					fmt.Printf("Error innerSubscribeToEvents , %v", err)
					//	case ret := <-eventStoreResmCh:
					//	fmt.Printf("return innerSubscribeToEvents , %v", ret)

				}
			}
		}()

	}

	benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now()))

	donewg.Done()
}

func runSubscriber(client *kubemq.Client, channelName string, group string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string) {

	received := 0
	errCh := make(chan error)
	//ch := make(chan time.Time, 2)
	mmperc := numMsgs / 10
	var finish sync.WaitGroup
	finish.Add(1)
	start := time.Now()
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
					fmt.Printf("Error lastMessages %d, %v", received, err)
				case <-eventCh:
					received++
					if received == numMsgs {
						finish.Done()
					}
					go logAndChanel(received, mmperc, numMsgs)
					// if logAndChanel(received, mmperc, numMsgs) {
					// 	ch <- time.Now()
					// }

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
					fmt.Printf("Errir lastMessages %d, %v", received, err)
				case <-eventSCh:
					received++

					if logAndChanel(received, mmperc, numMsgs) {
						//ch <- time.Now()
					}
					if received == numMsgs {
						finish.Done()
					}
				}
			}
		}()
	}

	startwg.Done()
	finish.Wait()

	//start := <-ch
	end := time.Now()
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

func createKubmeMQClient(serverAdd string, serverPort int, name string) (*kubemq.Client, error) {

	client, err := kubemq.NewClient(context.Background(),
		kubemq.WithAddress(serverAdd, serverPort),
		kubemq.WithClientId(name),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	return client, err
}
func logAndChanel(counter int, perc int, numMsgs int) bool {
	if counter%perc == 0 {
		fmt.Printf("perc %d\r", counter/perc*10)
	}
	if counter > numMsgs-5 {
		fmt.Printf("lastMessages %d\r", counter)
	}
	if counter == 1 || counter >= numMsgs {
		return true
	}

	return false

}
