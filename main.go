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
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Ido-Sheffer/KubeMQvsnats/bench"
	grpcKubeMQEmu "github.com/Ido-Sheffer/KubeMQvsnats/grpc"
	pb "github.com/Ido-Sheffer/KubeMQvsnats/grpc/pkg/pb"
	kubemq "github.com/kubemq-io/kubemq-go"
)

// Some sane defaults
const (
	DefaultNumMsgs     = 10000
	DefaultNumPubs     = 1
	DefaultNumSubs     = 1
	DefaultMessageSize = 128
	DefaultChannelName = "ido"
	DefaultKubeAddres  = "localhost:50001"
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

	//TODO validate testpattern

	benchmark = bench.NewBenchmark("KubeMQ", *numSubs, *numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs + *numSubs)

	// Run Subscribers first
	startwg.Add(*numSubs)

	/*patterens can be:
	grpc->event with emulated server
	grpcst->event stream with emulated server
	e	->pub sub event
	es	->pubsub event with stream sender
	est	->pub sub event with presistance
	esst	->pub sub event with stream sender with presistance
	*/

	//run GRPC server

	if *testpattern == "grpc" || *testpattern == "grpcst" {

		if (*numPubs == 0 && *numPubs == 0) || (*numPubs >= 1 && *numPubs >= 1) {

			_, port, err := splitServerCred(*kubeaAddress)
			if err != nil {
				log.Printf(err.Error())
				return
			}
			err = grpcKubeMQEmu.RunServer(strconv.Itoa(port))
			if err != nil {
				log.Printf("grpcKubeMQEmu.RunServer %s", err.Error())
				return
			}
		}
	}

	//get kube client or grpc client
	client, grpcclient, err := getClient(*kubeaAddress, *clientName, *testpattern)
	if err != nil {
		log.Printf("getClient %s", err.Error())
		return
	}

	for i := 0; i < *numSubs; i++ {
		if *testpattern == "grpc" || *testpattern == "grpcst" {
			go runSubscriberGRPC(grpcclient, *channelName, *clientName, &startwg, &donewg, *numMsgs, *msgSize)
		} else {
			go runSubscriber(client, *channelName, "", &startwg, &donewg, *numMsgs, *msgSize, *testpattern)
		}
	}
	startwg.Wait()

	// Now Publishers
	startwg.Add(*numPubs)
	pubCounts := bench.MsgsPerClient(*numMsgs, *numPubs)
	for i := 0; i < *numPubs; i++ {
		if *testpattern == "grpc" || *testpattern == "grpcst" {

			go runPublisherGRPC(grpcclient, *channelName, *clientName, &startwg, &donewg, pubCounts[i], *msgSize, *testpattern)
		} else {
			go runPublisher(client, *channelName, *clientName, &startwg, &donewg, pubCounts[i], *msgSize, *testpattern)
		}
	}

	log.Printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d testpattern=%s channel=%s client=%s]\n", *numMsgs, *msgSize, *numPubs, *numSubs, *testpattern, *channelName, *clientName)

	startwg.Wait()
	donewg.Wait()
	if *numSubs == 0 {

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		done := make(chan bool, 1)
		go func() {
			for {
				select {
				case <-sigs:
					close(done)
					return
				}
			}
		}()
		<-done
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

func runPublisher(client *kubemq.Client, channel string, clientName string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string) {

	startwg.Done()
	var body string
	if msgSize > 0 {
		body = randomString(msgSize)
	}

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
		benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now()))

		donewg.Done()
	}()
}

func runSubscriber(client *kubemq.Client, channel string, group string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string) {

	counter := 0
	errCh := make(chan error)
	ch := make(chan time.Time, 2)

	//events and event stream
	if pattern == "e" || pattern == "est" {
		eventCh, err := client.SubscribeToEvents(context.Background(), channel, group, errCh)
		if err != nil {
			fmt.Printf("Error SubscribeToEvents , %s", err.Error())
		}

		go func(counter int, numMsgs int, ch chan time.Time) {
			for {
				select {
				case err := <-errCh:
					fmt.Printf("Error lastMessages  %s", err.Error())
				case <-eventCh:
					counter++
					if handleCounterAndStop(counter, numMsgs, ch) {
						return
					}

				}
			}
		}(counter, numMsgs, ch)
	}

	//eventsStore and event stream Store
	if pattern == "es" || pattern == "esst" {
		eventSCh, err := client.SubscribeToEventsStore(context.Background(), channel, group, errCh, kubemq.StartFromNewEvents())

		if err != nil {
			fmt.Printf("Error SubscribeToEventsStore , %s", err.Error())
		}
		go func(counter int, numMsgs int, ch chan time.Time) {
			for {
				select {
				case err := <-errCh:
					fmt.Printf("Error lastMessages  %s", err.Error())
					ch <- time.Now()
					return
				case <-eventSCh:
					counter++
					if handleCounterAndStop(counter, numMsgs, ch) {
						return
					}
				}
			}
		}(counter, numMsgs, ch)
	}
	time.Sleep(5 * time.Second)
	startwg.Done()
	start := <-ch
	end := <-ch

	benchmark.AddSubSample(bench.NewSample(numMsgs, msgSize, start, end))

	donewg.Done()

}

func runPublisherGRPC(client pb.KubemqClient, channel string, clientName string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int, pattern string) {

	startwg.Done()
	var body string
	if msgSize > 0 {
		body = randomString(msgSize)
	}

	var goSend sync.WaitGroup
	goSend.Add(numMsgs)
	start := time.Now()

	//Event KubeMQ inmemory
	if pattern == "grpc" {

		for i := 0; i < numMsgs; i++ {

			go func(i int) {
				defer goSend.Done()

				result, err := client.SendEvent(context.Background(), &pb.Event{
					EventID:  clientName + strconv.Itoa(i),
					ClientID: clientName,
					Channel:  channel,
					Metadata: strconv.Itoa(i),
					Body:     []byte(body),
					Store:    false,
				})
				if err != nil {
					fmt.Printf("Error runPublisherGRPC  SendEvent, %s", err.Error())
					return
				}
				if !result.Sent {
					fmt.Printf("Error runPublisherGRPC  SendEvent, %s", err.Error())
					return
				}

			}(i)
		}
	}
	if pattern == "grpcst" {
		streamCtx, _ := context.WithCancel(context.Background())
		//defer cancel()
		stream, err := client.SendEventsStream(streamCtx)
		if err != nil {
			fmt.Printf("Error runPublisherGRPC  SendEvent, %s", err.Error())
			return
		}
		//defer stream.CloseSend()
		go func() {
			for {

				result, err := stream.Recv()
				if err != nil {
					fmt.Printf("Error runPublisherGRPC  SendEvent, %s", err.Error())
				}
				if !result.Sent {
					fmt.Printf(result.Error)
				}
				goSend.Done()

			}
		}()

		go func() {
			for i := 0; i < numMsgs; i++ {

				err := stream.Send(&pb.Event{
					EventID:  clientName + strconv.Itoa(i),
					ClientID: clientName,
					Channel:  channel,
					Metadata: strconv.Itoa(i),
					Body:     []byte(body),
					Store:    false,
				})
				if err != nil {
					fmt.Printf("Error runPublisherGRPC  SendEvent, %s", err.Error())
				}
			}
		}()

	}

	goSend.Wait()

	benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now()))

	donewg.Done()

}

func runSubscriberGRPC(client pb.KubemqClient, channel string, clientName string, startwg, donewg *sync.WaitGroup, numMsgs int, msgSize int) {

	errCh := make(chan error)
	ch := make(chan time.Time, 2)
	counter := 0

	subClient, err := client.SubscribeToEvents(context.Background(), &pb.Subscribe{
		//SubscribeTypeData: &pb.Subscribe_Events, //TODO: not sure how to implement
		ClientID: clientName,
		Channel:  channel,
	})
	if err != nil {
		fmt.Printf("runPublisherGRPC SubscribeToEvents , %s", err.Error())
		return
	}
	go func(counter int, numMsgs int, ch chan time.Time) {
		for {
			_, err := subClient.Recv()
			if err != nil {
				errCh <- err
				fmt.Printf("Error lastMessages  %v", err)
				ch <- time.Now()
				return
			}
			counter++
			if handleCounterAndStop(counter, numMsgs, ch) {
				return
			}
		}
	}(counter, numMsgs, ch)
	time.Sleep(5 * time.Second)
	startwg.Done()
	start := <-ch
	end := <-ch

	benchmark.AddSubSample(bench.NewSample(numMsgs, msgSize, start, end))

	donewg.Done()

}

func handleCounterAndStop(counter int, numMsgs int, ch chan time.Time) bool {

	if counter == 1 {
		ch <- time.Now()
		return false
	}
	if counter >= numMsgs {
		ch <- time.Now()
		return true
	}
	//	fmt.Printf("perc %.f Messages %d\r", (float32(counter) / float32(numMsgs) * 100), counter)
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

func getClient(address string, clientName string, testpattern string) (*kubemq.Client, pb.KubemqClient, error) {
	if testpattern == "grpc" || testpattern == "grpcst" {

		grpcclient, err := grpcKubeMQEmu.RunClient(address)
		if err != nil {
			fmt.Printf("runPublisherGRPC RunClient , %s", err.Error())
			return nil, nil, err
		}

		return nil, grpcclient, nil

	}
	address, port, err := splitServerCred(address)
	if err != nil {
		log.Printf(err.Error())
		return nil, nil, err
	}
	client, err := kubemq.NewClient(context.Background(),
		kubemq.WithAddress(address, port),
		kubemq.WithClientId(clientName),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Printf("kubemq.NewClient %s", err.Error())
		return nil, nil, err
	}
	return client, nil, nil

}
