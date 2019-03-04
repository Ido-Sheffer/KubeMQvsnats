package grpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/Ido-Sheffer/KubeMQvsnats/grpc/pkg/pb"

	"google.golang.org/grpc"
)

type server struct {
	errCh  chan error
	ch     chan time.Time
	numMsg int
}

func (s *server) SubscribeToEvents(subReq *pb.Subscribe, stream pb.Kubemq_SubscribeToEventsServer) (err error) {

	errCh := make(chan error, 10)
	msgCh := make(chan *pb.EventReceive, 1024)
	ctx, _ := context.WithCancel(stream.Context())
	counter := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-msgCh:
			//s.stats.receivedEvents.Inc()
			counter++
			handleEcentCh(counter, s.numMsg, s.ch)

		case <-errCh:
			s.errCh <- err
			return nil
		}
	}
}

func handleEcentCh(counter int, numMsgs int, ch chan time.Time) {

	if counter == 1 || counter >= numMsgs {
		ch <- time.Now()
	}

	fmt.Printf("perc %.f Messages %d\r", (float32(counter) / float32(numMsgs) * 100), counter)

}

func (s *server) SendEvent(ctx context.Context, msg *pb.Event) (*pb.Result, error) {

	result := &pb.Result{
		EventID: msg.EventID,
		Sent:    true,
		Error:   "",
	}
	return result, nil
}

func (s *server) SendEventsStream(stream pb.Kubemq_SendEventsStreamServer) error {
	return nil
}

//TODO - add tracing

func (s *server) SendRequest(ctx context.Context, req *pb.Request) (*pb.Response, error) {

	return nil, fmt.Errorf("error")

}

func (s *server) SendResponse(ctx context.Context, res *pb.Response) (empty *pb.Empty, err error) {

	return &pb.Empty{}, fmt.Errorf("error")

}

func (s *server) SubscribeToRequests(subReq *pb.Subscribe, stream pb.Kubemq_SubscribeToRequestsServer) error {
	return fmt.Errorf("error")
}

func (s *server) setValues(ch chan time.Time, errCh chan error, numnsg int) {
	s.numMsg = numnsg
	s.errCh = errCh
	s.ch = ch
}

func RunServer(port string) {

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("error %v", err)
		return
	}

	s := grpc.NewServer()

	pb.RegisterKubemqServer(s, &server{})

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("error %v", err)
		}

	}()

}

func RunClient(address string) pb.KubemqClient {

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	return pb.NewKubemqClient(conn)

}
