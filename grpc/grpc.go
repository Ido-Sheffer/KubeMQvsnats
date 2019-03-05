package grpc

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	pb "github.com/Ido-Sheffer/KubeMQvsnats/grpc/pkg/pb"

	"google.golang.org/grpc"
)

type server struct {
	msgCh chan *pb.EventReceive
}

func (s *server) SubscribeToEvents(subReq *pb.Subscribe, stream pb.Kubemq_SubscribeToEventsServer) (err error) {
	errCh := make(chan error, 10)
	s.msgCh = make(chan *pb.EventReceive, 100)
	ctx, _ := context.WithCancel(stream.Context())

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.msgCh:
			if err = stream.Send(msg); err != nil {
				fmt.Printf("SubscribeToEvents %s", err.Error())
				return
			}
			//s.stats.receivedEvents.Inc()

		case <-errCh:

			return nil
		}
	}
}

func (s *server) SendEvent(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	//fmt.Print("SubscribeToEvents")
	result := &pb.Result{
		EventID: msg.EventID,
		Sent:    true,
		Error:   "",
	}
	s.msgCh <- &pb.EventReceive{
		EventID:   msg.EventID,
		Channel:   msg.Channel,
		Metadata:  msg.Metadata,
		Body:      msg.Body,
		Timestamp: 123,
		Sequence:  1,
	}
	return result, nil

}

func (s *server) SendEventsStream(stream pb.Kubemq_SendEventsStreamServer) error {

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("error on receive event from stream %s", err.Error())
			return err
		}
		result := &pb.Result{
			EventID: msg.EventID,
			Sent:    true,
			Error:   "",
		}
		s.msgCh <- &pb.EventReceive{
			EventID:   msg.EventID,
			Channel:   msg.Channel,
			Metadata:  msg.Metadata,
			Body:      msg.Body,
			Timestamp: 123,
			Sequence:  1,
		}
		err = stream.Send(result)
		if err != nil {
			fmt.Printf("error on send result %s", err.Error())
			//	return err
		}
	}
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

func RunServer(port string) error {

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Printf("error %v", err)
		return err
	}

	s := grpc.NewServer()

	pb.RegisterKubemqServer(s, &server{})

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("error %s", err.Error())
		}

	}()

	return nil
}

func RunClient(address string) (pb.KubemqClient, error) {

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %s", err.Error())
		return nil, err
	}

	return pb.NewKubemqClient(conn), nil

}
