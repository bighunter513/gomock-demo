package grpc_mock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/mock"
	plannerMock "go.nhat.io/grpcmock/mock/planner"
	"go.nhat.io/grpcmock/planner"
	"go.nhat.io/grpcmock/service"
	"go.nhat.io/grpcmock/stream"
	"go.nhat.io/grpcmock/test/grpctest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.nhat.io/grpcmock"
	xassert "go.nhat.io/grpcmock/assert"
)

func TestGetItems(t *testing.T) {
	t.Parallel()

	expected := &grpctest.Item{Id: 42, Name: "Item 42"}

	_, d := grpcmock.MockServerWithBufConn(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		func(s *grpcmock.Server) {
			s.ExpectUnary("/grpctest.ItemService/GetItem").
				WithHeader("locale", "en-US").
				WithPayload(&grpctest.GetItemRequest{Id: 42}).
				Return(expected)
		},
	)(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()

	out := &grpctest.Item{}

	err := grpcmock.InvokeUnary(ctx,
		"/grpctest.ItemService/GetItem",
		&grpctest.GetItemRequest{Id: 42}, out,
		grpcmock.WithHeader("locale", "en-US"),
		grpcmock.WithContextDialer(d),
		grpcmock.WithInsecure(),
	)

	xassert.EqualMessage(t, expected, out)
	assert.NoError(t, err)
}

func TestExampleNewServer_unaryMethod(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	expected := &grpctest.Item{
		Id:     41,
		Locale: "en-US",
		Name:   "Item #41",
	}

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 41}).
				Return(expected)
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.Item{}
	err := grpcmock.InvokeUnary(context.Background(),
		"grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: 41}, out,
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)
	xassert.EqualMessage(t, expected, out)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "id": 41,
	//     "locale": "en-US",
	//     "name": "Item #41"
	// }
}

func TestExampleServer_WithPlanner(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),

		func(s *grpcmock.Server) {
			p := &plannerMock.Planner{}

			p.On("IsEmpty").Return(false)
			p.On("Expect", mock.Anything)
			p.On("Plan", mock.Anything, mock.Anything, mock.Anything).
				Return(nil, errors.New("always fail"))

			s.WithPlanner(p)

			s.ExpectUnary("grpctest.ItemService/GetItem").
				Run(func(context.Context, any) (any, error) {
					panic(`this never happens`)
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	err := grpcmock.InvokeUnary(context.Background(),
		"grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: 41}, &grpctest.Item{},
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)

	assert.NotNil(t, err, "rpc error: code = Internal desc = always fail")

	fmt.Printf("%v\n", err)
	// Output:
	// rpc error: code = Internal desc = always fail
}

func TestExampleServer_firstMatch_planner(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	items := []*grpctest.Item{
		{Id: 1, Name: "FoodUniversity"},
		{Id: 2, Name: "Metaverse"},
		{Id: 3, Name: "Crypto"},
	}

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithPlanner(planner.FirstMatch()),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 1}).
				Return(items[0])

			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 2}).
				Return(items[1])

			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 3}).
				Return(items[2])
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	ids := []int32{1, 2, 3}
	result := make([]*grpctest.Item, len(ids))

	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})

	var wg sync.WaitGroup

	for _, id := range ids {
		wg.Add(1)

		go func(id int32) {
			defer wg.Done()

			out := &grpctest.Item{}

			err := grpcmock.InvokeUnary(context.Background(),
				"grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: id}, out,
				grpcmock.WithInsecure(),
				grpcmock.WithBufConnDialer(buf),
			)
			assert.NoError(t, err)

			result[id-1] = out
		}(id)
	}

	wg.Wait()

	for i := 0; i < len(items); i++ {
		xassert.EqualMessage(t, items[i], result[i])
	}

	_, err := json.MarshalIndent(result, "", "    ")
	assert.NoError(t, err)

	// fmt.Println(string(output))
	// Output:
	// [
	//     {
	//         "id": 1,
	//         "name": "FoodUniversity"
	//     },
	//     {
	//         "id": 2,
	//         "name": "Metaverse"
	//     },
	//     {
	//         "id": 3,
	//         "name": "Crypto"
	//     }
	// ]
}

func TestExampleNewServer_withPort(t *testing.T) {
	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithPort(8080),
		func(s *grpcmock.Server) {
			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 41}).
				Return(&grpctest.Item{
					Id:     41,
					Locale: "en-US",
					Name:   "Item #41",
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.Item{}
	err := grpcmock.InvokeUnary(context.Background(),
		":8080/grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: 41}, out,
		grpcmock.WithInsecure(),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "id": 41,
	//     "locale": "en-US",
	//     "name": "Item #41"
	// }
}

func TestExampleNewServer_unaryMethod_customHandler(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 42}).
				Run(func(ctx context.Context, in any) (any, error) {
					req := in.(*grpctest.GetItemRequest) // nolint: errcheck

					var locale string

					md, _ := metadata.FromIncomingContext(ctx)
					if md != nil {
						if values := md.Get("locale"); len(values) > 0 {
							locale = values[0]
						}
					}

					return &grpctest.Item{
						Id:     req.GetId(),
						Locale: locale,
						Name:   fmt.Sprintf("Item #%d", req.GetId()),
					}, nil
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.Item{}
	err := grpcmock.InvokeUnary(context.Background(),
		"grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: 42}, out,
		grpcmock.WithHeader("locale", "en-US"),
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "id": 42,
	//     "locale": "en-US",
	//     "name": "Item #42"
	// }
}

func TestExampleNewServer_clientStreamMethod(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectClientStream("grpctest.ItemService/CreateItems").
				WithPayload([]*grpctest.Item{
					{Id: 41, Name: "Item #41"},
					{Id: 42, Name: "Item #42"},
				}).
				Return(&grpctest.CreateItemsResponse{NumItems: 2})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.CreateItemsResponse{}
	err := grpcmock.InvokeClientStream(context.Background(),
		"grpctest.ItemService/CreateItems",
		grpcmock.SendAll([]*grpctest.Item{
			{Id: 41, Name: "Item #41"},
			{Id: 42, Name: "Item #42"},
		}),
		out,
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "num_items": 2
	// }
}

func TestExampleNewServer_clientStreamMethod_customHandler(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectClientStream("grpctest.ItemService/CreateItems").
				WithPayload(grpcmock.MatchClientStreamMsgCount(3)).
				Run(func(_ context.Context, s grpc.ServerStream) (any, error) {
					out := make([]*grpctest.Item, 0)

					if err := stream.RecvAll(s, &out); err != nil {
						return nil, err
					}

					cnt := int64(0)

					for _, msg := range out {
						if msg.GetId() > 40 {
							cnt++
						}
					}

					return &grpctest.CreateItemsResponse{NumItems: cnt}, nil
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.CreateItemsResponse{}
	err := grpcmock.InvokeClientStream(context.Background(),
		"grpctest.ItemService/CreateItems",
		grpcmock.SendAll([]*grpctest.Item{
			{Id: 40, Name: "Item #40"},
			{Id: 41, Name: "Item #41"},
			{Id: 42, Name: "Item #42"},
		}),
		out,
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "num_items": 2
	// }
}

func TestExampleNewServer_serverStreamMethod(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectServerStream("grpctest.ItemService/ListItems").
				Return([]*grpctest.Item{
					{Id: 41, Name: "Item #41"},
					{Id: 42, Name: "Item #42"},
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := make([]*grpctest.Item, 0)
	err := grpcmock.InvokeServerStream(context.Background(),
		"grpctest.ItemService/ListItems",
		&grpctest.ListItemsRequest{},
		grpcmock.RecvAll(&out),
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// [
	//     {
	//         "id": 41,
	//         "name": "Item #41"
	//     },
	//     {
	//         "id": 42,
	//         "name": "Item #42"
	//     }
	// ]
}

func TestExampleNewServer_serverStreamMethod_customHandler(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectServerStream("grpctest.ItemService/ListItems").
				Run(func(_ context.Context, _ any, s grpc.ServerStream) error {
					_ = s.SendMsg(&grpctest.Item{Id: 41, Name: "Item #41"}) // nolint: errcheck
					_ = s.SendMsg(&grpctest.Item{Id: 42, Name: "Item #42"}) // nolint: errcheck

					return nil
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := make([]*grpctest.Item, 0)
	err := grpcmock.InvokeServerStream(context.Background(),
		"grpctest.ItemService/ListItems",
		&grpctest.ListItemsRequest{},
		grpcmock.RecvAll(&out),
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// [
	//     {
	//         "id": 41,
	//         "name": "Item #41"
	//     },
	//     {
	//         "id": 42,
	//         "name": "Item #42"
	//     }
	// ]
}

func TestExampleNewServer_serverStreamMethod_customStreamBehaviors(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectServerStream("grpctest.ItemService/ListItems").
				ReturnStream().
				Send(&grpctest.Item{Id: 41, Name: "Item #41"}).
				ReturnError(codes.Aborted, "server aborted the transaction")
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := make([]*grpctest.Item, 0)
	err := grpcmock.InvokeServerStream(context.Background(),
		"grpctest.ItemService/ListItems",
		&grpctest.ListItemsRequest{},
		grpcmock.RecvAll(&out),
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)

	fmt.Printf("received items: %d\n", len(out))
	fmt.Printf("error: %s", err)

	// Output:
	// received items: 0
	// error: rpc error: code = Aborted desc = server aborted the transaction
}

func TestExampleNewServer_bidirectionalStreamMethod(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectBidirectionalStream("grpctest.ItemService/TransformItems").
				Run(func(ctx context.Context, s grpc.ServerStream) error {
					for {
						item := &grpctest.Item{}
						err := s.RecvMsg(item)

						if errors.Is(err, io.EOF) {
							return nil
						}

						if err != nil {
							return err
						}

						item.Name = fmt.Sprintf("Modified #%d", item.GetId())

						if err := s.SendMsg(item); err != nil {
							return err
						}
					}
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	in := []*grpctest.Item{
		{Id: 40, Name: "Item #40"},
		{Id: 41, Name: "Item #41"},
		{Id: 42, Name: "Item #42"},
	}

	out := make([]*grpctest.Item, 0)

	err := grpcmock.InvokeBidirectionalStream(context.Background(),
		"grpctest.ItemService/TransformItems",
		grpcmock.SendAndRecvAll(in, &out),
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// [
	//     {
	//         "id": 40,
	//         "name": "Modified #40"
	//     },
	//     {
	//         "id": 41,
	//         "name": "Modified #41"
	//     },
	//     {
	//         "id": 42,
	//         "name": "Modified #42"
	//     }
	// ]
}

func TestExampleRegisterService(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterService(grpctest.RegisterItemServiceServer),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 41}).
				Return(&grpctest.Item{
					Id:     41,
					Locale: "en-US",
					Name:   "Item #41",
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.Item{}
	err := grpcmock.InvokeUnary(context.Background(),
		"grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: 41}, out,
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "id": 41,
	//     "locale": "en-US",
	//     "name": "Item #41"
	// }
}

func TestExampleRegisterServiceFromInstance(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterServiceFromInstance("grpctest.ItemService", (*grpctest.ItemServiceServer)(nil)),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 41}).
				Return(&grpctest.Item{
					Id:     41,
					Locale: "en-US",
					Name:   "Item #41",
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.Item{}
	err := grpcmock.InvokeUnary(context.Background(),
		"grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: 41}, out,
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "id": 41,
	//     "locale": "en-US",
	//     "name": "Item #41"
	// }
}

func TestExampleRegisterServiceFromMethods(t *testing.T) {
	buf := bufconn.Listen(1024 * 1024)
	defer buf.Close() // nolint: errcheck

	srv := grpcmock.NewServer(
		grpcmock.RegisterServiceFromMethods(service.Method{
			ServiceName: "grpctest.ItemService",
			MethodName:  "GetItem",
			MethodType:  service.TypeUnary,
			Input:       &grpctest.GetItemRequest{},
			Output:      &grpctest.Item{},
		}),
		grpcmock.WithListener(buf),
		func(s *grpcmock.Server) {
			s.ExpectUnary("grpctest.ItemService/GetItem").
				WithPayload(&grpctest.GetItemRequest{Id: 41}).
				Return(&grpctest.Item{
					Id:     41,
					Locale: "en-US",
					Name:   "Item #41",
				})
		},
	)

	defer srv.Close() // nolint: errcheck

	// Call the service.
	out := &grpctest.Item{}
	err := grpcmock.InvokeUnary(context.Background(),
		"grpctest.ItemService/GetItem", &grpctest.GetItemRequest{Id: 41}, out,
		grpcmock.WithInsecure(),
		grpcmock.WithBufConnDialer(buf),
	)
	assert.NoError(t, err)

	output, err := json.MarshalIndent(out, "", "    ")
	assert.NoError(t, err)

	fmt.Println(string(output))

	// Output:
	// {
	//     "id": 41,
	//     "locale": "en-US",
	//     "name": "Item #41"
	// }
}
