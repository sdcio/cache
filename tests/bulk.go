package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iptecharch/cache/proto/cachepb"
	schemapb "github.com/iptecharch/schema-server/protos/schema_server"
	"github.com/spf13/pflag"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var addr string
var conc int64
var numCache int64
var numPaths int64
var createFlag bool
var deleteFlag bool

func main() {
	pflag.StringVarP(&addr, "address", "a", "localhost:50100", "cache server address")
	pflag.Int64VarP(&numCache, "num-cache", "", 10, "number of caches to create")
	pflag.Int64VarP(&conc, "concurrency", "", 10, "max concurrent set requests")
	pflag.Int64VarP(&numPaths, "num-path", "", 100, "number of paths to write per cache")
	pflag.BoolVarP(&createFlag, "create", "", false, "create caches at startup")
	pflag.BoolVarP(&deleteFlag, "delete", "", false, "delete caches at the end")
	pflag.Parse()

	fmt.Println("caches          :", numCache)
	fmt.Println("concurrency     :", conc)
	fmt.Println("paths per cache :", numPaths)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cc, cclient, err := createCacheClient(addr)
	if err != nil {
		panic(err)
	}
	defer cc.Close()
	//
	// Create
	//
	if createFlag {
		runCreate(ctx, cclient)
	}
	//
	// WRITE
	//
	runWrite(ctx, cclient)
	//
	// READ
	//
	time.Sleep(50 * time.Millisecond)
	runRead(ctx, cclient)
	//
	// DELETE
	//
	if deleteFlag {
		runDelete(ctx, cclient)
	}
}

func createCacheClient(addr string) (*grpc.ClientConn, cachepb.CacheClient, error) {
	cc, err := grpc.Dial(addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return cc, cachepb.NewCacheClient(cc), nil
}

func runCreate(ctx context.Context, cclient cachepb.CacheClient) {
	wg := sync.WaitGroup{}
	wg.Add(int(numCache))
	sem := semaphore.NewWeighted(conc)
	durs := make(chan time.Duration, int(numCache))

	for i := int64(0); i < numCache; i++ {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			panic(err)
		}
		go func(i int64) {
			defer wg.Done()
			defer sem.Release(1)
			now := time.Now()
			_, err = cclient.Create(ctx, &cachepb.CreateRequest{
				Name: fmt.Sprintf("cache-instance-%d", i),
			})
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			durs <- time.Since(now)
		}(i)
	}
	wg.Wait()
	close(durs)
	rs := make([]time.Duration, 0)
	var tot time.Duration
	for d := range durs {
		tot += d
		rs = append(rs, d)
	}
	fmt.Println("cache creation:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	fmt.Println("	min:", rs[0])
	fmt.Println("	max:", rs[len(rs)-1])
	fmt.Println("	avg:", time.Duration(int(tot)/len(rs)))
}

func runWrite(ctx context.Context, cclient cachepb.CacheClient) {
	wg := sync.WaitGroup{}
	wg.Add(int(numCache))
	sem := semaphore.NewWeighted(conc)
	durs := make(chan time.Duration, int(numCache))

	for i := int64(0); i < numCache; i++ {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			panic(err)
		}
		go func(i int64) {
			defer wg.Done()
			defer sem.Release(1)
			now := time.Now()
			modStream, err := cclient.Modify(ctx)
			if err != nil {
				fmt.Println("fail stream", err)
				os.Exit(1)
			}
			go func() {
				// defer wg1.Done()
				for {
					_, err = modStream.Recv()
					if err != nil {
						if strings.Contains(err.Error(), "EOF") {
							return
						}
						fmt.Println("fail rcv", err)
						return
					}
				}
			}()
			for j := int64(0); j < numPaths; j++ {
				tv := &schemapb.TypedValue{
					Value: &schemapb.TypedValue_IntVal{
						IntVal: j,
					},
				}
				b, _ := proto.Marshal(tv)
				err = modStream.Send(&cachepb.ModifyRequest{
					Request: &cachepb.ModifyRequest_Write{
						Write: &cachepb.WriteValueRequest{
							Name: fmt.Sprintf("cache-instance-%d", i),
							Path: []string{
								"A",
								fmt.Sprintf("%d", i),
								fmt.Sprintf("%d", j),
								// fmt.Sprintf("%d", (i+1)*numCache),
								// fmt.Sprintf("%d", (j+1)*numPaths),
							},
							Value: &anypb.Any{
								Value: b,
							},
						},
					},
				})
				if err != nil {
					fmt.Println("fail send", err)
					os.Exit(1)
				}
			}
			modStream.CloseSend()
			durs <- time.Since(now)
		}(i)
	}
	wg.Wait()
	close(durs)
	rs := make([]time.Duration, 0)
	var tot time.Duration
	for d := range durs {
		tot += d
		rs = append(rs, d)
	}
	fmt.Println("values write:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	fmt.Println("	min:", rs[0])
	fmt.Println("	max:", rs[len(rs)-1])
	fmt.Println("	avg:", time.Duration(int(tot)/len(rs)))
}

func runRead(ctx context.Context, cclient cachepb.CacheClient) {
	wg := sync.WaitGroup{}
	wg.Add(int(numCache))
	sem := semaphore.NewWeighted(conc)
	durs := make(chan time.Duration, int(numCache))

	for i := int64(0); i < numCache; i++ {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			panic(err)
		}
		go func(i int64) {
			defer wg.Done()
			defer sem.Release(1)
			now := time.Now()

			// read all
			readStream, err := cclient.Read(ctx, &cachepb.ReadRequest{
				Name: fmt.Sprintf("cache-instance-%d", i),
				Path: []string{
					"A",
				},
			})
			if err != nil {
				fmt.Println("fail read:", err)
				os.Exit(1)
			}
			for {
				_, err := readStream.Recv()
				if err != nil {
					if strings.Contains(err.Error(), "EOF") {
						break
					}
					fmt.Println("fail rcv", err)
					break
				}
			}
			durs <- time.Since(now)
		}(i)
	}
	wg.Wait()
	close(durs)
	rs := make([]time.Duration, 0)
	var tot time.Duration
	for d := range durs {
		tot += d
		rs = append(rs, d)
	}
	fmt.Println("values read:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	fmt.Println("	min:", rs[0])
	fmt.Println("	max:", rs[len(rs)-1])
	fmt.Println("	avg:", time.Duration(int(tot)/len(rs)))
}

func runDelete(ctx context.Context, cclient cachepb.CacheClient) {
	wg2 := sync.WaitGroup{}
	wg2.Add(int(numCache))
	sem2 := semaphore.NewWeighted(conc)
	durs2 := make(chan time.Duration, int(numCache))
	for i := int64(0); i < numCache; i++ {
		err := sem2.Acquire(ctx, 1)
		if err != nil {
			panic(err)
		}
		go func(i int64) {
			defer wg2.Done()
			defer sem2.Release(1)
			now := time.Now()
			_, err = cclient.Delete(ctx, &cachepb.DeleteRequest{
				Name: fmt.Sprintf("cache-instance-%d", i),
			})
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			durs2 <- time.Since(now)
		}(i)
	}
	wg2.Wait()
	close(durs2)
	rs := make([]time.Duration, 0)
	var tot time.Duration
	for d := range durs2 {
		tot += d
		rs = append(rs, d)
	}
	fmt.Println("cache deletion:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	fmt.Println("	min:", rs[0])
	fmt.Println("	max:", rs[len(rs)-1])
	fmt.Println("	avg:", time.Duration(int(tot)/len(rs)))
}
