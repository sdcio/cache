/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var conc int64
var numCache int64
var numPaths int64
var createFlag bool
var deleteFlag bool
var doNotWrite bool
var periodic bool

// benchCmd represents the benchmark command
var benchCmd = &cobra.Command{
	Use:     "benchmark",
	Short:   "run a benchmark test",
	Aliases: []string{"bench"},

	RunE: func(cmd *cobra.Command, _ []string) error {
		cc, cclient, err := createCacheClient(address)
		if err != nil {
			panic(err)
		}
		defer cc.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		fmt.Println("caches          :", numCache)
		fmt.Println("concurrency     :", conc)
		fmt.Println("paths per cache :", numPaths)

		runAll(ctx, cclient, true)

		if periodic {
			ticker := time.NewTicker(15 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
				case <-ticker.C:
					runAll(ctx, cclient, false)
				}
			}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(benchCmd)
	benchCmd.Flags().Int64VarP(&numCache, "num-cache", "", 10, "number of caches to create")
	benchCmd.Flags().Int64VarP(&conc, "concurrency", "", 10, "max concurrent set requests")
	benchCmd.Flags().Int64VarP(&numPaths, "num-path", "", 100, "number of paths to write per cache")
	benchCmd.Flags().BoolVarP(&createFlag, "create", "", false, "create caches at startup")
	benchCmd.Flags().BoolVarP(&doNotWrite, "no-write", "", false, "do not write, only read")
	benchCmd.Flags().BoolVarP(&deleteFlag, "delete", "", false, "delete caches at the end")
	benchCmd.Flags().BoolVarP(&periodic, "periodic", "", false, "run periodically")
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
				Name:      fmt.Sprintf("cache-instance-%d", i),
				Ephemeral: false,
				Cached:    false,
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
	log.Println("cache creation:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	log.Println("	min:", rs[0])
	log.Println("	max:", rs[len(rs)-1])
	log.Println("	avg:", time.Duration(int(tot)/len(rs)))
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
			for j := int64(0); j < numPaths; j++ {
				tv := &schemapb.TypedValue{
					Value: &schemapb.TypedValue_StringVal{
						StringVal: fmt.Sprintf("ABCD%d", j),
					},
				}
				b, _ := proto.Marshal(tv)
				err = modStream.Send(&cachepb.ModifyRequest{
					Request: &cachepb.ModifyRequest_Write{
						Write: &cachepb.WriteValueRequest{
							Name: fmt.Sprintf("cache-instance-%d", i),
							Path: []string{
								"A",
								fmt.Sprintf("abcd%d", i),
								fmt.Sprintf("abcd%d", (i+1)*j),
								fmt.Sprintf("abcd%d", (i+2)*j),
								fmt.Sprintf("abcd%d", (i+3)*j),
							},
							Value: &anypb.Any{
								Value: b,
							},
						},
					},
				})
				if err != nil {
					log.Errorf("fail send: %v", err)
					os.Exit(1)
				}
			}
			_, err = modStream.CloseAndRecv()
			if err != nil {
				if !strings.Contains(err.Error(), "EOF") {
					log.Errorf("fail closeAndRecv: %v", err)
					os.Exit(1)
				}
			}
			durs <- time.Since(now)
			time.Sleep(1 * time.Second)
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
	log.Println("values write:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	log.Println("	min:", rs[0])
	log.Println("	max:", rs[len(rs)-1])
	log.Println("	avg:", time.Duration(int(tot)/len(rs)))
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
				log.Error("fail read:", err)
				os.Exit(1)
			}
			for {
				_, err := readStream.Recv()
				if err != nil {
					if strings.Contains(err.Error(), "EOF") {
						break
					}
					log.Error("fail rcv", err)
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
	log.Println("values read:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	log.Println("	min:", rs[0])
	log.Println("	max:", rs[len(rs)-1])
	log.Println("	avg:", time.Duration(int(tot)/len(rs)))
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
	log.Println("cache deletion:")
	sort.Slice(rs, func(i, j int) bool {
		return rs[i] < rs[j]
	})
	log.Println("	min:", rs[0])
	log.Println("	max:", rs[len(rs)-1])
	log.Println("	avg:", time.Duration(int(tot)/len(rs)))
}

func runAll(ctx context.Context, cclient cachepb.CacheClient, first bool) {
	//
	// Create
	//
	if createFlag && first {
		runCreate(ctx, cclient)
		time.Sleep(time.Second)
	}
	//
	// WRITE
	//
	if !doNotWrite {
		log.Println("writing...")
		runWrite(ctx, cclient)
		log.Println("waiting 10s before reading")
		time.Sleep(10 * time.Second)
	}
	//
	// READ
	//
	log.Println("reading...")
	runRead(ctx, cclient)
	//
	// DELETE
	//
	if deleteFlag {
		runDelete(ctx, cclient)
	}
}
