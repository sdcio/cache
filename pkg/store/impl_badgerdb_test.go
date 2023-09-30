package store

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
)

const (
	dbPath = "./db"
)

func Test_badgerDBStore_CreateCache(t *testing.T) {
	os.RemoveAll(dbPath)
	defer func() {
		os.RemoveAll(dbPath)
	}()
	type fields struct {
		path string
		m    *sync.RWMutex
		dbs  map[string]*bdb
	}
	type args struct {
		ctx    context.Context
		name   string
		meta   map[string]any
		bucket []string
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "simple_cache",
			fields: fields{
				path: dbPath,
				m:    &sync.RWMutex{},
				dbs:  map[string]*bdb{},
			},
			args: args{
				ctx:    nil,
				name:   "cache1",
				bucket: []string{},
			},
			wantErr: false,
		},
		{
			name: "simple_with_buckets",
			fields: fields{
				path: dbPath,
				m:    &sync.RWMutex{},
				dbs:  map[string]*bdb{},
			},
			args: args{
				ctx:    nil,
				name:   "cache2",
				bucket: []string{"bucket1", "bucket2"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &badgerDBStore{
				path: tt.fields.path,
				m:    tt.fields.m,
				dbs:  tt.fields.dbs,
			}
			if err := s.CreateCache(tt.args.ctx, tt.args.name, tt.args.meta, tt.args.bucket...); (err != nil) != tt.wantErr {
				t.Errorf("badgerDBStore.CreateCache() error = %v, wantErr %v", err, tt.wantErr)
			}

			caches, err := s.ListCaches(context.Background())
			if err != nil {
				t.Errorf("badgerDBStore.ListCaches() error = %v, wantErr %v", err, tt.wantErr)
			}
			t.Log(caches)
			if len(caches) == 0 {
				t.Errorf("failed to list caches: got %v: want: %v", caches, tt.args.name)
			}
			if caches[0] != tt.args.name {
				t.Errorf("failed to list caches: got %v: want: %v", caches[0], tt.args.name)
			}
			s.Close()
		})
	}

}

func Test_badgerDBStore_WriteValue(t *testing.T) {
	os.RemoveAll(dbPath)
	defer func() {
		os.RemoveAll(dbPath)
	}()
	cacheName := "cache1"
	type args struct {
		ctx    context.Context
		name   string
		bucket string
		k      []byte
		v      []byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "wr_config",
			args: args{
				ctx:    context.TODO(),
				name:   cacheName,
				bucket: "config",
				k:      []byte{1, 2, 3},
				v:      []byte{42},
			},
			wantErr: false,
		},
		{
			name: "wr_state",
			args: args{
				ctx:    context.TODO(),
				name:   cacheName,
				bucket: "state",
				k:      []byte{1, 2, 3},
				v:      []byte{42},
			},
			wantErr: false,
		},
		{
			name: "wr_intended",
			args: args{
				ctx:    context.TODO(),
				name:   cacheName,
				bucket: "intended",
				k: []byte{
					0, 0, 0, 1,
					42, 0x2c, 42, 0x2c, 42, 0x2c, 42, 0x2c,
					1, 2, 3, 4, 5, 6, 7, 8,
				},
				v: []byte{42},
			},
			wantErr: false,
		},
	}
	s := &badgerDBStore{
		path: dbPath,
		m:    &sync.RWMutex{},
		dbs:  map[string]*bdb{},
	}

	err := s.CreateCache(context.TODO(), cacheName, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.WriteValue(tt.args.ctx, tt.args.name, tt.args.bucket, tt.args.k, tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("badgerDBStore.WriteValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			rv, err := s.GetValue(tt.args.ctx, tt.args.name, tt.args.bucket, tt.args.k)
			if err != nil {
				t.Errorf("badgerDBStore.GetValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !bytes.Equal(tt.args.v, rv) {
				t.Errorf("badgerDBStore.GetValue() got = %x, want %x", tt.args.v, rv)
			}
		})
	}
}
