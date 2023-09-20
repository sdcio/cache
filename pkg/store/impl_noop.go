package store

import (
	"context"
)

type noopStore struct{}

func newNoopStore() Store {
	return &noopStore{}
}

func (s *noopStore) CreateCache(ctx context.Context, name string, cfg map[string]any, bucket ...string) error {
	return nil
}
func (s *noopStore) ListCaches(ctx context.Context) ([]string, error)    { return nil, nil }
func (s *noopStore) DeleteCache(ctx context.Context, name string) error  { return nil }
func (s *noopStore) Clone(ctx context.Context, name, cname string) error { return nil }
func (s *noopStore) GetMeta(ctx context.Context, name string) (map[string]any, error) {
	return nil, nil
}
func (s *noopStore) LoadCache(ctx context.Context, name string) error { return nil }

func (s *noopStore) WriteValue(ctx context.Context, name, bucket string, k []byte, v []byte) error {
	return nil
}
func (s *noopStore) GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error) {
	return nil, nil
}
func (s *noopStore) DeleteValue(ctx context.Context, name, bucket string, k []byte) error {
	return nil
}
func (s *noopStore) DeletePrefix(ctx context.Context, name, bucket string, k []byte, fn ...SelectFn) error {
	return nil
}
func (s *noopStore) GetAll(ctx context.Context, name, bucket string, _ ...SelectFn) (chan *KV, error) {
	return nil, nil
}
func (s *noopStore) GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte, fn ...SelectFn) (chan *KV, error) {
	return nil, nil
}
func (s *noopStore) Close() error                                                { return nil }
func (s *noopStore) Stats(ctx context.Context, name string) (*StoreStats, error) { return nil, nil }
