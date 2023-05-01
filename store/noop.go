package store

import (
	"context"

	"google.golang.org/protobuf/proto"
)

type noopStore[T proto.Message] struct{}

func newNoopStore[T proto.Message]() Store[T] {
	return &noopStore[T]{}
}

func (s *noopStore[T]) CreateCache(ctx context.Context, name string, cfg map[string]any, bucket ...string) error {
	return nil
}
func (s *noopStore[T]) ListCaches(ctx context.Context) ([]string, error)    { return nil, nil }
func (s *noopStore[T]) DeleteCache(ctx context.Context, name string) error  { return nil }
func (s *noopStore[T]) Clone(ctx context.Context, name, cname string) error { return nil }
func (s *noopStore[T]) GetCacheConfig(ctx context.Context, name string) (map[string]any, error) {
	return nil, nil
}
func (s *noopStore[T]) LoadCache(ctx context.Context, name string) error { return nil }
func (s *noopStore[T]) WriteValue(ctx context.Context, name, bucket string, k []byte, v T) error {
	return nil
}
func (s *noopStore[T]) WriteBytesValue(ctx context.Context, name, bucket string, k []byte, v []byte) error {
	return nil
}
func (s *noopStore[T]) GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error) {
	return nil, nil
}
func (s *noopStore[T]) DeleteValue(ctx context.Context, name, bucket string, k []byte) error {
	return nil
}
func (s *noopStore[T]) GetAll(ctx context.Context, name, bucket string) (chan *KV, error) {
	return nil, nil
}
func (s *noopStore[T]) GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte) (chan *KV, error) {
	return nil, nil
}
func (s *noopStore[T]) Close() error { return nil }
