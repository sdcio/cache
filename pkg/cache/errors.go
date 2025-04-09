package cache

import "fmt"

var (
	ErrorCacheInstanceNotFound      = fmt.Errorf("cache instance not found")
	ErrorCacheInstanceAlreadyExists = fmt.Errorf("cache instance already exists")
)
