
BRANCH :=$(shell git rev-parse --abbrev-ref HEAD)
COMMIT := $(shell git rev-parse --short HEAD)
REMOTE_REGISTRY :=registry.kmrd.dev/iptecharch/cache
TAG := 0.0.0-$(BRANCH)-$(COMMIT)
IMAGE := $(REMOTE_REGISTRY):$(TAG)

generate:
	cd proto;./generate.sh

build:
	mkdir -p bin
	go1.19.5 build -ldflags="-s -w" -o bin/cachectl cachectl/main.go
	go1.19.5 build -ldflags="-s -w" -o bin/cached main.go
	go1.19.5 build -o bin/bulk tests/bulk.go

docker-build:
	docker build . -t $(IMAGE)

docker-push: docker-build
	docker tag $(IMAGE) $(REMOTE_REGISTRY):latest
	docker push $(IMAGE)

