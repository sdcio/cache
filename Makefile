REMOTE_REGISTRY :=registry.kmrd.dev/sdcio/cache
TAG := $(shell git describe --tags)
IMAGE := $(REMOTE_REGISTRY):$(TAG)
USERID := 10000

generate:
	cd proto;./generate.sh

build:
	mkdir -p bin
	go build -ldflags="-s -w" -o bin/cachectl cachectl/main.go
	go build -ldflags="-s -w" -o bin/cached main.go

docker-build:
	ssh-add ./keys/id_rsa 2>/dev/null; true
	docker build --build-arg USERID=$(USERID) . -t $(IMAGE) --ssh default=$(SSH_AUTH_SOCK)

docker-push: docker-build
	docker push $(IMAGE)

release: docker-build
	docker tag $(IMAGE) $(REMOTE_REGISTRY):latest
	docker push $(REMOTE_REGISTRY):latest

