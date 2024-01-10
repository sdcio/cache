FROM golang:1.21.4 as builder

RUN apt-get update && apt-get install -y ca-certificates git-core ssh
RUN mkdir -p -m 0700 /root/.ssh && ssh-keyscan github.com >> /root/.ssh/known_hosts
RUN git config --global url.ssh://git@github.com/.insteadOf https://github.com/

ADD . /build
WORKDIR /build

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=ssh \
    CGO_ENABLED=0 go build -o /build/cache -ldflags="-s -w" .

FROM scratch
COPY --from=builder /build/cache /app/
WORKDIR /app
ENTRYPOINT [ "/app/cache" ]
