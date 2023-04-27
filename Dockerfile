FROM golang:1.19.5 as builder

RUN apt-get update && apt-get install -y ca-certificates git-core ssh
ADD keys/ /root/.ssh/
RUN chmod 700 /root/.ssh/id_rsa
RUN echo "Host github.com\n\tStrictHostKeyChecking no\n" >> /root/.ssh/config
RUN git config --global url.ssh://git@github.com/.insteadOf https://github.com/

ADD . /build
WORKDIR /build

RUN CGO_ENABLED=0 go build -o /build/cache -ldflags="-s -w" .

FROM scratch
COPY --from=builder /build/cache /app/
WORKDIR /app
ENTRYPOINT [ "/app/cache" ]
