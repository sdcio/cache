FROM golang:1.19.5 as builder

RUN apt-get update && apt-get install -y ca-certificates git-core ssh
ADD keys/ /root/.ssh/
RUN chmod 700 /root/.ssh/id_rsa
RUN echo "Host github.com\n\tStrictHostKeyChecking no\n" >> /root/.ssh/config
RUN git config --global url.ssh://git@github.com/.insteadOf https://github.com/

ADD . /build
WORKDIR /build

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o cached .

FROM scratch
COPY --from=builder /build/cached /app/
WORKDIR /app
ENTRYPOINT [ "/app/cached" ]
