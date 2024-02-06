# Copyright 2024 Nokia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
