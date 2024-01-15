FROM scratch

LABEL repo="https://github.com/iptecharch/cache"

COPY cache /app/
COPY cachectl /app/
WORKDIR /app

ENTRYPOINT [ "/app/cache" ]
