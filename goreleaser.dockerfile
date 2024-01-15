FROM scratch

COPY cache /app/
COPY cachectl /app/
WORKDIR /app

ENTRYPOINT [ "/app/cache" ]
