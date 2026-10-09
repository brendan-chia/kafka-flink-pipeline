# The upstream public images are no longer available. Compile the official
# pinned release using Go's module checksum verification instead.
FROM golang:1.24.8-alpine AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOMAXPROCS=2
RUN go install -p 2 -trimpath -ldflags="-s -w -X github.com/minio/minio/cmd.Version=2025-10-15T17:29:55Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-10-15T17-29-55Z" github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z

FROM alpine:3.22.2
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
