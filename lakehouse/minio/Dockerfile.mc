FROM golang:1.24.8-alpine AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOMAXPROCS=2
RUN go install -p 2 -trimpath -ldflags="-s -w -X github.com/minio/mc/cmd.Version=2025-08-13T08:35:41Z -X github.com/minio/mc/cmd.ReleaseTag=RELEASE.2025-08-13T08-35-41Z" github.com/minio/mc@RELEASE.2025-08-13T08-35-41Z

FROM alpine:3.22.2
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/mc /usr/local/bin/mc
ENTRYPOINT ["mc"]
