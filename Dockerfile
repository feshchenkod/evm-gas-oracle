FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS build
WORKDIR /src
COPY go.mod ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -buildvcs=false -ldflags "-s -w -X main.version=$VERSION" -o /out/evm-gas-oracle ./cmd/evm-gas-oracle

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/evm-gas-oracle /evm-gas-oracle
USER 65532:65532
EXPOSE 80
HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=3 CMD ["/evm-gas-oracle", "-healthcheck"]
ENTRYPOINT ["/evm-gas-oracle"]
