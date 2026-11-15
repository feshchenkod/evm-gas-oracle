FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS build
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
