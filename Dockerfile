FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -o /processor ./cmd/processor && mkdir -p /runtime/workspace/.scratch && chown -R 65532:65532 /runtime

FROM scratch
COPY --from=build /processor /processor
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /runtime/ /
COPY testdata/ /testdata/
USER 65532:65532
WORKDIR /workspace
ENTRYPOINT ["/processor"]
CMD ["doctor"]
