FROM golang:1.24-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /k8s-emulator .

FROM alpine:3.20
COPY --from=build /k8s-emulator /k8s-emulator
EXPOSE 6443
ENTRYPOINT ["/k8s-emulator"]
CMD ["--scenario", "large", "--headless"]
