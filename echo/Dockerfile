FROM golang:1.22.11-alpine as builder
RUN mkdir /build
ADD *.go go.* /build/
WORKDIR /build
RUN go build .

FROM alpine
RUN mkdir /app
WORKDIR /app
COPY --from=builder /build/echo .

CMD /app/echo