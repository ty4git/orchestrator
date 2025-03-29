FROM golang:1.22-alpine AS builder

RUN mkdir /build
WORKDIR /build

COPY . .
# RUN go mod download

# COPY . .
RUN go build .

EXPOSE 8080

CMD ["/build/main"]

# FROM alpine
    
# RUN mkdir /app
# WORKDIR /app

# COPY --from=builder /build/main .
# COPY --from=builder /build/.env .

# EXPOSE 8080

# CMD /app/main