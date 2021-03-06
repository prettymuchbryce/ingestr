FROM golang:alpine as builder

RUN apk add --no-cache gcc musl-dev git make

RUN mkdir /build 
ADD . /build/
WORKDIR /build 
RUN go build -o main .
FROM alpine
RUN adduser -S -D -H -h /app appuser
USER appuser
COPY --from=builder /build/main /app/
COPY --from=builder /build/.env /app/
WORKDIR /app

CMD ["./main"]
