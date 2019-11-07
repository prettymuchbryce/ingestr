FROM golang:alpine as builder

RUN apk add --no-cache gcc musl-dev

RUN mkdir /build 
ADD . /build/
WORKDIR /build 
RUN go build -o main .
FROM alpine
RUN adduser -S -D -H -h /app appuser
USER appuser
COPY --from=builder /build/main /app/
COPY --from=builder /build/.env /app/
COPY --from=builder /build/.env.development /app/
WORKDIR /app

CMD ["./main"]
