# go-bitcask

Yet another Bitcask implementation in Go

## Build

```
go build
```

## Run

To start server

```
./bitcask -c config.json
```

Client is not available yet, but it's always accessible via TCP connection

```
------------------------------------------------------------
🐼 ~ » echo -n "set foo bar" | nc localhost 9876
OK%                                                                                                                                                                          ------------------------------------------------------------
🐼 ~ » echo -n "get foo" | nc localhost 9876
bar%                                                                                                                                                                         ------------------------------------------------------------
🐼 ~ » echo -n "del foo" | nc localhost 9876
OK%                                                                                                                                                                          ------------------------------------------------------------
🐼 ~ » echo -n "get foo" | nc localhost 9876
Key not found: foo%
```
