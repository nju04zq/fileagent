```
go run src/fileagent/server/server.go <server_ip> <server_port> <src_path>
```

```
go run src/fileagent/client/client.go <server_ip> <server_port> <dst_path>
```

Server side output

```
Calculating CRC64...
CRC64: 1.40GB 889.49MB/s 1s
CRC64 C715540B50949E13
RD: 1.10GB/1.40GB 1.85MB/s, TX: 1.09GB/1.40GB 1.85MB/s, 10m8s, ETA 2m49s
```

Client side output

```
WR: 1.09GB/1.40GB 1.84MB/s, RX: 1.09GB/1.40GB 1.84MB/s, 10m12s, ETA 2m49s
```