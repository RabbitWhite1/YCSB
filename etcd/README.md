<!--
Copyright (c) 2026 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# etcd binding

`site.ycsb.db.etcd.EtcdClient` (`-db site.ycsb.db.etcd.EtcdClient`, or `bin/ycsb ... etcd`) is a port of
go-ycsb's etcd binding (pingcap/go-ycsb `db/etcd/db.go`, as of go-ycsb 515fd00). It uses the
[jetcd](https://github.com/etcd-io/jetcd) client (0.7.7). It talks to any etcd v3 server.

```
bin/ycsb load etcd -P workloads/workloada -p etcd.endpoints=10.0.0.1:2379,10.0.0.2:2379,10.0.0.3:2379
bin/ycsb run  etcd -P workloads/workloada -p etcd.endpoints=10.0.0.1:2379,10.0.0.2:2379,10.0.0.3:2379
```

## Semantics (same as go-ycsb)

| Operation | Behaviour |
|---|---|
| key | `<table>:<key>`, e.g. `usertable:user6284781860667377211` |
| record | Go's `json.Marshal(map[string][]byte)`: keys sorted, each value standard base64, no spaces. A 10 x 100-byte record is 1481 bytes. |
| read | linearizable `Get`. A missing key is an error (`Return=ERROR`, not `NOT_FOUND`). The record is decoded and every stored field is returned, whatever fields were requested. |
| update | `Put` of the JSON of only the given fields. This replaces the whole record: with workload A's single-field updates, the stored record shrinks to that one field. |
| insert | the same as update |
| delete | `Delete`. It succeeds whether or not the key existed. |
| scan | `Get(start, WithFromKey, WithLimit(n))`. It is an error unless the response's `Count` equals n. That count is the number of *all* keys from the start key onwards, so nearly every scan fails, as in go-ycsb. On success, the result holds n empty (null) entries before the records, as go-ycsb's does. |

Client settings:
- One client is shared by all threads, like go-ycsb's single `clientv3.Client`.
- Requests are spread round-robin over the endpoints, as clientv3's resolver configures.
- Each call waits for a ready connection and has no deadline.

| Property | Default | Meaning |
|---|---|---|
| `etcd.endpoints` | `localhost:2379` | comma-separated; `host:port` gets `http://` (or `https://` with TLS) |
| `etcd.dial_timeout` | 2 s | integer **nanoseconds**, as go-ycsb's `GetDuration` parses it; anything else means the default. Used as jetcd's connect timeout. |
| `etcd.cert_file`, `etcd.key_file`, `etcd.cacert_file` | | required when `etcd.endpoints` contains `https`; the client key must be PKCS#8 PEM (netty) |

Not identical, by construction:
- **Client library.** jetcd vs Go's clientv3 v3.5.2. They issue the same gRPC requests, but their retry policies after RPC failures (e.g. a leader change) differ. With no failures, the behaviour is the same.
- **Workload generators.** Java YCSB's core and go-ycsb's core differ, outside this binding:
  - Hashed key names use a different FNV variant, so the key names differ while the distribution is the same.
  - Random field bytes come from a different alphabet, with the same length.

## Open-loop mode (an addition, off by default)

`etcd.async=true` (requires `-target`) issues each operation without waiting and measures it when it
completes, from its intended start time, like `zookeeper.async`. The operations themselves are the same.

- The shared client connects before the schedule starts.
- `etcd.async.drainTimeoutMs` (default 10 minutes) bounds the wait for in-flight requests at the end and between rate-sweep stages.

## Verification

- `GoJsonTest` checks the record encoding and decoding byte for byte against vectors produced by Go's `encoding/json` (`src/test/resources/go-json-vectors.*`). The vectors include escaping, key order, empty and null values, duplicate keys, whitespace, missing padding and malformed input.
- Against a local etcd, with go-ycsb built from the same tree and `insertorder=ordered` (so key names match), these were run in both directions:
  - load with one tool, then read and update with the other: no errors;
  - raw stored records: Go re-marshals the Java-written record to identical bytes;
  - missing keys: about 50% `READ` errors with `recordcount` doubled, in both tools;
  - scans: all or nearly all fail in both tools.
