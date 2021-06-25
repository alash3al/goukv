Overview
=========
[![](https://godoc.org/github.com/alash3al/goukv?status.svg)](https://godoc.org/github.com/alash3al/goukv)
> `goukv` is an abstraction layer for golang based key-value stores, it is easy to add any backend provider.

Available Providers
===================
- `badgerdb`: [BadgerDB](/providers/badgerdb)
- `leveldb`: [levelDB](/providers/leveldb)
- `postgres`: [Postgresql](/providers/postgres)

Backend Stores Rules
=====================
> just keep it simple stupid!
- `Nil` value means *DELETE*.
- Respect the `Entry` struct.
- Respect the `ScanOpts` struct.
- On key not found, return `goukv.ErrKeyNotFound`, this replaces `has()`.

Example
=======
```go
package main

import (
    "time"
    "fmt"
    "github.com/alash3al/goukv" 
    _ "github.com/alash3al/goukv/providers/leveldb"
)

func main() {
    db, err := goukv.Open("leveldb", "./")

    if err != nil {
        panic(err.Error())
    }

    defer db.Close()

    db.Put(&goukv.Entry{
        Key: []byte("k1"),
        Value: []byte("v1"),
        TTL: time.Second * 10,
    })

    fmt.Println(db.Get([]byte("k1")))
}

```
