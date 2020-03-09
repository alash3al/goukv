Overview
=========
[![](https://godoc.org/github.com/alash3al/goukv?status.svg)](https://godoc.org/github.com/alash3al/goukv)
> `goukv` is an abstraction layer for golang based key-value stores, it is easy to add any backend provider.

Available Providers
===================
- `badgerdb`: [BadgerDB](/providers/badgerdb)
- `golveldb`: [GolevelDB](/providers/goleveldb)

Why
===
> I just built this to be used in my side projects such as [redix(v2)](https://github.com/alash3al/redix/tree/v2), but you can use it with no worries, it is production ready, and I'm open for any idea & contribution.

Backend Stores Rules
=====================
> just keep it simple stupid!
- Use the `map[string]interface{}` as your options.
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
    _ "github.com/alash3al/goukv/providers/goleveldb"
)

func main() {
    db, err := goukv.Open("goleveldb", map[string]interface{}{
        "path": "./db",
    })

    if err != nil {
        panic(err.Error())
    }

    defer db.Close()

    db.Put(goukv.Entry{
        Key: []byte("k1"),
        Value: []byte("v1"),
        TTL: time.Second * 10,
    })

    fmt.Println(db.Get([]byte("k1")))
}

```