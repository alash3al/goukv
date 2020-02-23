Overview
=========
> `goukv` is a unified interface for golang based key-value stores, it is easy to add any backend provider.

Available Providers
===================
- `badgerdb`: [BadgerDB](/badgerdb)
- `golveldb`: [GolevelDB](/goleveldb)

Why
===
> I just built this to be used in my side projects, but you can use it with no worries, it is production ready, and I'm open for any idea & contribution.

Backend Stores Rules
=====================
> just keep it simple stupid!
- Use the `map[string]interface{}` as your options.
- `Nil` value means *DELETE*.
- Respect the `Entry` struct.
- Respect the `ScanOpts` struct.
