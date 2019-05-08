[![everstream logo](https://raw.github.com/everstream/gists/master/everstream.png)](http://everstream.org)
<!--# everstream-->
  
<br/>

Postgres backed event store written in F# perhaps best described as barebones, or beginner-friendly, but in no way intended to compete with options such as the battle-tested [jet/equinox](https://github.com/jet/equinox/blob/master/README.md) or the increasingly compatible [Dzoukr/CosmoStore](https://github.com/Dzoukr/CosmoStore/blob/master/README.md), the latter of which also works with PostgreSQL now. Definitely look into those for something more tried and true.

### For those who download first and ask questions later

| Package | Description | NuGet
|---|---|---|
| Everstream.PostgreSQL | PostgreSQL-backed event store | [![NuGet](https://img.shields.io/nuget/v/Everstream.PostgreSQL.svg?style=flat)](https://www.nuget.org/packages/Everstream.PostgreSQL/) |
| Everstream | API definition (you don't need this) | [![NuGet](https://img.shields.io/nuget/v/Everstream.svg?style=flat)](https://www.nuget.org/packages/Everstream/) |

## The API

Here's the API, short, sweet, and to the point.
```fsharp
type EventStore =
    {
        /// Get a specific stream identified by a stream query
        GetStream : IProgress -> StreamQuery -> Async<PersistedEvent list>

        /// Get all streams of a specific type
        GetStreams : IProgress -> EventDataType -> Async<Map<Stream, PersistedEvent list>>

        /// Get all events identified by a history query
        GetHistory : IProgress -> HistoryQuery -> Async<PersistedEvent list>

        /// Append events, optionally as consequences of an existing event
        Append : PersistedEvent option -> EventData list -> Stream -> Async<PersistedEvent list>
    }
```
You may be thinking _"hold on, that is pretty simple, but what are these query paramters hiding"_.  
Fair enough, here they are:
```fsharp
type Stream =
    {
        Id: StreamId // string
        Version: Version // int
    }
    
module Query =

    type HistoryQuery =
        | SinceTheBeginning
        | SinceTimestamp of Timestamp // long

    type StreamQuery =
        | SinceVersion of Stream
        | Full of StreamId // string
```
To make things easier, there's an `EventProxy` agent that you can use that implements the `EventStore` API above. This proxy takes care of maintaining the history of events in memory, and only fetching new ones whenever an update is necessary. Its API mirrors the `EventStore` API, adding a few more functions for convenience. This proxy is not particularly optimized, it'll get you up and running with no effort, but eventually you may want to replace it with something more optimized for your own particular needs.

## Why, oh why

Everstream was created with the very specific purpose of having an F# library with Event persistence as its single concern. While I myself use it with CQRS, there is no reference to commands or projections, the storing logic is completely distilled into its own independent package here. Think of Everstream as monkey sees Event, monkey writes Event, and monkey eventually fetches a previously written Event. But monkey does not even know (or care) what an Event is.

### Everything is a projection

In line with the above mission statement, Everstream is incredibly feature-light and will always be that way. The idea is that most of what you'll do with events are projections (really, think about it), and thus should be defined elsewhere. In my solutions, the projects that define projections — no pun intended — don't even reference Everstream. It's easy to fall into the trap of mixing everything Event-related together in your code, when in fact, Event persistence is an infrastructure concern, it's unrelated to business logic which is what Events are.

## Pitch in

Since this is my first project here, I'm looking for contributors with experience in Github repository management, Nuget package management or both. This package does have some rudimentary tests using Expecto, but this part could definitely use some love, so if you like writing tests for breakfast and `F# |> U <3`, please wear some aluminum foil over your head so I can see you.

## Disclaimer

This is not meant to be used by Fortune 500 companies, the stability of the API is far less important than it being as concise and self-documenting as possible. Expect new versions to change the API using `[<Obsolete>]` whenever possible so as to not break your code without a fair warning. That is unless you update your packages once a year, in which case, shame on you... and me, I do it too.

## Examples

_I will try to add some examples asap, feel free to tweet [@fishyrock](https://twitter.com/fishyrock) and nag me out of my procrastination zone..._
