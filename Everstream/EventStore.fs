namespace Everstream

open Query
type IProgress = System.IProgress<float>

module Progress =

    let ignore = System.Progress<double>(System.Action<double>(fun _ -> ())) :> IProgress

// TODO interface was originally for interop purposes, but interop will require more than an interface
// for instance, lists should be seq, async should be task, etc...
//type IEventStore =
//    abstract member GetStream: query:StreamQuery -> Async<PersistedEvent list>
//    abstract member GetStreams: eventDataType:EventDataType -> Async<Map<Stream, PersistedEvent list>>
//    abstract member GetHistory: query:HistoryQuery -> Async<PersistedEvent list>
//    abstract member Append: stream:Stream * events:EventData seq * causingEvent:PersistedEvent option -> Async<PersistedEvent list>
//    abstract member PartitionName : string with get

type EventStore =
    {
        PartitionName : string

        GetStream : IProgress -> StreamQuery -> Async<PersistedEvent list>
        GetStreams : IProgress -> EventDataType -> Async<Map<Stream, PersistedEvent list>>
        GetHistory : IProgress -> HistoryQuery -> Async<PersistedEvent list>
        Append : PersistedEvent option -> EventData list -> Stream -> Async<PersistedEvent list>
    }