namespace Everstream.PostgreSQL

open Everstream
open Everstream.Query
open Npgsql.FSharp
open Everstream.PostgreSQL.Db

module Store =



    /// Append the given list of events for the specified stream with their metadata if any
    let private appendEventsWithMetaData
        (persistEvents:bool)
        (serialization:Serialization)
        (db:string)
        (partition:Partition)
        (events:(EventData * MetaData option) list)
        (stream:Stream)
        (causingEvent:PersistedEvent option) =

        let partition = if persistEvents then partition else Default.Partition.Temporary
        let create = Events.newEvent serialization partition stream

        async {

            let newEvents =
                events
                |> List.mapi (fun i (data,meta) -> create i causingEvent data meta)
            

            let! _ = newEvents |> insertMany persistEvents serialization partition db

            return newEvents
        }



    /// Append the given list of events for the specified stream
    let private appendEvents persistEvents serialization db partition originalEvent events stream =
        appendEventsWithMetaData persistEvents serialization db partition (events |> List.map (fun e -> e, None)) stream originalEvent



    /// Get an EventStore backed by PostgreSQL
    let create persistEvents serialization partition db =
        {
            GetStream = fun p q ->

                queryStreams persistEvents serialization db partition (Stream q) p


            GetStreams = fun p t ->

                queryStreams persistEvents serialization db partition (Type t) p
                |> Async.map (fun events ->
                    events
                    |> List.groupBy (fun e -> {Id = e.StreamId; Version = e.Version})
                    |> Map.ofList)


            GetHistory = fun p hq ->

                queryStreams persistEvents serialization db partition (All hq) p


            Append = appendEvents persistEvents serialization db partition


            PartitionName = partition
        }

//type PostgresEventStore(serialization, partition, db) =
//    let store = EventStore.getStore serialization partition db
//    interface IEventStore with
        
//        member val PartitionName = partition
//        member x.GetStream query = store.GetStream query
//        member x.GetStreams eventDataType = store.GetStreams eventDataType
//        member x.GetHistory query = store.GetHistory query
//        member x.Append (stream: Stream, events: EventData seq, ?causingEvent: PersistedEvent) =
//            store.Append causingEvent (events |> List.ofSeq) stream