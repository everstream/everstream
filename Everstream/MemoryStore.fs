namespace Everstream

open Everstream.Query
open System.Data

module Async = let ofConst x = async { return x }

module MemoryStore =
    
    type internal TypeOf = interface end

    let newStore serialization partition =

        let events =
            System.Collections.Concurrent.ConcurrentDictionary<Partition * StreamId, Set<PersistedEvent>>()

        let getPartition (x:Partition * StreamId) = let partition, _ = x in partition

        let partitionEvents () =
            events
            |> Seq.filter (fun x -> getPartition x.Key = partition)
            |> Seq.collect (fun x -> x.Value)

        let queryStreams (query:EventQuery) =
            match query with
            | Stream(Full id) ->
                events.[partition, id]
                |> Set.toSeq

            | Stream(SinceVersion stream) ->
                events.[partition, stream.Id]
                |> Set.filter (fun x -> x.Version > stream.Version)
                |> Set.toSeq

            | Type t ->
                partitionEvents ()
                |> Seq.filter (fun x -> x.Type = t.FullName)

            | All(SinceTimestamp ts) ->
                partitionEvents ()
                |> Seq.filter (fun x -> x.Timestamp >= ts)

            | All(SinceTheBeginning) ->
                partitionEvents ()

            |> Seq.sortBy (fun x -> x.StreamId, x.Version)

        let appendEventsWithMetaData
            (newEvents:(EventData * MetaData option) list)
            (stream:Stream)
            (causingEvent:PersistedEvent option) =

            let create = Events.newEvent serialization partition stream

            // create new PersistedEvents
            let newEvents =
                newEvents
                |> List.mapi (fun i (data,meta) -> create i causingEvent data meta)

            // some helpers
            let currentMaxVersion () = events.[partition, stream.Id] |> Set.toSeq |> Seq.maxBy (fun x -> x.Version)
            let newEventsMinVersion () = newEvents |> Seq.minBy (fun x -> x.Version)
            let existingSet () = events.[partition, stream.Id]
            let newSet () = events.[partition, stream.Id] |> Set.union (newEvents |> Set.ofList)

            // validate append operation
            match stream.Version with

            | 0 ->
                if not (events.TryAdd((partition, stream.Id), newEvents |> Set.ofList)) then
                    // stream already exists
                    raise(StreamIdAlreadyExistsException(stream.Id))

            | _ when currentMaxVersion().Version <> newEventsMinVersion().Version - 1 ->
                    // enexpected stream version
                    raise(ExpectedVersionDidNotMatchActualVersionException(stream.Id, stream.Version - 1, stream.Version))

            | _ -> 
                if not (events.TryUpdate((partition, stream.Id), newSet(), existingSet())) then
                    // unknown error
                    raise(DBConcurrencyException())                    

            newEvents |> Async.ofConst

        {
            PartitionName = partition

            GetStream =
                fun _ q ->
                    queryStreams (Stream q) |> List.ofSeq |> Async.ofConst

            GetStreams =
                fun _ t ->
                    queryStreams (Type t)
                    |> List.ofSeq
                    |> List.groupBy (fun e -> {Id = e.StreamId; Version = e.Version})
                    |> Map.ofList
                    |> Async.ofConst

            GetHistory =
                fun _ hq ->
                    queryStreams (All hq)
                    |> List.ofSeq
                    |> Async.ofConst

    
            Append =
                fun causingEvent newEvents stream ->                
                    let eventsWithMetaData = newEvents |> Seq.map (fun e -> e, None) |> List.ofSeq
                    appendEventsWithMetaData eventsWithMetaData stream causingEvent
        }

    //interface IEventStore with
        
    //    member val PartitionName = partition

    //    member x.GetStream query = x.GetStream query

    //    member x.GetStreams eventDataType = x.GetStreams eventDataType

    //    member x.GetHistory query = x.GetHistory query

    //    member x.Append (stream: Stream, newEvents: EventData seq, ?causingEvent: PersistedEvent) =
    //        match causingEvent with
    //        | Some e -> x.Append (stream, newEvents, e)
    //        | None -> x.Append (stream, newEvents)
           
