namespace Everstream


type EventStoreError =
    | StreamIdAlreadyExists of streamId:StreamId
    | ExpectedVersionDidNotMatchActualVersion of streamId:StreamId * expectedVersion:Version * actualVersion:Version
    | InfrastructureException of System.Exception


type EventProxyResult<'TSuccess> = Result<'TSuccess, EventStoreError>


type EventProxy = 
    {
        GetStream : StreamId -> Async<EventProxyResult<(Stream * PersistedEvent []) option>>
        GetStreams : EventDataType -> Async<EventProxyResult<Map<Stream, PersistedEvent []>>>
        GetHistory : IProgress -> Async<EventProxyResult<Map<Stream, PersistedEvent []>>>
        Append : Stream -> EventData list -> Async<EventProxyResult<Stream * PersistedEvent []>>
        AppendNext : PersistedEvent -> Stream -> EventData list -> Async<EventProxyResult<Stream * PersistedEvent []>>
        GetLatest: EventDataType -> Async<EventProxyResult<PersistedEvent option>>
        Specialized: Specialized
    }
and Specialized =
    {
        GetSingleStream: EventDataType -> Async<EventProxyResult<Stream>>
    }

module EventProxy =

    type private DispatcherMessage =
        | GetStream of StreamId * AsyncReplyChannel<EventProxyResult<(Stream * PersistedEvent []) option>>
        | GetStreams of EventDataType * AsyncReplyChannel<EventProxyResult<Map<Stream, PersistedEvent []>>>
        | GetHistory of IProgress * AsyncReplyChannel<EventProxyResult<Map<Stream, PersistedEvent []>>>
        | Append of PersistedEvent option * Stream * EventData list * AsyncReplyChannel<EventProxyResult<Stream * PersistedEvent []>>


    let ofStore (store:EventStore) =

        let nonLetterOrDigit = System.Text.RegularExpressions.Regex("[^a-zA-Z0-9]")

        /// Add events to map 
        let add (events:PersistedEvent list) (map:Map<Stream, PersistedEvent []>) =
            let find stream map = map |> Map.tryFind stream |> Option.defaultValue [||]
            let append events stream =
                events |> List.toArray |> Array.append (find stream map)
                // distinct can be removed when GetStreams is modified to only return the delta
                |> Array.distinct

            events
            |> List.groupBy (fun e -> e.StreamId)
            |> List.map (fun (_, x) -> Events.streamOf x, Events.streamOf x |> append x)
            |> List.append (map |> Map.toList)
            |> Map.ofList

        let update ts p history =
            async {
                
                let! newEvents =
                    Query.SinceTimestamp ts
                    |> store.GetHistory p

                let history =
                    history
                    |> add newEvents

                let ts =
                    match newEvents with
                    | [] -> 0L
                    | x -> x |> List.maxBy (fun e -> e.Timestamp) |> fun e -> e.Timestamp

                return
                    ts, history
            }

        let mailbox =
            MailboxProcessor.Start(fun (inbox:MailboxProcessor<DispatcherMessage>) ->
                let error (e:System.Exception) =
                    Error <|
                        match e with

                        | :? StreamIdAlreadyExistsException as e ->
                            StreamIdAlreadyExists e.StreamId

                        | :? ExpectedVersionDidNotMatchActualVersionException as e ->
                            ExpectedVersionDidNotMatchActualVersion (e.StreamId, e.ExpectedVersion, e.ActualVersion)

                        | e -> InfrastructureException e
                    
                let rec loop ts (history:Map<Stream, PersistedEvent []>) =
                
                    async {
                    
                        let! msg = inbox.Receive()

                        match msg with

                        | GetStream (streamId, channel) ->

                            try
                                // update
                                let! ts, history =
                                    history |> update ts Progress.ignore
                            
                                // reply
                                history
                                |> Map.tryFindKeyUsingId streamId
                                |> Option.map (fun key -> key, Map.find key history)
                                |> Ok |> channel.Reply
                            
                                // return
                                return! loop ts history
                            
                            with e ->
                                error e |> channel.Reply;
                                return! loop ts history

                        | GetStreams (eventDataType, channel) ->

                            try
                                // update
                                let! ts, history =
                                    history |> update ts Progress.ignore

                                // reply
                                history
                                |> Map.filter (fun _ x -> x.[0].Type = eventDataType.FullName)
                                |> Ok |> channel.Reply

                                // return
                                return! loop ts history
                            
                            with e ->
                                error e |> channel.Reply;
                                return! loop ts history
                            
                        | GetHistory (p, channel) ->

                            try
                                // update
                                let! ts, history =
                                    history |> update ts p
                            
                                // reply
                                history
                                |> Ok |> channel.Reply

                                // return
                                return! loop ts history
                            
                            with e ->
                                error e |> channel.Reply;
                                return! loop ts history


                        | Append (causingEvent, stream, events, channel) ->

                            try
                                let! newEvents =
                                        stream
                                        |> store.Append causingEvent events

                                // update history for this particular stream
                                // but don't change timestamp
                                let history =
                                    history |> add newEvents
                            
                                // reply
                                history
                                |> Map.tryFindKeyUsingId stream.Id
                                |> function
                                    | Some key -> key, Map.find key history
                                    | None when events.IsEmpty -> stream, Array.empty<PersistedEvent>
                                    | _ -> invalidOp "New events missing from map."
                                |> Ok
                                |> channel.Reply

                                // return
                                return! loop ts history
                            
                            with e ->
                                error e |> channel.Reply
                                return! loop ts history
                                
                        }
                
                Map.empty<Stream, PersistedEvent []>
                |> loop 0L
            )
        
        // disable silent exceptions
        mailbox.Error.Add (fun e -> raise(e))

        let getLatestEvent (t:EventDataType) =
            async {
                let! streams = mailbox.PostAndAsyncReply (fun channel -> GetHistory (Progress.ignore, channel))

                return
                    streams
                    |> Result.map (
                        function
                        | x when x.IsEmpty -> None
                        | streams ->
                            streams
                            |> Map.filter (fun _ x -> x |> Seq.exists (fun e -> e.Type = t.FullName))
                            |> Map.toList
                            |> List.tryLast
                            |> Option.map (fun (stream, events) -> (stream, Array.last events)))
            }

        // return
        {
            GetStream = fun stream ->
                mailbox.PostAndAsyncReply (fun channel ->
                    GetStream (stream, channel))

            GetStreams = fun t ->
                mailbox.PostAndAsyncReply (fun channel ->
                    GetStreams (t, channel))

            GetHistory = fun p ->
                mailbox.PostAndAsyncReply (fun channel ->
                    GetHistory (p, channel))
                    
            Append = fun stream events ->
                mailbox.PostAndAsyncReply (fun channel ->
                    Append (None, stream, events, channel))

            AppendNext = fun causingEvent stream events ->
                mailbox.PostAndAsyncReply (fun channel ->
                    raise(System.NotImplementedException()))
                    //Append (Some causingEvent, stream, events, channel))

            GetLatest = fun t ->
                async {
                    let! result = getLatestEvent t
                    return
                        result
                        |> function
                            | Error x -> Error x
                            | Ok None -> Ok None //(Events.zero store.PartitionName t)
                            | Ok (Some (_,x)) -> Ok (Some x)
                }

            Specialized =
                {
                    GetSingleStream =
                        fun t ->
                            async {

                                match! getLatestEvent t with

                                | Error x -> return Error x

                                | Ok (Some(stream, _)) ->
                                    return Ok stream

                                | Ok None ->
                                    return
                                        SpecificId(nonLetterOrDigit.Replace(t.FullName, "").ToLowerInvariant())
                                        |> Stream.create
                                        |> Ok
                            }
                }
        }

