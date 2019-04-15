namespace Everstream

open System.Collections.Generic

[<CustomEquality; CustomComparison>]
type PersistedEvent =
    {
        Id:             EventId
        Partition:      Partition
        StreamId:       StreamId
        Version:        Version
        Type:           string
        Data:           EventData
        Meta:           MetaData
        Timestamp:      Timestamp
    }

    override x.Equals(another) =
        match another with
        | :? PersistedEvent as event -> (x.Id = event.Id)
        | _ -> false

    override x.GetHashCode() = hash x.Id
    interface System.IComparable with
        member x.CompareTo another =
            match another with
                | :? PersistedEvent as y -> compare x.Id y.Id
                | _ -> invalidArg "another" "Cannot compare values of different types."


    member x.Identify () = (x.Meta :> IDictionary<string,obj>).["$name"] :?> string
    member x.Describe () = (x.Meta :> IDictionary<string,obj>).["$description"] :?> string



    
[<RequireQualifiedAccess>]
module Events =
    open System.Dynamic
    open System.Collections.Generic
    open System

    let data (events:PersistedEvent seq) : 'Event list = events |> List.ofSeq |> List.map (fun e -> e.Data :?> 'Event)

    let internal newEvent (serialization:Serialization) partition (stream:Stream) (offset:int) (causingEvent:PersistedEvent option) (data:EventData) meta =
        let cast expando = (expando :> IDictionary<string,obj>)
        let meta = defaultArg meta (ExpandoObject()) |> cast        
        let guid = Guid.NewGuid()

        meta.["$correlationId"] <-
            match causingEvent with None -> guid.ToString() | Some e -> downcast (cast e.Meta).["$correlationId"]
        meta.["$causationId"] <-
            causingEvent |> Option.map (fun e -> e.Id)
        meta.["$name"] <-
            data.GetType().Name
        meta.["$description"] <-
            serialization.Describe data
        meta.["$build"] <-
            data.GetType().Assembly.GetName().Version |> string

        {
            Id = guid
            Partition = partition
            StreamId = stream.Id
            Version = stream.Version + 1 + offset
            Type = data.GetType().DeclaringType.FullName
            Data = data
            Meta = meta :?> ExpandoObject
            Timestamp = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        }

    let internal streamOf (events:PersistedEvent list) =
        let latest = (events |> List.maxBy (fun x -> x.Version))
        {Id = latest.StreamId; Version = latest.Version}


    let internal versionOf (events:PersistedEvent list) =
        (streamOf events).Version

    [<Obsolete("I'm not sure this is necessary")>]
    let zero partition (eventDataType:System.Type) =
        {
            Id = Guid.NewGuid()
            Partition = partition
            StreamId = null
            Version = -1
            Type = eventDataType.FullName
            Data = null
            Meta = null
            Timestamp = DateTimeOffset.MinValue.ToUnixTimeMilliseconds()
        }