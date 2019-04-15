namespace Everstream

type Partition = string
type EventId = System.Guid
type StreamId = string
type Version = int
type EventData = obj
type EventDataType = System.Type
type MetaData = System.Dynamic.ExpandoObject
type Timestamp = int64

type IdLength =
    | Default
    | Exactly of int16

module IdGenerator =
    
    let private Rng = System.Random()
    
    let newId(length:IdLength) =

        let l = match length with Default -> 16 | Exactly l -> int l
        if l < 1 then invalidArg "length" "Min length is 1."
        if l > 28 then invalidArg "length" "Max length is 28."

        (System.Guid.NewGuid().ToString().Split('-')
        |> String.concat "").ToCharArray()
        |> Array.sortBy (fun _ -> Rng.Next())
        |> Array.take l
        |> System.String.Concat

type NewStreamId =
    | SpecificId of string
    | RandomId of IdLength

[<CustomEquality; CustomComparison>]
type Stream =
    {
        Id: StreamId
        Version: Version
    }
    with
        override x.Equals(another) =
            match another with
            | :? Stream as stream -> (x.Id = stream.Id)
            | :? StreamId as id -> (x.Id = id)
            | _ -> false
        override x.GetHashCode() = hash x.Id
        interface System.IComparable with
            member x.CompareTo another =
                match another with
                    | :? Stream as y -> compare x.Id y.Id
                    | _ -> invalidArg "another" "Cannot compare values of different types."

exception StreamIdAlreadyExistsException of StreamId:StreamId
exception ExpectedVersionDidNotMatchActualVersionException of StreamId:StreamId * ExpectedVersion:Version * ActualVersion:Version

module Stream =

    let create =
        function
        | SpecificId id -> {Id = id; Version = 0}
        | RandomId l -> {Id = IdGenerator.newId l; Version =0}

//type StreamVersion =
//    | NewStream of NewStreamId
//    | Latest of StreamId
//    | ExactVersion of Stream
//    with
//    member x.Id =
//        match x with
//            | Latest(id) -> id
//            | ExactVersion(x) -> x.Id
//            | NewStream _ -> invalidOp "Cannot retrieve the id of a stream before it's created."

