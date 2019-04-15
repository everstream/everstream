namespace Everstream

module Query =

    type HistoryQuery =
        | SinceTheBeginning
        | SinceTimestamp of Timestamp

    type StreamQuery =
        | SinceVersion of Stream
        | Full of StreamId
        with member x.Id = match x with SinceVersion s -> s.Id | Full id -> id
    
    type EventQuery =
        | Stream of StreamQuery
        | Type of EventDataType
        | All of HistoryQuery