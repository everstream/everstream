[<RequireQualifiedAccess>]
module Map

open Everstream

let private keymatch id (stream:Stream) _ = stream.Id = id

let tryFindKeyUsingId id =
    Map.tryFindKey (keymatch id)

let findKeyUsingId id =
    Map.findKey (keymatch id)

let containsStreamId id map =
    tryFindKeyUsingId id map |> Option.isSome

let findStreamId id map =
    Map.find (findKeyUsingId id map) map

let tryFindStreamId id map =
    tryFindKeyUsingId id map
    |> Option.map (fun key -> Map.find key map)        