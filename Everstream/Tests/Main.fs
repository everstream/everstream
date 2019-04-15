

open Expecto
open System.Reflection
open System
open System.Diagnostics
open Everstream.Tests

[<EntryPoint>]
let main _ =

    // Run all tests in this assembly
    let result = getTests<Everstream.PersistedEvent> |> Seq.map (runTests defaultConfig) |> Seq.sum

    result