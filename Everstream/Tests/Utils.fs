namespace Everstream.Tests

open Expecto
open Expecto.Expect
open System
open System.Reflection
open Everstream
open Microsoft.FSharp.Reflection

type Flags = System.Reflection.BindingFlags

[<AutoOpen>]
[<Diagnostics.DebuggerStepThrough>]
module Utils =

    let private chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
       


    /// A finite constant integer (42) ready to be used in tests
    let magicNumber = 42

    type Random with
        /// Returns a random string of the specified length
        member x.NextString (?length:int) =
            [1 .. (defaultArg length magicNumber)]
            |> List.map (fun _ -> chars.[x.Next(chars.Length)])
            |> List.toArray
            |> String

    /// Generates pseudo random numbers using a constant seed
    let pseudoRandom = Random(magicNumber)
       
    /// Generates a small int
    let rnd () = pseudoRandom.Next(1, magicNumber)

    /// Provides a non-specific error message when a more specific message is deemed unnecessary
    let check = "This test failed"

    /// List of one or two elements, starting with i = 1
    let coupleOf mapping = [1 .. pseudoRandom.Next (1, 3)] |> List.map mapping



    /// Build a test list, automatically naming it after the given type (or marker interface)
    let testof<'TType> =
        let name =
            match typeof<'TType> with
            | t when t.IsInterface && t.Name.EndsWith("TypeOf") -> t.DeclaringType.Name
            | t -> t.Name
        testList (sprintf "%s tests" name)
        


    /// Get tests of the assembly containing the specified marker interface
    //  This is necessary because https://github.com/haf/expecto/issues/94
    let getTests<'IMarker> =
        let isTests (pi:PropertyInfo) =
            pi.IsDefined typeof<TestsAttribute>

        let getval (pi:PropertyInfo) = pi.GetValue(null) :?> Test

        let asm = Assembly.GetAssembly(typeof<'IMarker>)

        let tests =
            asm.GetTypes()
            |> Seq.map (fun x -> x.GetProperties(BindingFlags.Public ||| BindingFlags.Static))
            |> Seq.concat
            |> Seq.filter isTests
            |> Seq.map getval
            |> Seq.rev

        if tests |> Seq.exists (fun x -> Test.isFocused x) then
            tests |> Seq.filter (fun x -> Test.isFocused x)
        else tests


    /// Adds a more specific message to the provided custom message
    let private addTo (message:string) (newMessage:string) = 
        let message = message.Trim().TrimEnd('.')
        if String.IsNullOrWhiteSpace message then newMessage
        else sprintf "%s. %s" message newMessage




    /// Tests that sequence contains exactly one element, and return it for further testing
    let exactlyOne actual message =
        let msg =
            "The provided 'actual' sequence did not have exactly one element"
            |> addTo message
        hasCountOf actual 1u (fun x -> true) msg
        Seq.exactlyOne actual |> ignore



    /// Test expectation and return actual
    let expectAndGet expectation mapping actual = actual |> expectation <| check; actual |> mapping



    /// Result is error and its error = given error
    let isErrorOfType actual expectedError message =
        match actual with
        | Error e -> equal e expectedError ("The actual error did not match the expected error" |>  addTo message)
        | _ -> isTrue false ("The actual Result did not contain any error" |> addTo message)



    /// Tests if two sequences contain the same elements regardless of order
    let setEqual (actual:'TActual seq) (expected:'TActual seq) message =
        let msg =
            "The given 'actual' and 'expected' are not the same set"
            |> addTo message
        containsAll actual expected msg
        containsAll expected actual msg



    /// Tests that two inputs are the same as per IEquatable implementation as well as per public instance property-wise comparison
    let propertiesEqual (ignore:string list) (actual:'TActual) (expected:'TActual) message =
        let msg text = text |> addTo message
        equal actual expected (msg "The provided 'actual' and 'expected' are not equal as per defined IEquatable implementation")

        typeof<'TActual>.GetProperties(Flags.Public ||| Flags.Instance)
        |> Seq.filter (fun pi -> not (ignore |> List.contains pi.Name))
        |> Seq.map (fun pi -> pi.Name, pi.GetValue(actual), pi.GetValue(expected))
        |> Seq.iter (fun (name, a, b) -> equal a b (sprintf "Property '%s' does not match" name |> msg))



    /// Returns true if sequence elements are one and the same
    let allOneAndTheSame (actual:'TActual seq) message =
        allEqual actual (Seq.head actual) ("The elements of 'actual' are not all equal" |> addTo message)



    /// Check that two persisted events are equal, ignoring Timestamp and order of MetaData
    let persistedEventsEqual (actual:PersistedEvent) (expected:PersistedEvent) message =
        propertiesEqual ["Timestamp"; "Meta"] actual expected message
        setEqual actual.Meta expected.Meta message

    
    /// Check that two persisted event sequences are the same order and content-wise
    let persistedEventsAllEqual (actual:PersistedEvent seq) (expected:PersistedEvent seq) message =
        let expected = expected |> Array.ofSeq

        // count is the same
        hasCountOf actual (uint32 expected.Length) (fun _ -> true) "The two sequences do not have the same number of events"

        // individual ids are the same
        let error = "The two sequences do not have the same stream ids"
        let actualIds = actual |> Seq.map (fun x -> x.StreamId)
        let expectedIds = expected |> Seq.map (fun x -> x.StreamId)
        sequenceContainsOrder actualIds expectedIds error
        sequenceContainsOrder expectedIds actualIds error

        // event properties are the same
        actual |> Seq.iteri (fun i x -> persistedEventsEqual x expected.[i] message)



[<RequireQualifiedAccess>]
[<Diagnostics.DebuggerStepThrough>]
module Result =

    let get =
        function
        | Ok x -> x
        | Error e -> failwith (sprintf "Attempted to access error result: %A" e)


[<Diagnostics.DebuggerStepThrough>]
type Async() = class end with

    /// Ignore result (unless it's Error)
    static member IgnoreResult (result') =
        async {
            let! result = result'
            Result.get result |> ignore
        }   


[<RequireQualifiedAccess>]
module Async =

    /// Map computation result whenever it's ready
    let map mapping computation =
        async {
            let! x = computation
            return mapping x
        }

    /// Returns an async computation result consisting of a static value
    let ofConst x = async { return x }