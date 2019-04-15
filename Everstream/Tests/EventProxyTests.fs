namespace Everstream.Tests

open Expecto
open Expecto.Expect
open Everstream
open PersistedEventTests
open MemoryStoreTests
open Everstream

module EventProxyTests =

    let newProxy () =
        newStore() |> EventProxy.ofStore

    let append stream data (proxy:EventProxy) =
        proxy.Append stream data
    
    let stream i = Stream.create (SpecificId (sprintf "stream%d" i))

    [<Tests>]
    let Tests =
        testof<EventProxy>
            [
                testAsync "Test store & get" {
                    let proxy = newProxy ()

                    let stream = Stream.create (RandomId Default)
                    let data = intData ()

                    let! expected =
                        proxy |> append stream [data]
                        |> Async.map Result.get
                        |> Async.map snd
                        |> Async.map Array.exactlyOne

                    let! stream', actual =
                        proxy.GetStream stream.Id
                        |> Async.map Result.get
                        |> Async.map (Option.map (fun (s, x) -> s, Array.exactlyOne x))
                        |> Async.map Option.get

                    check |> equal (stream.Id, stream.Version + 1) (stream'.Id, stream'.Version)
                    check |> equal actual.Data data
                    check |> equal actual expected
                }

                testAsync "Test store & get many (single op)" {
                    let proxy = newProxy ()
                    let stream = Stream.create (RandomId Default)

                    let! expected =
                        [1 .. magicNumber]
                        |> List.map (fun _ -> intData ())
                        |> fun x -> proxy |> append stream x
                        |> Async.map Result.get

                    let! actual =
                        stream.Id
                        |> proxy.GetStream
                        |> Async.map Result.get
                        |> Async.map Option.get

                    check |> equal (fst actual).Version (snd expected).Length
                    check |> equal (snd actual) (snd expected)
                }

                testAsync "Test store & get many" {
                    let proxy = newProxy ()

                    let! results =
                        [1 .. magicNumber]
                        |> List.map (fun _ -> coupleOf (fun _ -> intData ()))
                        |> List.mapi (fun i x -> proxy |> append (stream i) x)
                        |> Async.Parallel

                    let expected =
                        results
                        |> Seq.collect (fun x -> x |> Result.get |> snd)
                        |> Seq.sortBy (fun x -> x.StreamId, x.Version)

                    let! actual =
                        proxy.GetHistory Progress.ignore 
                        |> Async.map Result.get
                        |> Async.map Map.toSeq
                        |> Async.map (Seq.collect (fun x -> x |> snd))

                    (expected, actual)
                    ||> Seq.zip
                    |> Seq.iter (fun (a,b) -> check |> persistedEventsEqual a b)
                }

                
                testAsync "Test GetStreams, GetLatest and GetSingleStream" {
                    let proxy = newProxy ()

                    let! actual =
                        proxy.GetLatest typeof<Event>
                        |> Async.map Result.get

                    check |> isNone actual

                    let! expected =
                        proxy.Specialized.GetSingleStream typeof<Event>
                        |> Async.map Result.get
                    
                    check |> equal expected.Version 0

                    let! count =
                        [1 .. magicNumber]
                        |> List.map (fun _ -> intData ())
                        |> fun x -> proxy |> append expected x
                        |> Async.map Result.get
                        |> Async.map (fun x -> (snd x).Length)

                    let! actual = 
                        proxy.Specialized.GetSingleStream typeof<Event>
                        |> Async.map Result.get

                    check |> equal actual.Version count
                }
                
                
                testAsync "Test exceptions" {
                    let proxy = newProxy ()

                    let newStreamS1 = SpecificId "s1" |> Stream.create

                    // test StreamIdAlreadyExists
                    let! result = proxy.Append newStreamS1 [stringData()]
                    let newStream, _ = result |> expectAndGet isOk Result.get

                    let! result = proxy.Append newStreamS1 [stringData()]
                    
                    check |> isErrorOfType result (StreamIdAlreadyExists newStreamS1.Id)

                    // test ExpectedVersionDidNotMatchActualVersion
                    let! result = proxy.Append newStream [stringData()]
                    check |> isOk result

                    let! result = proxy.Append newStream [stringData()]
                    check |> isErrorOfType result (ExpectedVersionDidNotMatchActualVersion (newStreamS1.Id, newStreamS1.Version, newStream.Version))
                }
            ]

