namespace Everstream.Tests

open PersistedEventTests
open Everstream
open Expecto
open Expecto.Expect

module MemoryStoreTests =

    let newStore () =
        MemoryStore.newStore (Default.Serialization []) Default.Partition.Temporary

    [<Tests>]
    let Tests =

        let append stream data (store:EventStore) =
            store.Append None data stream

        testof<MemoryStore.TypeOf>
            [
                testAsync "Test store & get" {
                    let store = newStore ()

                    let! expected =
                        store
                        |> append (Stream.create (RandomId Default)) [intData ()]
                        |> Async.map (expectAndGet exactlyOne List.exactlyOne)

                    let! actual =
                        Query.Full expected.StreamId
                        |> store.GetStream Progress.ignore
                        |> Async.map (expectAndGet exactlyOne List.exactlyOne)

                    check |> persistedEventsEqual actual expected
                }

                testAsync "Test store & get many (single op)" {
                    let store = newStore ()
                    let stream = Stream.create (RandomId Default)

                    let! expected =
                        [1 .. magicNumber]
                        |> List.map (fun _ -> intData ())
                        |> fun x -> store |> append stream x

                    let! actual =
                        Query.Full stream.Id |> store.GetStream Progress.ignore

                    check |> equal actual expected
                }

                testAsync "Test store & get many" {
                    let store = newStore ()
                    let stream i = Stream.create (SpecificId (sprintf "stream%d" i))

                    let! expected =
                        [1 .. magicNumber]
                        |> List.map (fun _ -> coupleOf (fun _ -> intData ()))
                        |> List.mapi (fun i x -> store |> append (stream i) x)
                        |> Async.Parallel
                        |> Async.map List.ofArray
                        |> Async.map (List.collect id)
                        |> Async.map (List.sortBy (fun x -> x.StreamId, x.Version))

                    let! actual =
                        Query.SinceTheBeginning
                        |> store.GetHistory Progress.ignore

                    check |> equal actual expected
                }
            ]

