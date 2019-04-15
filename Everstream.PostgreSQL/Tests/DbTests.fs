namespace Everstream.PostgreSQL.Tests

open Expecto
open Expecto.Expect
open Everstream
open Everstream.Tests.Utils
open Everstream.PostgreSQL
open Everstream.PostgreSQL.Db
open Everstream.PostgreSQL.Utils
open Everstream.Query
open Everstream.Tests.PersistedEventTests

module StoreTests =

    let insertMany db =
        insertMany true (Default.Serialization []) Static.partition.Value db

    let queryStreams db =
        queryStreams true (Default.Serialization []) db Static.partition.Value

    let db = Static.db.Value

    let clearTestDb =
        Db.clearPartition db None Static.partition.Value

    let newEvents = newEvents Static.partition.Value

    [<Tests>]
    let Tests =
        testSequenced <| testof<Db.TypeOf>
            [
                testAsync "Test insert & query" {

                    // clean slate
                    do! clearTestDb |> Async.Ignore
                    
                    let expected = newEvents magicNumber intData |> List.sortBy (fun x -> x.StreamId)

                    // number of inserted rows = number of events
                    let! count = insertMany db expected
                    check |> equal count expected.Length

                    // deserialized queried events = original events
                    let! actual = queryStreams db (All SinceTheBeginning) Progress.ignore
                    check |> persistedEventsAllEqual actual expected
                }

                testAsync "Test clear" {

                    // clean slate
                    do! clearTestDb |> Async.Ignore

                    let events = newEvents 18 intData
                    
                    let! expected1 =
                        events |> List.take 7 |> insertMany db
                    let! expected2 =
                        events |> List.skip 7 |> insertMany db

                    // clear rows = inserted rows
                    let! count = clearTestDb
                    check |> equal count (expected1 + expected2)

                    // queried is empty
                    let! actual = queryStreams db (All SinceTheBeginning) Progress.ignore
                    check |> isEmpty actual
                }
            ]

