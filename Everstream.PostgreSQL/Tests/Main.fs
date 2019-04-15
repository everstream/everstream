open Expecto
open System.Reflection
open System.Diagnostics
open Everstream.PostgreSQL
open Everstream.Tests.Utils
open Everstream.PostgreSQL.Utils
open Everstream
open Everstream
open Npgsql.FSharp
open Everstream.PostgreSQL.Tests

// moduleof implementation
type TypeOf = interface end

[<EntryPoint>]
let main _ =

    Static.partition <- IdGenerator.newId Default |> sprintf "0%s" |> Some

    let result =
        try        

            Static.db <-

                let uri =
                    "https://raw.github.com/everstream/gists/master/everstream.uri"

                let cs =
                    (new System.Net.Http.HttpClient()).GetAsync(uri)
                    |> Async.AwaitTask
                    |> Async.RunSynchronously
                    |> fun x -> x.Content.ReadAsStringAsync()
                    |> Async.AwaitTask
                    |> Async.RunSynchronously
                    |> parseUri "SslMode=Require;TrustServerCertificate=true;"

                Db.awaitDb Static.partition.Value cs |> Async.RunSynchronously |> Some

            // Run all tests in both assemblies
            (getTests<Everstream.Stream>
            |> Seq.map (runTests defaultConfig)
            |> Seq.sum)

            +

            (getTests<TypeOf>
            |> Seq.map (runTests defaultConfig)
            |> Seq.sum)


        finally

            let q =
                (tableName Static.partition.Value).Id
                |> sprintf "DROP TABLE IF EXISTS %s CASCADE;"

            if Static.db.IsSome then
                Static.db.Value
                |> Sql.connect
                |> Sql.query q
                |> Sql.executeNonQuery
                |> ignore

    result