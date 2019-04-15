namespace Everstream.PostgreSQL

open Npgsql.FSharp
open System.Dynamic
open Everstream
open Everstream.Query
open System.Data.Common
open Npgsql
open Everstream.PostgreSQL.Utils
open System.Threading

module Db =

    /// Moduleof implementation
    type TypeOf = interface end
        


    /// Clears the specified partition
    let internal clearPartition db streamId partition =
        let table = tableName partition
        let whereClause =
            match streamId with Some id -> sprintf "WHERE stream_id='%s'" id | None -> ""

        let connection =
            db
            |> Sql.connect
        
        let exists =
            connection
            |> Sql.query (sprintf "SELECT EXISTS (SELECT relname FROM pg_class WHERE relname = '%s');" table.Name)
            |> Sql.executeScalar
            |> Sql.toBool

        if exists then
            
            let q = sprintf "DELETE FROM %s %s" table.Id whereClause

            db
            |> Sql.connect
            |> Sql.query q
            |> Sql.executeNonQueryAsync

        else async { return 0 }

    let clearVolatilePartition db = Default.Partition.Temporary |> clearPartition db None



    /// Creates partition schema if missing and returns connection string
    let awaitDb partition (cs:ConnectionString) =

        let connection =
            Sql.host cs.Host
            |> Sql.port cs.Port
            |> Sql.username cs.Username
            |> Sql.password cs.Password
            |> Sql.database cs.Database
            |> Sql.config cs.Parameters
            |> Sql.str

        async {

            let makeCreateTableQuery (partition:Partition) =
                let table = tableName partition                
                let quote = sprintf (if table.Id.EndsWith("\"") then "\"%s_%s\"" else "%s_%s") table.Name
                let pkey = quote "pkey"
                let ukey = quote "partition_streamid_version_ukey"
                let query = sprintf @"
-- Requires GRANT CREATE ON DATABASE <events db> TO <application user>
CREATE SCHEMA IF NOT EXISTS %s;

CREATE TABLE IF NOT EXISTS %s.events
(
    id uuid NOT NULL,
    partition text NOT NULL,
    stream_id text NOT NULL,
    version int NOT NULL,
    type text NOT NULL,
    data jsonb NOT NULL,
    meta jsonb NOT NULL,
    timestamp bigint DEFAULT EXTRACT (EPOCH FROM (now() at time zone 'utc')) * 1000
    -- parent table constraints not supported before v11
    --CONSTRAINT pk_event_id PRIMARY KEY (partition, id),
    --CONSTRAINT uk_event_streamid_version UNIQUE (partition, stream_id, version)
    --PRIMARY KEY (partition, id)
) PARTITION BY LIST (partition);

CREATE TABLE IF NOT EXISTS %s PARTITION OF %s.events FOR VALUES IN ('%s');
ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;
ALTER TABLE %s ADD PRIMARY KEY (partition, id);
ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;
ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (partition, stream_id, version);

CREATE INDEX IF NOT EXISTS idx_stream_id_%s ON %s (partition, stream_id, version);
CREATE INDEX IF NOT EXISTS idx_timestamp_%s ON %s (partition, timestamp, version);
CREATE INDEX IF NOT EXISTS idx_type_%s ON %s (partition, type, version);" table.Schema table.Schema table.Id table.Schema partition table.Id pkey table.Id table.Id ukey table.Id ukey table.Name table.Id table.Name table.Id table.Name table.Id
                query

            let createTable partition =
                connection
                |> Sql.connect
                |> Sql.query (makeCreateTableQuery partition)
                |> Sql.executeNonQueryAsync
                |> Async.Ignore

            do! createTable partition

            // create temp partition if not testing        
            if Schema.Test <> (tableName partition).Schema then
                do! createTable Default.Partition.Temporary

            do! clearVolatilePartition connection |> Async.Ignore

            return connection
        }



    /// Inserts multiple serialized events
    let internal insertMany persistEvents serialization partition db events =
        let values = events |> List.map (pgSerialize serialization) |> String.concat ",\n"
        let isConcurrencyException (ae:System.AggregateException) =
            ae.InnerException :? Npgsql.PostgresException &&
            (ae.InnerException :?> Npgsql.PostgresException).ConstraintName
            |> String.endsWith "stream_id_version_key"

        let table =
            if persistEvents then tableName partition
            else tableName Default.Partition.Temporary
        
        async {

            let query = (table.Id, values) ||>  sprintf @"
INSERT INTO %s (id,partition,stream_id,version,type,data,meta)
VALUES %s"

            try
                return!
                    db
                    |> Sql.connect
                    |> Sql.query query         
                    |> Sql.executeNonQueryAsync
                
            with
            | :? System.AggregateException as ae when isConcurrencyException ae ->
                    let x = events.Head
                    return
                        if x.Version = 1 then raise (StreamIdAlreadyExistsException x.StreamId)
                        else raise (ExpectedVersionDidNotMatchActualVersionException (x.StreamId, x.Version + -1, x.Version))
        }

        
    

    /// Query streams using the given event query definition
    let internal queryStreams persistEvents (serialization:Serialization) db partition (query:EventQuery) (p:IProgress) =
        let whereClause partition =
            match query with 

                | Stream(Full id) ->
                    sprintf "WHERE partition = '%s' AND stream_id = '%s'" partition id

                | Stream(SinceVersion stream) ->
                    sprintf "WHERE partition = '%s' AND stream_id = '%s' AND version > %i" partition stream.Id stream.Version

                | Type t -> sprintf "WHERE partition = '%s' AND type = '%s'" partition t.FullName

                | All(SinceTimestamp ts) -> sprintf "WHERE partition = '%s' AND timestamp >= %i"partition ts
                | All(SinceTheBeginning) -> ""

        let q table partition = (table, whereClause partition) ||> sprintf @"SELECT * FROM %s %s"

        let query =
            if persistEvents then q (tableName partition).Id partition
            else  q (tableName partition).Id partition + "\nUNION\n" + q (tableName Default.Partition.Temporary).Id Default.Partition.Temporary
        
        async {

            let! count =
                db
                |> Sql.connect
                |> Sql.query (sprintf "SELECT COUNT(*) FROM (%s) x" query)
                |> Sql.executeScalarAsync
                |> Async.map Sql.toLong
                |> Async.map float

            use connection = new NpgsqlConnection(db)
            connection.Open()
            
            // insert data example
            //use cmd = new NpgsqlCommand()
            //cmd.CommandText <- (sprintf "%s ORDER BY stream_id DESC, version DESC" query)
            //cmd.ExecuteNonQuery() |> ignore

            use cmd = new NpgsqlCommand(sprintf "%s ORDER BY version" query, connection)
            let! ct = Async.CancellationToken
            use! reader = cmd.ExecuteReaderAsync(ct) |> Async.AwaitTask

            return readRows p reader count serialization
        }
