namespace Everstream.PostgreSQL

open Everstream
open Everstream.Serialization
open Npgsql.FSharp
open System.Dynamic
open System.Threading
open System.Data.Common

module Schema =
    [<Literal>]
    let Public = "public"
    [<Literal>]
    let Test = "test"

type Table =
    {
        Schema: string
        Name: string
    }
    with
    member x.Id =
        match x.Name.Chars (0) with
        | '0' -> sprintf "\"%s\"" x.Name
        | _ -> x.Name
        |> sprintf "%s.%s" x.Schema

type ConnectionString =
    {
        Host:string
        Port:int
        Username:string
        Password:string
        Database:string
        Parameters:string
    }

module internal Utils =

    let private regex = System.Text.RegularExpressions.Regex("[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]")  
    let private nonWordRegex = System.Text.RegularExpressions.Regex("\W")

    let pgSerialize (serialization:Serialization) (event:PersistedEvent) =
        let escape (s:string) = s.Replace("'","''") |> sprintf "'%s'"

        event
        |> (fun e ->
            [
                e.Id            |> string |> escape
                e.Partition     |> escape
                e.StreamId      |> escape
                e.Version       |> string
                e.Type          |> escape
                e.Data          |> serialization.serialize |> escape
                e.Meta          |> serialization.serialize |> escape
            ])
        |> String.concat "," 
        |> sprintf "(%s)"

    let tableName (partition:Partition) =

        if not (regex.IsMatch partition) then
            invalidArg "partition" "Must start with a letter and only \
                                    contain letters, digits, or (but not end with) a dash."

        let pub = Schema.Public

        partition.Trim([|'[';']'|]).Replace('-','_').ToLowerInvariant()
        |> function
        | name when name.StartsWith('0') -> { Schema = Schema.Test; Name = name }
        | name when partition <> Default.Partition.Temporary -> { Schema = pub; Name = name }
        | name -> { Schema = pub; Name = sprintf "_%s" name }



    /// Parses a (possibly base64 encoded) postgresql:// uri into a connection string
    let internal parseUri (parameters:string) (uri:string) =

        let decode s =
            System.Convert.FromBase64String(s)
            |> System.Text.Encoding.UTF8.GetString

        let uri =
            if uri.StartsWith("postgres") then uri else decode uri
            |> System.Uri

        let username, password =
            uri.UserInfo.Split(':') |> fun x -> x.[0], x.[1]

        {
            Host = uri.Host
            Port = uri.Port
            Username = username
            Password = password
            Database = uri.PathAndQuery.TrimStart('/')
            Parameters = parameters
        }


    let readRow (reader:DbDataReader) (serialization:Serialization) =

        if reader.FieldCount < 8 then
            invalidOp "This row does not contain the appropriate number of fields."
    
        let typeName = reader.GetString(4)
                
        let eventType =
            Everstream.Serialization.eventType typeName
    
        let data =
            reader.GetString(5)
            |> serialization.deserialize eventType
    
        let meta =
            reader.GetString(6)
            |> serialization.deserialize typeof<ExpandoObject> :?> ExpandoObject
    
        {
            Id = reader.GetGuid(0)
            Partition = reader.GetString(1)
            StreamId = reader.GetString(2)
            Version = reader.GetInt32(3)
            Type = typeName
            Data = data
            Meta =  meta
            Timestamp = reader.GetInt64(7)
        }
    
    let readRows (p:IProgress) (reader:DbDataReader) total serialization =
        let mutable i = 0
        [
            while reader.Read() do
                i <- i + 1
                p.Report((float i) / total)
                        
                yield readRow reader serialization
        ]



module String =

    let endsWith (sub:string) s = not (System.String.IsNullOrWhiteSpace s) && s.EndsWith sub