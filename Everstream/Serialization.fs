namespace Everstream

open System.Reflection
open Microsoft.FSharpLu.Json
open System
open Newtonsoft.Json
open Newtonsoft.Json.Serialization

module internal Serialization =

    let private typeCache = System.Collections.Concurrent.ConcurrentDictionary<string,System.Type>()

    let newSettings converters =
        let x = Compact.Strict.CompactStrictSettings.settings
        x.Converters <-
            System.Collections.Generic.List<JsonConverter>(x.Converters |> Seq.append converters)
        x.ContractResolver <- CamelCasePropertyNamesContractResolver(); x

    type DisplayName = System.ComponentModel.DisplayNameAttribute

    /// Replaces 'aSringLikeThis' with 'a String Like This'
    let private unpascalize s =
        System.Text.RegularExpressions.Regex.Replace(s, "([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))", "$1 ")

    /// To sentence case (uppercase first char)
    let private sentence (s:string) =
        sprintf "%c%s" (Char.ToUpper s.[0]) (s.Substring(1))

    /// Make a sexy event description
    let describe (data:Object) =
        let dataType = data.GetType().GetTypeInfo()
        let moduleType = dataType.DeclaringType.DeclaringType            
        
        moduleType.GetCustomAttribute(typeof<DisplayName>) :?> DisplayName
        |> Option.ofObj
        |> function
            | None -> (unpascalize moduleType.Name |> sentence)
            | Some x -> x.DisplayName
        |> sprintf "%s %s" <| (unpascalize dataType.Name).ToLowerInvariant()   
        
    let rec loadReferencedAssemblies (assemblyName:AssemblyName) =
        "Could not find type in loaded assemblies, loading additional referenced assemblies."
        |>  System.Diagnostics.Trace.TraceInformation
        Assembly.Load(assemblyName).GetReferencedAssemblies()
        |> Array.iter loadReferencedAssemblies

    let eventType name =
        let error () = sprintf "Type '%s' not found, make sure domain assemblies are loaded prior to deserialization attempt." name |> failwith
        typeCache.GetOrAdd(name,
            fun _ -> 
                AppDomain.CurrentDomain.GetAssemblies()
                |> Array.where (fun x -> not x.IsDynamic)
                |> Array.collect (fun x -> x.GetTypes())
                |> Array.tryFind (fun x -> x.FullName.Equals(name))
                |> Option.defaultWith error)


type Serialization =
    {
        Settings: JsonSerializerSettings
        Describe: EventData -> string
    }
    with

        member x.serialize o =
            JsonConvert.SerializeObject(o, x.Settings)

        member x.deserialize t o =
            JsonConvert.DeserializeObject(o, t, x.Settings)