namespace Everstream.Tests

open Expecto
open Expecto.Expect
open Everstream

module PersistedEventTests =

    let settings = Default.Serialization []
    
    type TestEventIntContent = TestEventIntContent of int
    type TestEventStringContent = TestEventStringContent of string
    type Event =
        | TestEvent1 of TestEventIntContent
        | TestEvent2 of TestEventStringContent

    let intDataMax max = TestEvent1(TestEventIntContent (pseudoRandom.Next(max))) :> EventData
    let intData () = intDataMax magicNumber
    let stringDataLength length = TestEvent2(TestEventStringContent (pseudoRandom.NextString(length))) :> EventData
    let stringData () = stringDataLength magicNumber

    let newEvents partition num (dataGenerator:unit -> EventData) =
        [1 .. num]
        |> List.map (fun i ->
            let id = sprintf "teststream%09d" i
            Events.newEvent settings partition (Stream.create (SpecificId id)) 0 None (dataGenerator ()) None)

    let newStream (dataGenerator:unit -> EventData) =
        let streamId = Stream.create (RandomId Default)
        [1 .. magicNumber]
        |> List.map (fun i ->
            Events.newEvent settings Default.Partition.Temporary streamId i None (dataGenerator ()) None)

    let newEvent dataGenerator = newEvents Default.Partition.Temporary 1 dataGenerator |> List.exactlyOne

    [<Tests>]
    let Tests =
        testof<PersistedEvent>
            [
                test "Create two events with the same id and test equality" {
                    let evt1 = newEvent intData
                    let evt2 = newEvent stringData

                    check |> equal evt1 {evt2 with Id = evt1.Id}
                }

                test "Create two distinct events and test that they are different" {
                    let evt1 = newEvent intData
                    let evt2 = newEvent intData

                    check |> notEqual evt1 evt2
                }

                test "Create several events with different offsets" {
                    let evts = newStream intData
                        
                    check |> setEqual
                        (evts |> List.map (fun x -> x.Version))
                        ([1 .. magicNumber] |> List.map (fun x -> x+1))
                }

                test "Test event data function" {
                    let evt = newEvent intData
                    let data1 = evt.Data
                    let data2 = Events.data [evt] |> List.exactlyOne

                    check |> equal data1 data2
                }
            ]