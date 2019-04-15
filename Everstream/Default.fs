namespace Everstream


module Default =

    module Partition =
        [<Literal>]
        let Temporary : Partition = "[TEMP]"

    let Serialization converters =
        {
            Settings = Serialization.newSettings converters
            Describe = Serialization.describe
        }