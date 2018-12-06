# MapD
This Julia package is an unofficial interface to MapD/OmniSci and makes use of the `mapdql` and `StreamImporter` tools. It was developed to let people conveniently send DataFrames into MapD for visualization.

I have only bundled the Linux binaries so if you wish to use this wrapper on Windows or OSX, please use their native tools instead.
You shouldn't be downloading binaries from untrusted git repos anyway.

Currently the following data-types are supported:

|Julia Type|MapD type|
| :--- | ---: |
|String | TEXT|
|Int16 | SMALLINT |
|Int32 | INTEGER|
|Int64 | BIGINT|
|Bool | BOOLEAN|
|Float32 | FLOAT|
|Float64 | DOUBLE|
|DateTime | TIMESTAMP|

# Example use

`mapdwrite()` will return a `UInt16` error code which you can check. 
If the returned value is `0` then you're in good shape.
You can also truncate the table before inserting new data.
If the table doesn't exist, it will be created.
If the types do not match, you will get a warning.
```
julia> using MapD, DataFrames

julia> con = MapDcon()
MapD.MapDcon("localhost", 9091, "mapd", "HyperInteractive", "mapd")

julia> err = mapdwrite(con, dataframe, "specialtable", truncate=true)
```
