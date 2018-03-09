include("../src/MapD.jl")
host = "192.168.1.115"
port = 19091

using MapD
using DataFrames

function gendata(rows)
    data = DataFrame([Int64, Int32, Int16, Float64, Float32, String, DateTime, Bool], [:one, :two, :three, :four, :five, :six, :seven, :eight], 0)
    for i in 1:rows
        push!(data, [rand(Int64), rand(Int32), rand(Int16), rand(Float64), rand(Float32), "derp", now(), rand(Bool)])
    end
    return data
end

data = gendata(10)
con = MapDcon(host, port)
err = mapdwrite(con, data, "testtable2", truncate=true)
