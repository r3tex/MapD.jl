module MapD
using DataFrames

export DataFrames, mapdwrite, MapDcon
const MAPDVER = v"3.4-"
const MAPDOK = 0x0000


const mapdql = homedir()*"/.julia/v0.6/MapD/tools/mapdql"
const streaminsert = homedir()*"/.julia/v0.6/MapD/tools/StreamInsert"

mutable struct MapDcon
    host::String
    port::Int16
    user::String
    pass::String
    db::String
    MapDcon() = new("localhost", 9091, "mapd", "HyperInteractive", "mapd")
    MapDcon(host, port, user, pass, db) = new(host, port, user, pass, db)
    MapDcon(host) = new(host, 9091, "mapd", "HyperInteractive", "mapd")
    MapDcon(host, port) = new(host, port, "mapd", "HyperInteractive", "mapd")
end

const MAPDTYPES = Dict(
            Int64 => "BIGINT", 
            Int32 => "INTEGER", 
            Int16 => "SMALLINT",
            Bool => "BOOLEAN",
            Float32 => "FLOAT",
            Float64 => "DOUBLE",
            String => "TEXT",
            DateTime => "TIMESTAMP"
            )

function mapdversion(con::MapDcon)
    try
        cmd = `echo "\version"`, `$mapdql $(con.db) -u $(con.user) -p $(con.pass) --port $(con.port) -s $(con.host)`
        ver = parse.(split(split(readstring(pipeline(cmd[1], cmd[2])), r"\n")[2][22:end], r"\.|-")) 
        version = VersionNumber(ver[1], ver[2], ver[3], (ver[4], string(ver[5])))
        MAPDVER <= version && return 0x0000
        println("Error: unsupported version of MapD"); return 0x0002
    catch err
        println(err)
        println("Error: could not connect to MapD"); return 0x0001
    end
end

function mapdfindtable(con::MapDcon, table::String)
    try
        cmd = `echo "\t"`, `$mapdql $(con.db) -u $(con.user) -p $(con.pass) --port $(con.port) -s $(con.host)`
        tables = split(readstring(pipeline(cmd[1], cmd[2])), r"\n")
        in(table, tables) ? (return MAPDOK) : (return 0x0003)
    catch err
        println(err)
        println("Error: could not find table: $table"); return 0x0003
    end
end

function mapdtruncate(con::MapDcon, table::String)
    try 
        cmd = `echo "truncate table $table;"`, `$mapdql $(con.db) -u $(con.user) -p $(con.pass) --port $(con.port) -s $(con.host)`
        run(pipeline(cmd[1], cmd[2]))
    catch err
        println(err)
        println("Error: could not truncate table: $table")
        return 0x0011
    end
    return MAPDOK
end

function mapdcreate(con::MapDcon, df::DataFrame, table::String)
    props = zip(string.(names(df)), eltypes(df))
    create = ""
    
    try
        create = ["CREATE TABLE $table ("]
        for prop in props
            push!(create, "$(prop[1]) $(MAPDTYPES[prop[2]]),")
        end
        create[end] = replace(create[end], ',', ')')
        create = string(create...) * ";"
    catch err
        println(err)
        println("Error: could not generate valid CREATE statement")
        return 0x0006
    end
    
    try
        cmd = `echo "$create"`, `$mapdql $(con.db) -u $(con.user) -p $(con.pass) --port $(con.port) -s $(con.host)`
        run(pipeline(cmd[1], cmd[2]))
    catch err
        println(err)
        println("Error: could not create table: $table")
        return 0x0007
    end
    return MAPDOK
end

function mapdchecktypes(con, df, table)
    try
        cmd = `echo "\d $table"`, `$mapdql $(con.db) -u $(con.user) -p $(con.pass) --port $(con.port) -s $(con.host)`
        tabledescription = split(readstring(pipeline(cmd[1], cmd[2])), r"\n")[3:end-2]
        
        dftyps = [MAPDTYPES[el] for el in eltypes(df)]
        tbtyps = [split(col[1:end-1], r" ")[2] for col in tabledescription]

        if dftyps == tbtyps 
            return MAPDOK
        else
            println("Error: type mismatch in DataFrame and table $table")
            println("DataFrame: ", dftyps)
            println("Table: ", tbtyps)
            return 0x0014
        end
    catch err
        println(err)
        println("Error: could not check $table types"); return 0x0013
    end
end

function mapdstreaminsert(con::MapDcon, df::DataFrame, table::String, delim::Char)
    try
        cmd = `$streaminsert --table $table --host $(con.host) -u $(con.user) -p $(con.pass) --port $(con.port) --database $(con.db) --delim $delim`
        height, width = size(df)
        open(cmd, "w", STDOUT) do io
            for row in eachrow(df)
                data = string(["$(row[el])," for el in 1:width]...)[1:end-1]
                println(io, data)
            end
        end
    catch err
        println(err)
        println("Error: could not StreamInsert to table $table")
        return 0x0010
    end
    return MAPDOK
end

function mapdwrite(con::MapDcon, df::DataFrame, table::String; truncate=false, delim=',')
    err = mapdversion(con)
    err != MAPDOK && return err

    err = mapdfindtable(con, table)
    if err == MAPDOK
        if truncate 
            err = mapdtruncate(con, table) 
            err != MAPDOK && return err
        end
    else
        err = mapdcreate(con, df, table)
        err != MAPDOK && return err
    end

    err = mapdchecktypes(con, df, table)
    err != MAPDOK && return err

    err = mapdstreaminsert(con, df, table, delim)
    return err
end

end
