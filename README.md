# pbcat

Utility for deserializing length encoded binary proto files to JSON lines


## Installation
```
go get github.com/fbergen/pbcat
```

## Basic Usage

Tool is similar to `protoc --decode MyMessage my.proto < proto.bin` but it outputs JSON lines so you can consume the output in e.g `jq` later

```
$ pbcat -p ../proto_dir/ --msg=MyMessage FILE.bin | jq '.'
$ pbcat -p ../proto_dir/ --msg=MyMessage < FILE.bin | jq '.'
```

## ENV variables

Instead of specifying directory of proto definitions all the time, you can also use the `PBCAT_PROTO_ROOT` env variable

```
export PBCAT_PROTO_ROOT="/PATH/TO/PROTO/DIR"
```

## Command line flags

Specify the mesage type `--msg=MessageType`
Pbcat is trying to infer the type of the binary message by looking at the available types from the proto definitions, 
however sometimes it fails to correctly determine the type so you can instruct pbcat to interpret themessage as a particular proto

Match for specific properties `--match=FieldName='my value'`
Output only records matching the match expression

Only count the number of records `--count`
Outputs only the number of records in the file
