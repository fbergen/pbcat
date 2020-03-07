# pbcat

Cat utility for length encoded binary proto files, prints the results in json lines format.


## Installation
```
go get github.com/fbergen/pbcat
```

## Basic Usage

```
$ ls protos/
> example.proto
ls data/
> data.pb 

$ $GOPATH/bin/pbcat -p protos data/data.pb
```

## ENV variables

Instead of specifying directory of proto definitions all the time, you can also use the `PBCAT_PROTO_ROOT` env variable

```
export PBCAT_PROTO_ROOT="/PATH/TO/PROTO/DIR"
```

## Specifying the message type

Specify the mesage type `--msg=MessageType`
Pbcat is trying to infer the type of the binary message by looking at the available types from the proto definitions, 
however sometimes it fails to correctly determine the type so you can instruct pbcat to interpret themessage as a particular proto

Match for specific properties `--match=FieldName='my value'`
Output only records matching the match expression

Only count the number of records `--count`
Outputs only the number of records in the file
