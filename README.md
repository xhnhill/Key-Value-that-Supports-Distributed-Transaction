# Key-Value-that-Supports-Distributed-Transaction
A Key-Value store that based on Accord to Achieve distributed transaction

## Basic Design
Use gRPC to communicate
use Badger to persist data

The config file of cluster and individual node is in config directory.
This could be modified according to use case.

cmd dir contains the main code.
Replica dir contains the code for node and statemachine.
use go build -o Servx cmd/Replica/*.go to build, the output Servx is the server binary
the command should be run in root directory.
Please use go >= 1.20 version make sure the requirements in go.mod is satisfied

BDClient dir contains the testing clients, could be built by:
go build -o dbClient cmd/DBClient/*.go
Pay attention, the Accord paper assumes some constraints on time.
Unsuitable load on server will break the constrains.

UIDBClient dir contains a simple interface, which allows you to type in the
transactions manually
Please refer GUI.md to build or use our prepared version, which may depend on running env

proto dir contains the protocol messages that we used in the statemachine,
which could be compiled by protoc. The compiled version is in the Primitive dir



