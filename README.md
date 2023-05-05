# Key-Value-that-Supports-Distributed-Transaction
A Key-Value store that based on Accord to Achieve distributed transaction

## Basic Design
Use gRPC to communicate
use Badger to persist data

## Client
### Messages
- Admin Messages
  - TODO
- Query Message
  - TODO
- Other Message
  - HeartBeat
### Design
* How to represent a transaction?
* How to represent Command?
* How to represent Result?

## Server
### Design
#### Basic Model
- Timestamp
#### Roles
- Shard
- Replica or Master
- Electorate
- Quorum

## Test Framework
### Test Cases Design
- Consider if original source should Leave some handler

