#!/bin/bash

# Start four processes in the background
process1_command="./concrr -addr=localhost:50011 -ser=localhost:50031"
process2_command="./concrr -addr=localhost:50012 -ser=localhost:50032"
process3_command="./concrr -addr=localhost:50013 -ser=localhost:50033"
process4_command="./concrr -addr=localhost:50014 -ser=localhost:50034"
process5_command="./concrr -addr=localhost:50015 -ser=localhost:50035"
process6_command="./concrr -addr=localhost:50016 -ser=localhost:50036"

$process1_command &
$process2_command &
$process3_command &
$process4_command &
$process5_command &
$process6_command &