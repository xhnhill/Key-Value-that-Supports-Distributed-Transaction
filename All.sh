#!/bin/bash

# Start four processes in the background
process1_command="./test -path=config/cl -self=config/1"
process2_command="./test -path=config/cl -self=config/2"
process3_command="./test -path=config/cl -self=config/3"
process4_command="./test -path=config/cl -self=config/4"
process5_command="./test -path=config/cl -self=config/5"
process6_command="./test -path=config/cl -self=config/6"

$process1_command &
$process2_command &
$process3_command &
$process4_command &
$process5_command &
$process6_command &


