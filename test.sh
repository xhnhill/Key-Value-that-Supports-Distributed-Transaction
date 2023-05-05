#!/bin/bash

# Start four processes in the background
process1_command="./Servx -path=config/cl -self=config/1"
process3_command="./Servx -path=config/cl -self=config/3"
process4_command="./Servx -path=config/cl -self=config/4"
process5_command="./Servx -path=config/cl -self=config/5"
process6_command="./Servx -path=config/cl -self=config/6"

$process1_command &
$process3_command &
$process4_command &
$process5_command &
$process6_command &



