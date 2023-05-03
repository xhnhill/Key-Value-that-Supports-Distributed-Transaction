#!/bin/bash

# Start four processes in the background
process2_command="./Ser -path=config/cl -self=config/2"
process3_command="./Ser -path=config/cl -self=config/3"
process4_command="./Ser -path=config/cl -self=config/4"
process5_command="./Ser -path=config/cl -self=config/5"
process6_command="./Ser -path=config/cl -self=config/6"

$process2_command &
$process3_command &
$process4_command &
$process5_command &
$process6_command &




