#!/bin/bash

while read -r line; do
  x=$(echo "$line" | cut -d'-' -f1)
  y=$(echo "$line" | cut -d'-' -f2)
  echo "$x" > "$x"
  echo "$y" >> "$x"
done < cl