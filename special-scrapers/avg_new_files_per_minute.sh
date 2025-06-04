#!/bin/bash
DIR="data/it" # <-- set this to your target folder

# Get current time and 1 hour ago, in seconds since epoch
now=$(date +%s)
start=$((now - 300))

# List all files, get their mtime in seconds since epoch
find "$DIR" -type f -exec stat -f "%m" {} \; | \
awk -v start="$start" -v now="$now" '
  $1 >= start && $1 <= now { 
    minute = int(($1 - start) / 60); 
    count[minute]++; 
  } 
  END { 
    for (i in count) { total += count[i]; mins++ } 
    if (mins > 0) print "Average files per minute (last 5 min):", total/mins; 
    else print 0 
  }
'
