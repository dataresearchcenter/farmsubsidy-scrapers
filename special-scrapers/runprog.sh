#!/bin/bash

# Script to start a Node.js program and restart it every hour, 
# with a forced exit ("exit harder") if the process does not terminate gracefully.

NODE_PROGRAM="it.js"
RESTART_INTERVAL=3600  # seconds (1 hour)
GRACE_PERIOD=10        # seconds to wait for a graceful shutdown

while true; do
    echo "Starting $NODE_PROGRAM at $(date)"
    /Users/user/.local/share/mise/installs/node/20.18.1/bin/node "$NODE_PROGRAM" &
    NODE_PID=$!

    SECONDS=0
    while kill -0 $NODE_PID 2>/dev/null; do
        if [ $SECONDS -ge $RESTART_INTERVAL ]; then
            echo "Restart interval reached. Sending SIGTERM to $NODE_PROGRAM (PID: $NODE_PID)"
            kill $NODE_PID
            WAITED=0
            # Wait for GRACE_PERIOD seconds for the process to exit gracefully
            while kill -0 $NODE_PID 2>/dev/null && [ $WAITED -lt $GRACE_PERIOD ]; do
                sleep 1
                WAITED=$((WAITED+1))
            done
            # If still running, force kill
            if kill -0 $NODE_PID 2>/dev/null; then
                echo "Process did not exit after $GRACE_PERIOD seconds. Sending SIGKILL to $NODE_PID"
                kill -9 $NODE_PID
            fi
            wait $NODE_PID 2>/dev/null
            break
        fi
        sleep 5
    done

    echo "$NODE_PROGRAM stopped at $(date). Restarting..."
done
