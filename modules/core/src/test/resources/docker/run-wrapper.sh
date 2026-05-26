#!/bin/sh

# Wrapper script for starting Ignite that doesn't die on SIGINT
IGNITE_SCRIPT="/opt/ignite/apache-ignite/run.sh"

# Function to stop Java process
stop_ignite() {
    echo "Received SIGINT/SIGTERM, stopping Ignite..."
    pkill -f "org.apache.ignite.startup.cmdline.CommandLineStartup"

    if [ -n "$IGNITE_PID" ]; then
        wait $IGNITE_PID 2>/dev/null
    fi
    echo "Ignite stopped."
}

# Catch SIGTERM and SIGINT, but don't exit — only stop the Java process
trap 'stop_ignite' TERM INT

# Start Ignite in the background
$IGNITE_SCRIPT &
IGNITE_PID=$!

# Wait for Java process to be alive
wait $IGNITE_PID

# If Java process died on its own, just wait for the next start
while true; do
    sleep 1
done
