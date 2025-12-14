#!/bin/bash
cd /Users/anner/code/job-classifier/show
echo "Starting server..."
/Users/anner/code/job-classifier/show/out/server-darwin-arm64 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 3
echo "Testing curl..."
curl -s http://127.0.0.1:38866/ | head -20
echo ""
echo "---"
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true
echo "Done"
