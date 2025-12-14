#!/bin/bash
cd /Users/anner/code/job-classifier/show/server
rm -f ../out/server-darwin-arm64
GOOS=darwin GOARCH=arm64 go build -o ../out/server-darwin-arm64 .
if [ $? -eq 0 ]; then
  echo "Build succeeded"
  cd /Users/anner/code/job-classifier/show
  /Users/anner/code/job-classifier/show/out/server-darwin-arm64 &
  PID=$!
  sleep 3
  curl -s http://127.0.0.1:38866/ | head -30
  kill $PID 2>/dev/null
else
  echo "Build failed"
fi
