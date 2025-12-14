#!/bin/bash
cd /Users/anner/code/job-classifier/show
rm -rf out
cd server
go build -o ../out/server-darwin-arm64 .
echo "Build complete"
ls -lh ../out/
