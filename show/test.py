#!/usr/bin/env python3
import subprocess
import time
import sys

# Build
print("Building...")
result = subprocess.run([
    "go", "build", "-o", "out/server-darwin-arm64", "."
], cwd="server", capture_output=True, text=True)

if result.returncode != 0:
    print("Build failed:")
    print(result.stderr)
    sys.exit(1)

print("Build succeeded")

# Start server
print("Starting server...")
proc = subprocess.Popen([
    "out/server-darwin-arm64"
], cwd="/Users/anner/code/job-classifier/show", stdout=subprocess.PIPE, stderr=subprocess.PIPE)

time.sleep(3)

# Test curl
print("Testing...")
curl_result = subprocess.run([
    "curl", "-s", "http://127.0.0.1:38866/"
], capture_output=True, text=True)

print(curl_result.stdout[:1000])

# Cleanup
proc.terminate()
proc.wait()
print("\nDone")
