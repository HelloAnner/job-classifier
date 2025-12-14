.PHONY: stop clean build start out-build

# 默认端口（从 .env 读取）
PORT ?= $(shell grep '^PORT=' .env 2>/dev/null | cut -d= -f2 || echo 8080)

# 检测当前平台并选择合适的二进制文件
CURRENT_OS := $(shell uname -s)
CURRENT_ARCH := $(shell uname -m)

ifeq ($(CURRENT_OS),Darwin)
    ifeq ($(CURRENT_ARCH),arm64)
        SERVER_BINARY := server-darwin-arm64
    else
        SERVER_BINARY := server-darwin-amd64
    endif
else ifeq ($(CURRENT_OS),Linux)
    SERVER_BINARY := server-linux-amd64
else
    SERVER_BINARY := server-linux-amd64
endif

stop:
	@echo "Stopping server on port $(PORT)..."
	@lsof -ti:$(PORT) 2>/dev/null | xargs kill -9 2>/dev/null || true
	@echo "Server stopped"

build:
	@echo "Building for multiple architectures..."
	@mkdir -p out
	@cd server && \
	GOOS=linux GOARCH=amd64 go build -o ../out/server-linux-amd64 . && \
	GOOS=darwin GOARCH=arm64 go build -o ../out/server-darwin-arm64 . && \
	GOOS=darwin GOARCH=amd64 go build -o ../out/server-darwin-amd64 . && \
	GOOS=windows GOARCH=amd64 go build -o ../out/server-windows-amd64.exe .
	@echo ""
	@echo "Build complete! Binaries in out/:"
	@echo "  - server-linux-amd64"
	@echo "  - server-darwin-arm64"
	@echo "  - server-darwin-amd64"
	@echo "  - server-windows-amd64.exe"
	@echo ""
	@echo "Current platform: $(CURRENT_OS)/$(CURRENT_ARCH)"
	@echo "Server binary: $(SERVER_BINARY)"
	@echo ""
	@echo "To start server manually, run:"
	@echo "  cd out && ./$(SERVER_BINARY)"

start: build
	@echo "Starting server for $(CURRENT_OS)/$(CURRENT_ARCH) (port from .env)..."
	@cd out && ./$(SERVER_BINARY)
	@echo "Server started. Open http://localhost:$(PORT)"

out-build:
	@echo "Building production version (Linux AMD64)..."
	@mkdir -p out
	@cd server && GOOS=linux GOARCH=amd64 go build -o ../out/server .
	@echo "Production build complete. Binary: out/server"
	@echo "To start: cd out && ./server"

clean:
	@rm -rf out/*
	@echo "Cleaned build artifacts"
