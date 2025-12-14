# Makefile
# 功能：统一启动 combine 全量处理与查看日志
# 作者：Anner
# 创建时间：2025/12/12

# 若 make 命令带了额外目标（例如 `make run 51job`），把第一个额外目标当作平台名
EXTRA_GOALS := $(filter-out init start run log stop,$(MAKECMDGOALS))
ifneq ($(EXTRA_GOALS),)
PLATFORM := $(firstword $(EXTRA_GOALS))
endif

# 默认平台（可通过 `make run 51job` 或 `make run bo` 覆盖）
PLATFORM ?= 51job
# 平台名统一转小写，便于匹配大小写不同的数据目录
PLATFORM_LC := $(shell echo $(PLATFORM) | tr 'A-Z' 'a-z')

# 尝试在 data/ 下大小写不敏感地找到平台目录，找不到则回退原路径
PLATFORM_DIR ?= $(shell find data -maxdepth 1 -type d -iname "$(PLATFORM)" | head -n 1)
ifeq ($(strip $(PLATFORM_DIR)),)
PLATFORM_DIR := data/$(PLATFORM)
endif

# 输入文件匹配规则：data/<platform>/*.csv（平台目录大小写不敏感）
INPUT ?= $(PLATFORM_DIR)/*.csv

# 推荐默认参数（稳态优先，可通过 make run QUERY_WORKERS=80 EMB_BATCH=256 覆盖）
# 单文件处理：默认一次只处理 1 个 CSV，避免同时多文件导致系统卡顿
QUERY_WORKERS ?= 48
# 收敛批量，降低单次 /api/embed 压力，减少超时
EMB_BATCH ?= 15
EMB_TIMEOUT ?= 600s
# Chroma 仅取前 5 个候选，加快查询与反序列化
CHROMA_TOPK ?= 5
SQL_BATCH ?= 5000
# 默认 embedding 模型（512 维）。如切换模型，需重新 init（会清空 chroma-data 重建索引）

RUN_LOG ?= run_$(PLATFORM_LC).log
PID_FILE ?= .run.$(PLATFORM_LC).pid

# init 相关配置
OLLAMA_HOST ?= http://127.0.0.1:11434
CHROMA_HOST ?= http://localhost:8000
EMB_MODEL ?= qllama/bge-small-zh-v1.5:latest
CHROMA_IMAGE ?= chromadb/chroma
CHROMA_CONTAINER ?= job-classifier-chroma

.PHONY: init start run log stop

init:
	@bash -lc '\
	set -eo pipefail; \
	echo "== 检查 Ollama 服务 =="; \
	if ! command -v ollama >/dev/null 2>&1; then \
		echo "未找到 ollama 命令，请先安装 Ollama。"; exit 1; \
	fi; \
	if ! ollama list >/dev/null 2>&1; then \
		echo "Ollama 未启动或不可访问，请先启动 ollama serve (或 Ollama.app)。"; exit 1; \
	fi; \
	if ollama show "$(EMB_MODEL)" >/dev/null 2>&1; then \
		echo "模型 $(EMB_MODEL) 已存在。"; \
	else \
		echo "模型 $(EMB_MODEL) 未找到，开始拉取..."; \
		ollama pull "$(EMB_MODEL)"; \
	fi; \
	echo ""; \
	echo "== 检查 Chroma 服务（Docker） =="; \
	if ! command -v docker >/dev/null 2>&1; then \
		echo "未找到 docker 命令，请先安装/启动 Docker Desktop。"; exit 1; \
	fi; \
	echo "重建 Chroma 容器 $(CHROMA_CONTAINER)，使用性能优先参数..."; \
	old8000=$$(docker ps -aq --filter "publish=8000"); \
	if [ -n "$$old8000" ]; then \
		echo "检测到占用 8000 端口的容器：$$old8000，正在移除..."; \
		docker rm -f $$old8000 >/dev/null 2>&1 || true; \
	fi; \
	docker rm -f "$(CHROMA_CONTAINER)" >/dev/null 2>&1 || true; \
	rm -rf chroma-data; mkdir -p chroma-data; \
	docker run -d --name "$(CHROMA_CONTAINER)" \
		-p 8000:8000 \
		-v "$$(pwd)/chroma-data":/chroma/index \
		-e CHROMA_SERVER_HTTP_THREADS=8 \
		-e CHROMA_SERVER_WORKERS=8 \
		-e CHROMA_SERVER_CORS_ALLOW_ORIGINS="*" \
		"$(CHROMA_IMAGE)" >/dev/null; \
	echo "等待 Chroma 启动..."; \
	for i in $$(seq 1 30); do \
		status=$$(curl -s -o /dev/null -w "%{http_code}" "$(CHROMA_HOST)/api/v2/tenants/default_tenant/databases/default_database/collections" || true); \
		if [ "$$status" = "200" ]; then break; fi; \
		sleep 2; \
	done; \
	if [ "$$status" != "200" ]; then \
		echo "Chroma 仍不可访问，请手动检查：$(CHROMA_HOST)"; exit 1; \
	else \
		echo "Chroma 已就绪，线程=8, workers=8, 数据目录=./chroma-data"; \
	fi; \
	echo ""; \
	echo "== 初始化基础数据 =="; \
	if [ ! -f "classification.json" ] && [ ! -f "data/classification.json" ]; then \
		echo "未找到 classification.json（根目录或 data/ 下），请先放置分类基础文件。"; exit 1; \
	fi; \
	mkdir -p db; \
	echo "1) 构建 Chroma 向量基础库（cmd/jobimport）..."; \
	go run ./cmd/jobimport; \
	echo "2) 构建职业知识图谱（cmd/jobgraph）..."; \
	go run ./cmd/jobgraph --db=db/job_graph.db; \
	echo "init 完成。"; \
	'

run:
	@set -e; \
	if [ ! -d "data" ]; then \
		echo "data 目录不存在，请先准备 data/ 下的输入文件。"; \
		exit 1; \
	fi; \
	if [ ! -d "output" ]; then \
		echo "output 目录不存在，请先创建 output/ 目录。"; \
		exit 1; \
	fi; \
	input_files="$(wildcard $(INPUT))"; \
	if [ -z "$$input_files" ]; then \
		echo "未找到输入 CSV：$(INPUT)"; \
		echo "请确认 data/ 下存在平台目录（大小写不敏感），例如 data/BO/*.csv"; \
		exit 1; \
	fi; \
	files_count=$$(set -- $$input_files; echo $$#); \
	echo "Start combine (platform=$(PLATFORM), dir=$(PLATFORM_DIR), files=$$files_count)"; \
	echo "INPUT pattern: $(INPUT)"; \
	echo "Workers: query=$(QUERY_WORKERS) emb-batch=$(EMB_BATCH) chroma-topk=$(CHROMA_TOPK)"; \
	nohup go run ./combine \
		--input="$(INPUT)" \
		--query-workers=$(QUERY_WORKERS) \
		--emb-batch=$(EMB_BATCH) \
		--emb-timeout=$(EMB_TIMEOUT) \
		--chroma-topk=$(CHROMA_TOPK) \
		--batch=$(SQL_BATCH) \
		> $(RUN_LOG) 2>&1 & \
	echo $$! > $(PID_FILE); \
	echo "started platform=$(PLATFORM) pid=$$(cat $(PID_FILE)), log=$(RUN_LOG)"

# 兼容旧命令：make start 等价于 make run
start: run

log:
	@if [ ! -f "$(RUN_LOG)" ]; then \
		echo "log file not found: $(RUN_LOG)"; \
		exit 1; \
	fi
	@echo "== tail $(RUN_LOG) =="
	@tail -f "$(RUN_LOG)"

stop:
	@echo "Stop combine processes (platform=$(PLATFORM), input=$(INPUT))..."
	@pids=""; \
	if [ -f "$(PID_FILE)" ]; then \
		pid=$$(cat "$(PID_FILE)"); \
		if ps -p $$pid >/dev/null 2>&1; then \
			pids="$$pid"; \
		fi; \
	fi; \
	if [ -z "$$pids" ]; then \
		pids=$$(pgrep -f "combine .*--input=$(INPUT)"); \
	fi; \
	if [ -z "$$pids" ]; then \
		pids=$$(pgrep -f "combine .*$(INPUT)"); \
	fi; \
	if [ -n "$$pids" ]; then \
		for p in $$pids; do \
			echo "kill -9 $$p (and its children)"; \
			pkill -9 -P $$p 2>/dev/null || true; \
			kill -9 $$p 2>/dev/null || true; \
		done; \
	else \
		echo "no matching combine processes found"; \
	fi; \
	rm -f "$(PID_FILE)"

# 额外平台目标只用于设置 PLATFORM，本身不执行任何动作
.PHONY: $(EXTRA_GOALS)
$(EXTRA_GOALS):
	@:
