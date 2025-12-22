# 岗位智能分类系统

基于向量数据库的职业分类系统，支持批量处理招聘平台的岗位数据，自动进行职业分类。

## 📁 项目结构

```
job-classifier/
├── cmd/                          # 核心命令模块
│   ├── jobimport/               # 职业基础数据导入（构建向量基础库）
│   ├── jobgraph/                # 职业知识图谱构建
│   ├── classifier/              # 分类器模块
│   ├── query/                   # 查询处理模块
│   ├── count/                   # 计数统计模块
│   └── importer/                # 数据导入模块
├── combine/                     # 主程序入口（批量处理流水线）
├── data/                        # 📥 输入数据目录（需要用户准备）
│   ├── 51job/                   # 51job 平台数据
│   ├── bo/                      # BO 平台数据
│   └── 58tong/                  # 58同城市数据
├── output/                      # 📤 输出结果目录（自动生成）
│   ├── processed/               # 已处理完成的文件
│   ├── failed/                  # 处理失败的文件
│   ├── 51job_*.csv              # 按平台分组的输出文件
│   └── stats/                   # 统计信息
├── db/                          # 数据库目录
│   └── job_graph.db             # 职业知识图谱数据库
├── chroma-data/                 # Chroma 向量数据库数据
├── show/                        # Web 展示界面（进度可视化看板）
│   ├── web/                     # 前端页面（HTML/CSS/JS）
│   │   ├── index.html           # 主页面
│   │   ├── css/style.css        # 样式文件
│   │   ├── js/                  # JavaScript 脚本
│   │   ├── redis/               # Redis 连接模块
│   │   └── embed/               # 嵌入组件
│   ├── server/                  # 后端 API 服务
│   │   ├── main.go              # 服务入口
│   │   ├── handler/             # HTTP 处理器
│   │   └── redis/               # Redis 客户端
│   ├── out/                     # 编译产物目录
│   ├── .env                     # 配置文件
│   ├── Makefile                 # 构建脚本
│   └── build.sh                 # 快速构建脚本
├── stats/                       # 进度统计服务
│   ├── config/                  # 配置文件
│   ├── redis/                   # Redis 集成
│   ├── model/                   # 数据模型
│   └── scanner/                 # 扫描器
├── doc/                         # 项目文档
├── classification.json          # 职业分类基础数据（需要准备）
└── Makefile                     # 构建与运行脚本
```

## 🚀 快速开始

### 环境依赖

1. **Ollama** - 本地 LLM 服务
   - 安装：https://ollama.com/download
   - 启动：`ollama serve` 或启动 Ollama App
   - 模型：系统会自动拉取 `qllama/bge-small-zh-v1.5:latest`

2. **Docker** - 用于运行 Chroma 向量数据库
   - 安装并启动 Docker Desktop
   - 确保 8000 端口未被占用

3. **Go** - 开发环境
   - 版本：Go 1.21+
   - 安装依赖：`go mod tidy`

### 📋 从 0 开始完整流程

#### 1. 准备基础数据

```bash
# 放置职业分类文件到项目根目录
cp /path/to/classification.json ./

# 或放置到 data 目录
cp /path/to/classification.json ./data/
```

> classification.json 格式参考项目根目录示例文件，包含职业分类、关键词、典型岗位等信息。

#### 2. 准备输入数据

在 `data/` 目录下创建平台子目录，并放置 CSV 文件：

```
data/
├── 51job/
│   ├── job_data_001.csv
│   └── job_data_002.csv
├── bo/
│   └── bo_jobs.csv
└── 58tong/
    └── 58_jobs.csv
```

**CSV 文件格式要求：**
- 必须包含以下列（列名大小写不敏感）：
  - `职位名称` 或 `title`
  - `职位描述` 或 `description`
  - `公司名称` 或 `company`（可选）

示例：
```csv
职位名称,职位描述,公司名称
高级Java工程师,"负责后端系统开发，熟悉Spring Boot...",某科技公司
前端开发工程师,"负责前端页面开发，熟练使用Vue.js...",某互联网公司
```

#### 3. 初始化环境（只需执行一次）

```bash
make init
```

**执行内容：**
- ✅ 检查并拉取 Ollama 模型（qllama/bge-small-zh-v1.5:latest）
- ✅ 启动 Chroma 向量数据库（Docker 容器）
- ✅ 构建 Chroma 向量基础库
- ✅ 构建职业知识图谱数据库

#### 4. 创建输出目录

```bash
mkdir -p output/processed output/failed output/stats
```

#### 5. 开始处理

```bash
# 处理指定平台（默认 51job）
make run

# 处理 BO 平台
make run bo

# 处理 58同城市
make run 58tong

# 自定义参数
make run QUERY_WORKERS=64 EMB_BATCH=32
```

**参数说明：**
- `QUERY_WORKERS`: 查询并发数（默认 48）
- `EMB_BATCH`: Embedding 批处理大小（默认 15）
- `CHROMA_TOPK`: 向量检索候选数量（默认 5）
- `SQL_BATCH`: 数据库写入批大小（默认 5000）

#### 6. 监控进度

```bash
# 查看实时日志
make log

# 停止处理
make stop
```

## 📊 输出文件结构

处理完成后，`output/` 目录结构如下：

```
output/
├── processed/          # 已处理完成的源文件
│   └── 51job/
│       ├── job_data_001.csv
│       └── job_data_002.csv
├── failed/             # 处理失败的源文件
│   └── bo/
│       └── failed_file.csv
├── 51job_result.csv    # 51job 平台输出文件
├── bo_result.csv       # BO 平台输出文件
└── stats/              # 统计信息
    ├── progress.json   # 处理进度
    └── summary.json    # 汇总统计
```

**输出 CSV 格式：**
```csv
source_file,job_title,company,predicted_category,confidence,matched_keywords,llm_summary
job_data_001.csv,高级Java工程师,某科技公司,2-04-05-00 软件开发技术人员,0.95,"Java;Spring Boot;微服务","负责后端系统架构设计..."
```

**字段说明：**
- `source_file`: 源文件名
- `job_title`: 职位名称
- `company`: 公司名称
- `predicted_category`: 预测的职业分类编码
- `confidence`: 置信度（0-1）
- `matched_keywords`: 匹配的关键词（分号分隔）
- `llm_summary`: LLM 生成的能力要求总结

## 🛠 Makefile 命令详解

| 命令 | 说明 | 示例 |
|------|------|------|
| `make init` | 初始化环境（安装模型、启动服务、构建基础库） | `make init` |
| `make run` | 开始批量处理 | `make run 51job` |
| `make start` | 同 `make run`（别名） | `make start bo` |
| `make log` | 查看实时日志 | `make log` |
| `make stop` | 停止处理进程 | `make stop` |

### 环境变量

可通过环境变量自定义配置：

```bash
# 指定平台
export PLATFORM=bo
make run

# 自定义参数
export QUERY_WORKERS=80
export EMB_BATCH=256
export CHROMA_TOPK=10
make run
```

## 📈 进度统计

系统支持实时进度上报（需要配置 Redis）：

```bash
# 在 stats/ 目录下启动统计服务
cd stats && ./dist/server
```

访问 `http://localhost:8080` 查看可视化进度界面。

### 📊 Web 展示界面（show）

`show/` 目录提供独立的 Web 展示界面，用于实时可视化任务进度统计。

**主要功能：**
- 🎯 多平台进度监控（支持同时查看多个平台的数据）
- 📈 实时进度条和统计图表
- 📋 详细的任务列表和状态展示
- 🔄 自动刷新（可配置间隔）

**启动方式：**

```bash
cd show

# 方式一：使用 Makefile 自动构建并启动
make start

# 方式二：使用构建脚本
./build.sh && cd out && ./server-darwin-arm64

# 方式三：手动构建
cd server
go build -o ../out/server .
cd ../out && ./server
```

**访问界面：**
- 默认地址：http://localhost:38866
- 端口可在 `show/.env` 中配置 `PORT` 参数

**界面说明：**
- 左侧边栏：显示所有配置的数据源（Redis key）
- 主内容区：显示选中数据源的详细进度信息
- 实时更新：根据 `UPDATE_INTERVAL` 配置自动刷新

### 📝 配置文件说明

系统提供两个主要的配置文件：

#### 1. stats/.env - 进度统计服务配置

```bash
# 原始数据目录（需要监控的输入数据）
SOURCE_DIR=/Users/anner/code/job-classifier/data/BO

# 输出目录
OUTPUT_DIR=/Users/anner/code/job-classifier/output

# Redis 配置
REDIS_HOST=118.196.16.11          # Redis 服务器地址
REDIS_PORT=36688                  # Redis 端口
REDIS_PASSWORD=Anner_login_123    # Redis 密码
REDIS_KEY=bo                      # Redis 键名（用于存储进度数据）

# 扫描间隔(秒)
SCAN_INTERVAL=3                   # 扫描文件变化的间隔时间
```

**配置说明：**
- `SOURCE_DIR`: 指向需要监控进度的输入数据目录
- `OUTPUT_DIR`: 指向输出结果目录
- `REDIS_KEY`: 建议不同平台使用不同的 key（如 `bo`, `51job`, `58tong`）进行隔离

#### 2. show/.env - Web 展示界面配置

```bash
# Redis 配置
REDIS_HOST=118.196.16.11
REDIS_PORT=36688
REDIS_PASSWORD=Anner_login_123

# 需要展示的 Redis key 列表 (逗号分隔)
REDIS_KEYS=bo,51job

# 服务端口
PORT=38866

# 更新间隔(秒)
UPDATE_INTERVAL=3
```

**配置说明：**
- `REDIS_KEYS`: 指定需要在 Web 界面展示的 Redis key，多个 key 用逗号分隔
- `PORT`: Web 服务的监听端口
- `UPDATE_INTERVAL`: 前端页面自动刷新间隔

## 🔧 常见问题

### Q: `make init` 失败，提示 "Ollama 未启动"
**A:** 确保 Ollama 服务正在运行
```bash
# 检查服务状态
ollama list

# 启动服务
ollama serve
```

### Q: `make run` 失败，提示 "未找到输入 CSV"
**A:** 检查 data 目录结构
```bash
# 确保目录存在
ls -la data/

# 确保 CSV 文件存在
ls -la data/51job/*.csv
```

### Q: Chroma 数据库连接失败
**A:** 检查 Docker 容器状态
```bash
# 查看容器日志
docker logs job-classifier-chroma

# 重启容器
docker restart job-classifier-chroma
```

### Q: 处理速度慢
**A:** 调整并发参数
```bash
# 增加查询并发数（注意：过高会导致 API 超时）
make run QUERY_WORKERS=80

# 减小 embedding 批大小
make run EMB_BATCH=10
```

## 📚 更多文档

### 项目文档

详细的技术方案和设计文档位于 `doc/` 目录：

- **[20251107 方案对比](./doc/20251107%20方案对比.md)** - 2025-11-27
  - 不同技术方案的对比分析

- **[20251124 基于 text2vec 岗位分类基础方案](./doc/20251124%20%20基于%20text2vec%20岗位分类基础方案.md)** - 2025-11-26
  - 岗位分类的基础技术方案

- **[20251126 准确度提升方案记录](./doc/20251126%20准确度提升方案记录.md)** - 2025-12-22
  - 准确度优化的详细记录

- **[20251127 数据统计 & 分阶段过程介绍](./doc/20251127%20数据统计%20&%20分阶段过程介绍.md)** - 2025-12-22
  - 数据统计方法和分阶段处理流程

### 其他资源

- **职业分类标准**：[`classification.json`](./classification.json)
- **进度统计配置**：`stats/config/`

## 🤝 贡献

遵循项目工程文化：
- 第一性原理：快速直接解决问题
- 分层设计，保持代码简洁
- 优先中文注释和文档

---

**作者：** Anner
**创建时间：** 2025-12-22
**联系方式：** helloanner@gmail.com
