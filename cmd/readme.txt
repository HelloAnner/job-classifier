cmd/ 目录说明
====================

本目录包含项目的所有命令行工具，每个子目录对应一个独立可执行程序。

## 目录结构

### cmd/classifier/
Excel职业分类数据转JSON工具
- **主要功能**：读取Excel格式的职业分类数据，转换为结构化JSON文件
- **依赖**：github.com/xuri/excelize/v2
- **输入**：Excel文件（默认：中国职业分类大典_去掉一些大类_去掉99(1).xlsx）
- **输出**：JSON文件（默认：data/classification.json）
- **用法**：./classifier [-input 文件路径] [-output 输出路径] [-sheet 工作表名] [-header 表头行号]

### cmd/importer/
文件数据导入SQLite数据库工具
- **主要功能**：递归扫描目录，将文本文件内容导入SQLite数据库
- **依赖**：github.com/mattn/go-sqlite3
- **数据库**：jobs.db（自动创建）
- **输入**：递归扫描指定目录下的所有文件
- **输出**：jobs 表（包含mid、job_intro、source、status、category等字段）
- **特性**：
  - 支持多线程并行处理
  - 配额管理（每个来源目录限制最大导入数）
  - 自动更新机制（基于文件修改时间）
- **用法**：./importer [-path 扫描路径] [-db 数据库路径] [-concurrency 并发数] [-limit 单目录配额]

### cmd/jobimport/
职业分类数据向量化导入ChromaDB工具
- **主要功能**：将职业分类JSON数据向量化并导入ChromaDB向量数据库
- **依赖**：github.com/google/uuid
- **输入**：data/classification.json（职业分类数据）
- **输出**：ChromaDB的job_classification集合（UUID: 48cc070b-66b5-4e78-8a90-a8a23f64a028）
- **特性**：
  - 调用Ollama API进行文本向量化（模型：bge-zh）
  - 批量处理（每批50条）
  - 保留完整元数据
  - 自动处理JSON中的特殊字符
- **依赖服务**：
  - Ollama服务（http://localhost:11434，提供bge-zh模型）
  - ChromaDB服务（http://localhost:8000）
- **用法**：./job-import

### cmd/query/
岗位分类查询工具（向量相似度搜索）
- **主要功能**：从SQLite数据库读取岗位描述，向量化后查询ChromaDB，输出最匹配的岗位分类到CSV文件
- **依赖**：github.com/mattn/go-sqlite3（数据库），encoding/csv（CSV处理）
- **输入**：
  - SQLite数据库：data/jobs.db
  - ChromaDB集合：job_classification（UUID: 48cc070b-66b5-4e78-8a90-a8a23f64a028）
- **输出**：CSV文件（result/YYYYMMDD_HHMMSS_query_results.csv）
- **特性**：
  - 批量向量查询
  - 相似度阈值过滤（≥0.7）
  - 自动创建result目录
  - 详细的进度日志
- **CSV字段**：job_id, job_intro, similarity_score, 大类, 大类含义, 中类, 中类含义, 小类, 小类含义, 细类职业, 细类含义, 细类主要工作任务
- **依赖服务**：
  - Ollama服务（http://localhost:11434，提供bge-zh模型）
  - ChromaDB服务（http://localhost:8000）
- **用法**：./job-query

## 构建说明

在每个子目录下执行：
```bash
go build -o [程序名] main.go
```

或在项目根目录：
```bash
go build -o [程序名] cmd/[目录名]/main.go
```

示例：
```bash
# 构建所有工具
go build -o classifier cmd/classifier/main.go
go build -o importer cmd/importer/main.go
go build -o job-import cmd/jobimport/main.go
go build -o job-query cmd/query/main.go
```

## 执行顺序建议

1. **数据准备**（如需要）：
   - 运行 classifier 将Excel转换为JSON
   - 运行 importer 导入岗位数据到SQLite

2. **向量数据库构建**：
   - 确保Ollama和ChromaDB服务已启动
   - 运行 job-import 导入职业分类数据

3. **查询分析**：
   - 运行 job-query 查询岗位分类匹配结果
   - 查看result/目录下的CSV文件

## 环境要求

- Go 1.24.6 或更高版本
- SQLite3（importer和query需要）
- Ollama + bge-zh模型（jobimport和query需要）
- ChromaDB（jobimport和query需要）

## 服务启动命令

```bash
# 启动Ollama（已安装bge-zh模型）
ollama serve

# 启动ChromaDB
docker run -p 8000:8000 chromadb/chroma
```
