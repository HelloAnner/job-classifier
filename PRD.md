# PRD — 招聘数据入库（SQLite）

## 目标
- 从 `data/` 目录下每个来源子目录（51job/58tong/BO/zhilian 等）各抽取 1000 条招聘数据，落地到 SQLite `data/jobs.db`。
- 记录来源目录名、处理状态（待处理/处理完成）、分类（默认空）与上次修改时间，满足后续标注需求。

## 范围
- 数据源：`51job/`、`58tong/`、`BO/`、`zhilian/` 目录下所有 `.csv`。
- 工具：Go 程序 `cmd/importer/main.go`，SQLite；新增 Excel 解析工具 `cmd/classifier/main.go`。

## Excel 职业分类导出（新增）
- 目标：将 `中国职业分类大典_去掉一些大类_去掉99(1).xlsx` 全量解析为平铺 JSON，任何一行中的大类/中类/小类/细类字段均不能缺失。
- 输出：`data/classification.json`，记录数与 Excel 数据行一致，并保留源行号。
- 可配置项：`-input/-output/-sheet/-header` 命令行参数，默认使用仓库根目录 Excel、首个工作表、第一行表头。

### 职业分类 JSON 增强（更新）
- `cmd/classifier` 现默认读取 `classification.json`，并使用大模型对每条职业补充自然语言描述后再写回。
- 通过 `LLM岗位概述`（120~180 字）、`LLM典型职责`（3~5 条）、`LLM关联关键词`（6~10 个）、`LLM典型岗位`（3~5 个）四个新字段，提升与企业 JD 的语义贴合度；默认跳过已存在描述，可用 `-force` 强制重写。
- 支持 `-workers/-limit/-temperature/-timeout/-retries` 等参数控制并发和重试，LLM 访问信息通过 `LLM_API_KEY/LLM_BASE_URL/LLM_MODEL_NAME` 环境变量或命令行传入。
- 输出文件使用临时文件 + rename，避免中途失败导致 JSON 损坏。

### 职业分类向量入库（更新）
- `cmd/jobimport` 直接消费增强后的 `classification.json`（优先 `data/classification.json`，不存在则读取根目录版本），并在嵌入文本中拼接 LLM 概述、职责、关键词与典型岗位。
- 元数据同时写入上述 LLM 字段，便于 query 模块在 CSV 中回填自然描述；空数组/空字符串会被清洗掉。
- 向量模型升级为 `quentinz/bge-large-zh-v1.5`（Ollama embeddings API），仍保持 1024 维度的向量，并执行归一化以匹配 `cosine` 距离集合设置。
- 重新导入时会自动删除/重建 Chroma 集合 `job_classification`，批大小 50，日志打印首个 ID 及进度，确保全量 1608 条职业全部落库。

## 功能需求
1. 支持多协程并发读取 CSV，单连接写入 SQLite，批量提交。
2. 表结构：`mid`(PK)、`job_intro`、`structured_json`（本地小模型返回的结构化 JSON）、`structured_summary`、`structured_responsibilities`（JSON 数组）、`structured_skills`（JSON 数组）、`structured_industry`、`structured_locations`（JSON 数组）、`structured_category_hints`（JSON 数组）、`structured_updated_at`（RFC3339）、`source`（来源目录名）、`status`（待处理/处理完成，默认待处理）、`category`（默认为空字符串）、`updated_at`（文件最后修改时间，RFC3339）。
3. 每个来源目录（以 `data/` 下一级目录划分）最多导入 1000 条记录，默认值可通过 `-limit` 参数调整（0 表示不限制）。
4. 数据去重：`mid` 主键，使用 `INSERT OR REPLACE` 覆盖。
5. 性能：默认批量 5000，WAL + synchronous=NORMAL，支持 `-workers` 和 `-batch` 参数调优。
6. 可靠性：遇到文件解析错误或结构化失败仅打印日志不中断。

### 结构化岗位摘要（更新）
- importer 不再从 CSV 读取新岗位，而是直接查询 SQLite 中 `structured_summary` 为空的记录，逐条调用本地 `ollama` 服务 `qwen2:7b-instruct` 模型生成结构化结果。
- 所有结构化请求通过 HTTP `POST http://localhost:11434/api/chat` 完成，并带 45s 超时控制；失败会回落为空字符串，日志中记录 `mid` 与异常原因。
- `structured_json` 保存模型原始 JSON（格式化），其余字段（summary/responsibilities/skills/industry/locations/category_hints/updated_at）逐列展开，便于 SQL 分析与 query 端组合特征。
- importer 的查询、模型调用与 UPDATE 均需打印详细日志（`[QUERY]`、`[MODEL]`、`[STORE]`），确保每条记录可追溯。
- importer 以 10 个并发 worker 处理待结构化岗位；对每条数据在完成后额外输出 `[STATUS]` 日志，并在批量结束时输出 `[SUMMARY] total/success/failed`。模型请求超时时间为 5 分钟。

### 向量查询策略（更新）
- query 程序在构造向量输入时优先拼接 `structured_json` 中的摘要、职责、技能与大类猜测，再附上原始 `job_intro` 及来源/状态/分类字段，保证文本风格与职业分类库一致。
- query 程序在构造向量输入时优先拼接 `structured_json` 中的摘要、职责、技能与大类猜测，再附上原始 `job_intro` 及来源/状态/分类字段，与职业分类库新增的 LLM 字段保持语言风格一致。
- Chroma 集合固定使用 `cosine` 距离，查询阶段对返回的 `distance` 直接计算 `score = clamp(1 - distance, 0, 1)`，得分区间严格为 `[0,1]`，默认阈值仍为 `0.7`（等价于距离 ≤ 0.3）。
- 日志中持续打印 `distance` 与换算后的 `similarity`，用于监控分布是否过于集中；当得分低于阈值直接跳过 CSV 输出。
## 非功能
- 仅本地 SQLite，禁用远端 CI/CD。
- 新文件编码 UTF-8，无 BOM；文档与注释中文。

## 验收
- 对每个来源目录执行抽样导入（默认 1000 条）后，`jobs` 表中各来源计数与日志一致。
- `docs/IMPORT_GUIDE.md` 记录运行方法、字段含义与抽样策略。
- `data/file_stats.csv` 若更新，仍需列出每个文件的记录数及总计。
- 在 `jobs` 表中随机抽检 10 条记录，`structured_json` 必须是合法 JSON 并包含“岗位概述”“核心职责”等字段。
- query 运行日志需展示 `distance` 与 `similarity`（0~1），确保高于 0.7 的记录被写入 CSV，且 CSV 中包含结构化信息映射出的分类字段。
- `classification.json` 中任意抽查 10 条记录需包含 LLM 新增的四个字段，且内容非空；Chroma `job_classification` 集合中的 metadata 也能够返回这些字段供 query 使用。
