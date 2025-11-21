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

## 功能需求
1. 支持多协程并发读取 CSV，单连接写入 SQLite，批量提交。
2. 表结构：`mid`(PK)、`job_intro`、`source`（来源目录名）、`status`（待处理/处理完成，默认待处理）、`category`（默认为空字符串）、`updated_at`（文件最后修改时间，RFC3339）。
3. 每个来源目录（以 `data/` 下一级目录划分）最多导入 1000 条记录，默认值可通过 `-limit` 参数调整（0 表示不限制）。
4. 数据去重：`mid` 主键，使用 `INSERT OR REPLACE` 覆盖。
5. 性能：默认批量 5000，WAL + synchronous=NORMAL，支持 `-workers` 和 `-batch` 参数调优。
6. 可靠性：遇到文件解析错误仅打印日志不中断。

## 非功能
- 仅本地 SQLite，禁用远端 CI/CD。
- 新文件编码 UTF-8，无 BOM；文档与注释中文。

## 验收
- 对每个来源目录执行抽样导入（默认 1000 条）后，`jobs` 表中各来源计数与日志一致。
- `docs/IMPORT_GUIDE.md` 记录运行方法、字段含义与抽样策略。
- `data/file_stats.csv` 若更新，仍需列出每个文件的记录数及总计。
