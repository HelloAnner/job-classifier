# 单文件顺序模式下的提速方案（当前代码现状版）

> 现状摘要（2025-12-13）：  
> - Makefile 默认单文件顺序处理；文件内高并发（query-workers 48，emb-batch 128，topK 150）。  
> - embedding 请求已带 keep_alive=-1；Chroma 批查询会按 topK 拆批防 SQL 变量超限。  
> - CSV 写入异步（单文件独立写协程，5s flush/心跳）。  
> - 多文件并行已关闭，避免系统卡顿。

目标：在单文件顺序模式下，最大化单 CSV 吞吐，避免 0 TPS、超时或长尾卡顿。

## 1. 观察与定位
- **TPS=0 且 result 行数不变**：大概率卡在 embedding 请求（超时/排队）；日志无 ERROR 但长时间无 Progress。  
- **Chroma 500 too many SQL variables**：批次过大或 topK 过高；代码已自动拆批，但仍需控制 topK。  
- **Ollama 日志 aborting embedding request due to client closing**：客户端因超时/stop 重启导致连接中断。

## 2. 推荐的运行参数（稳态）
- `make start bo QUERY_WORKERS=48 EMB_BATCH=128 CHROMA_TOPK=150`（当前默认）。  
- 若仍有 embedding 超时：将 `EMB_BATCH` 下调到 `96`；如 GPU/ANE 很空，再把 `QUERY_WORKERS` 提到 `64`。  
- 若 Chroma 偶发 500：将 `CHROMA_TOPK` 降到 `120`。

## 3. Ollama 侧避免冷启动/超时
- 保持服务常驻（你已用 `OLLAMA_KEEP_ALIVE=-1` 启动）。  
- 预热：启动前跑一次小批请求 `/api/embed`。  
- 如果仍见超时：把 `EMB_TIMEOUT` 提到 `900s`；同时减小 `EMB_BATCH` 防止单请求体积过大。

## 4. 单 CSV 内部的并行与 I/O
- **读取/导入**：继续使用流式导入；如磁盘不忙，可将 `batchSize` 提到 `4000` 以减少事务提交次数。  
- **查询并发**：`query-workers` 控制 CPU/网络压力；调高时务必同步观察 Ollama 请求排队。  
- **写出**：异步写线程已存在，5s flush；无需额外改动。

## 5. 顺序处理全部 CSV（无排队但无并行）
- 现在会按文件名顺序依次处理所有 CSV。  
- 若需要“优先处理某批”，可用 `INPUT=data/BO/bo_2022_14.csv` 单独跑，再恢复全量。

## 6. 避免无效工作
- 跳过逻辑已生效：有 `count.txt` 或 `result+ignore == db` 会自动回填/跳过。  
- 检查未动的文件：看 `run_bo.log` 是否出现对应 `[START]/[RESUME]`；没有则输入路径或 skip 条件未触发。

## 7. 调优快捷清单
- embedding 超时多：`EMB_BATCH=96`，`QUERY_WORKERS=48`，`EMB_TIMEOUT=900s`。  
- Chroma 500：`CHROMA_TOPK=120`（或更低）或保持现在的拆批规则。  
- CPU/内存很空但 TPS 不高：`QUERY_WORKERS` + `EMB_BATCH` 小步一起上调，观察 5 分钟日志。  
- 只跑单文件验证：`make start bo INPUT=data/BO/bo_2022_14.csv`.

## 8. 后续可以考虑（如需进一步提速）
- 在单文件内增加结果/embedding 缓存（按 buildFullText 哈希）以复用重复 JD。  
- 关闭 `NormalizeJobIDs` 全表扫描（如 mid 已规范）。  
- SQLite MarkProcessed 批量化（当前逐条 UPDATE，是潜在下一瓶颈）。
