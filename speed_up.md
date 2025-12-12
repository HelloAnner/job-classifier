# 170M 数据处理加速分析（combine 目录）

> 目标：在 **M4 Mac mini** 单机上，两天内完成约 **1.7 亿条岗位数据**的向量检索+图纠偏分类输出，且准确性不下降（底线）。  
> 说明：以下只基于现有代码与日志格式做分析，不运行、不改代码；给出可实施的加速方案与预期收益。

---

## 1. combine 当前实现的处理链路

combine 目录只有 `combine/main.go`，把原 `cmd/importer` + `cmd/query` 的逻辑整合到单文件流程里。

### 1.1 单 CSV 的 pipeline

入口：`processSingleCSV`（`combine/main.go:920`）。

1. **跳过/回填判定**  
   `tryBackfillOrSkip`（`combine/main.go:977`）  
   - 若 `output/<base>/count.txt` 存在，视为已完成直接 skip。  
   - 若不存在，但 `result.csv 行数 + ignore.csv 行数 == origin.db 行数`，回填 count.txt 后 skip。  

2. **导入阶段（CSV -> SQLite + ignore.csv）**  
   `streamCSVToDB`（`combine/main.go:109`）  
   - CSV 流式读取。  
   - intro 长度 `<6` 的写入 `ignore.csv`，并仍写入 DB 但 `Filtered=true`。  
   - 其余写入 DB，按 `batchSize`（默认 2000）做事务批插入。  

3. **查询阶段（SQLite -> embedding -> Chroma ANN -> 图纠偏 -> result.csv）**  
   `QueryProcessor.Process`（`combine/main.go:618`）。  
   - `StreamJobs`（`combine/main.go:350`）只取 status 不是“处理完成/过滤”的岗位，支持续跑。  
   - 启动 `queryWorkers`（默认 10）个 goroutine。  
   - 每条 job 调用 `processSingleJob`（`combine/main.go:695`）：  
     1) `Ollama /api/embeddings` 获取 embedding  
     2) normalize 后 `Chroma /query` 查 top‑K（默认200，可调）  
     3) 取最优 top‑1  
     4) `GraphRepository.GetNeighbors` 做图纠偏（最多 5 个邻居）  
   - consumer 侧对每条结果执行：  
     - `MarkProcessed`（`combine/main.go:1067`）单行 UPDATE  
     - 写入 `result.csv`（每 100 行 flush）  

4. **统计与一致性校验**  
   - count `result.csv` + `ignore.csv` 与 DB 行数一致后写 `count.txt`。  

### 1.2 并发层级

- 文件级：`fileWorkers`（默认 `NumCPU/2`）并行多个 CSV（`combine/main.go:1042`）。  
- 文件内：每个 CSV 再开 `queryWorkers` 并行 embedding + Chroma 请求。  
- **总并发 ≈ fileWorkers × queryWorkers**。在 M4 上默认可能是 5×10=50 左右并发请求。

---

## 2. 从代码推断的主要性能瓶颈

### 2.1 每条 job 的“硬成本”几乎全在 embedding + Chroma

`processSingleJob`（`combine/main.go:695`）对每条 job 都做两次 HTTP 往返：

1. **Ollama embedding**  
   - 本地大模型 `quentinz/bge-large-zh-v1.5`（large 级别）。  
   - 每条 prompt 长度包含 structured_json + intro + 元信息，token 数不小。  
   - 单请求典型耗时在 **几十到数百 ms**（取决于量化、GPU/ANE 利用、并发队列）。  

2. **Chroma top‑K ANN 查询**  
   - 当前默认 `n_results=200`（`--chroma-topk` 可调），历史实现是 2000。  
   - K 过大时，传输/JSON decode 与遍历距离的成本近似线性增长。  

结论：当前 **吞吐上限 ≈ 1 / (embedding_latency + chroma_latency)**。  
只要你仍然对每条数据都做 embedding+ANN，这个上限非常难到达 1000/s。

### 2.2 额外的 O(n) 全表扫描/更新

这些在数据巨大时会变成“第二瓶颈”：

1. **NormalizeJobIDs 全量扫描**  
   `NormalizeJobIDs`（`combine/main.go:287`）  
   - 扫描 `jobs` 全表，`used := make(map[string]struct{})` 记录所有合法 mid。  
   - 若单 CSV 级别达到千万量级，map 内存与扫描时间都很可观；  
   - 若 mid 基本都合法，这一步完全是冗余成本。

2. **MarkProcessed 单行 UPDATE**  
   `MarkProcessed`（`combine/main.go:1067`）每条 job 一次 UPDATE。  
   - 170M 次 UPDATE 对 SQLite 是灾难级别的 IO；  
   - 当前 embedding 已经慢，但一旦你加速 embedding，这里会立刻顶住。

### 2.3 processedSet 续跑集合的内存风险

`loadProcessedSet`（`combine/main.go:867`）会把已有 `result.csv` 的 job_id 全量读到 map。  
若某个 CSV 的已处理量达到数千万，map 会吃掉数 GB 内存。

---

## 3. 两天目标的数量级判断

### 3.1 必要总吞吐

两天 = 48h = 172,800 秒。  

**必须吞吐**：

```
170,000,000 / 172,800 ≈ 984 条/秒（全量都要处理的情况下）
```

这是单机“逐条 embedding + ANN”几乎不可能的量级。  

### 3.2 现实可行的关键：避免对所有 170M 做 embedding

代码里已经过滤 intro `<6` 的行。  
如果 **过滤占比很高**（例如 70‑90%），真正进入 embedding 的数据量可能是 17M‑51M：  

- 51M / 172,800 ≈ 295/s  
- 17M / 172,800 ≈ 98/s  

后者在 M4 上通过“批量化 + 减少 topK + 合理并发 + 缓存”是有希望的。

---

## 4. 如何从现有日志精确算出当前速度（你可以马上算）

`combine/main.go:1027` 启用了微秒日志。两类关键 log：

1. **进度日志**（每 50 条打印一次）  
   `Progress: curr/total`（`combine/main.go:674`）  
   - 取两条 Progress 的时间差 Δt  
   - **瞬时速度 ≈ 50 / Δt**  

2. **完成日志**  
   `Query completed! ... elapsed=xxs`（`combine/main.go:690`）  
   - **平均速度 = processedSession / elapsed**  

把你 log 里这两个值代入即可得到真实吞吐。  
如果你给我一段 `Query completed` 的日志（含 processed 与 elapsed），我可以帮你算差距与需要的提速倍数。

---

## 5. “Ollama 向量化 + Chroma Docker”方案原理

### 5.1 原理（你现在的实现）

1. **离线阶段**（你已完成）  
   - 对“职业分类库/细类职业”文本做 embedding  
   - 存入 Chroma collection `job_classification`，metadata 带“大类/中类/小类/细类职业”等

2. **在线分类阶段（combine）**  
   - 对每条岗位描述做 embedding（同一模型、同一向量空间）  
   - Chroma 用 HNSW/ANN 搜最近邻，返回 topK 候选  
   - 取 top1 作为分类  
   - 再用图谱邻居做“纠偏”：  
     如果 topK 里有图邻居且相似度更高，则替换（`combine/main.go:562`）

### 5.2 主要成本构成

对单条 job：

- embedding 推理：O(tokens × layers)  
- ANN 检索：O(log N)（HNSW）+ 返回 K 条 JSON  
- 图纠偏：一次小型 SQLite 查询（5 个邻居）

当 K 很大（如 2000）时，**返回与遍历 K 的成本接近线性**，几乎把 ANN 的优势吃掉了。

---

## 6. 可实施的加速方案（按优先级）

> 原则：**不牺牲准确性**，优先做“减少需要推理的条数”和“提高单位推理吞吐”。

### 6.1 方案 A：结果/向量缓存（最关键，准确性零损失）

**做法**  
- 对 `full`（即 `processSingleJob` 里的拼接文本）做稳定 hash。  
- 先查缓存：  
  - **命中：直接复用 embedding 或最终分类结果**  
  - 未命中：走正常 embedding+Chroma+图纠偏，然后写回缓存  

**为什么安全**  
完全相同文本得到完全相同 embedding 与 top1，准确性不变。

**收益预期**  
与重复率线性相关：  
- 重复率 30% ⇒ 总时间立刻缩短 ~30%  
- 重复率 60% ⇒ 总时间缩短 ~60%  

对 170M 这种规模，重复率通常不低（岗位 JD 模板化严重），这是能否两天完成的决定性因素。

### 6.2 方案 B：Embedding 请求批量化（高收益，准确性零损失）

**做法**  
- Ollama embedding API 若支持“多 prompt 一次请求”，就把 jobsCh 按 batch 聚合后发送。  
- 目标是让 GPU/ANE 做 **batch 推理**，减少 per‑request 固定开销。  

**收益预期**  
在 Apple Silicon 上，batch embedding 常见 2‑5× 吞吐提升。

> 如果你确认 Ollama 版本不支持批量，备选是：在客户端层做“并发上限 + 更大 queryWorkers”，但收益会弱很多。

### 6.3 方案 C：降低 Chroma 返回 topK（需小样本验证）

历史实现 `n_results=2000`；当前默认已降到 `200`（`--chroma-topk` 可调）。  
对“只要 top1 + 5 个图邻居纠偏”的算法来说 **严重过量**。

**做法**  
- 先在小样本上测试：K=2000 与 K=200/100 的 top1 一致率  
- 若一致率 ≥99%（通常会是这样），就降 K  

**收益预期**  
- 传输/JSON/遍历成本 2000→200 约 10× 减少  
- 单条 job 端到端可期 1.5‑3× 提升  

**准确性守护**  
如果担心 ANN recall 下降，可在 Chroma 侧提高 `ef_search`，让 recall 保持。

### 6.4 方案 D：并发“找甜点”（无代码/少代码）

当前总并发可能 50+。但 embedding 与 Chroma 都是服务型组件：  
并发过高会让服务排队/抢锁，吞吐反而下降。

**做法**  
1. 先用 `file-workers=1`，只跑一个 CSV，逐步调 `query-workers`：  
   - 观察 `Query completed` 的平均速度  
   - 找到速度峰值对应的 worker 数  
2. 再把 `file-workers` 加到 2‑3（只在 embedding 仍未打满时增加）。  

**收益预期**  
通常能拿到 1.2‑2× 的稳定提升（来自减少排队与上下文切换）。

### 6.5 方案 E：批量更新“处理完成”状态（小改动，高收益）

**问题**  
`MarkProcessed` 每条 UPDATE（`combine/main.go:1067`）在大规模下会成为 IO 瓶颈。

**做法**  
- consumer 侧把 mid 缓存在 slice  
- 每 N=5k‑20k 条做一次事务批更新  
- crash 时最多回滚 N 条，续跑会重算，但结果一致  

**收益预期**  
SQLite 批事务写通常比单条 UPDATE 快 5‑20×。  
在 embedding 加速后，这是必须做的补刀。

### 6.6 方案 F：跳过 NormalizeJobIDs 全表扫描（小改动，高收益）

**问题**  
`NormalizeJobIDs`（`combine/main.go:287`）对每个 origin.db 全表扫描+建 map。  
对“mid 已规范”的数据是纯浪费，且极耗内存。

**做法**  
- 增加开关 `--skip-normalize` 或  
- 只抽样前 1k 行：若合法率>99.9% 就跳过

**收益预期**  
按 CSV 规模不同，节省 5‑30% 的单文件启动时间，避免大文件 OOM 风险。

### 6.7 方案 G：Embedding 模型/量化的速度‑准确性折中（需验证）

如果以上手段仍不够（尤其在过滤率和重复率都不高时），只能考虑更快模型或更强量化。

**可选路径**  
1. **更小模型（如 bge‑small‑zh / gte‑base 等）**  
   - 先取 1‑5 万条样本比对 top1 一致率  
   - 若一致率≥99%，可切  
2. **更激进量化（Q4/Q5）**  
   - embedding 维持同一模型家族，通常一致率下降很小  

**收益预期**  
2‑4× embedding 吞吐提升。  
但这是“最后的杠杆”，一定要先做 A/B 验证。

### 6.8 方案 H：Chroma 侧加速（重点，可落地）

> 你已经把 Ollama 做成批量了，接下来 Chroma 会更容易变成主瓶颈。  
> 以下方案按“准确性不下降”为前提排序。

#### H1. 降低返回 K + 减少返回字段（准确性零损失/可验证）

历史查询：`n_results=2000` 且 `include=["documents","metadatas","distances"]`。  
当前默认已精简为 `include=["metadatas","distances"]` 并降 K。  
你的在线逻辑只用到：
- `distances`（选 top1 + 图纠偏排序）
- `metadatas`（写 result.csv）
- **完全不使用 `documents`**

**做法**  
1. 先按 6.3 把 `n_results` 试降到 200/100（在小样本上对比 top1 一致率）。  
2. `include` 去掉 `documents`（只保留 metadatas + distances）。  

**收益预期**  
- K 降低是最大项：2000→200 通常 1.5‑3× 提升  
- include 精简会让 HTTP payload/JSON decode 下降 10‑30%  

#### H2. Chroma Query 批量化（准确性零损失）

Chroma `/query` 支持一次提交多个 `query_embeddings`，返回与输入等长的结果矩阵。  
当前 combine 已实现 Chroma 批量 query（与 embedding 批量对齐），下面是原理与收益说明。

**做法**  
- 对同一批 embedding 组成一个 `QueryEmbeddings: [][]float32{...}` 请求。  
- 每次 batch 只发 1 个 HTTP，减少 RTT/JSON overhead。  

**收益预期**  
在本机 Docker 场景常见 1.2‑2×（取决于你当前 query 延迟结构）。  

#### H3. HNSW `ef_search`/`M` 参数调优（需验证）

Chroma 的 ANN 基于 HNSW，关键参数：
- `hnsw:ef_search`：查询时的候选扩展数，越大 recall 越高但越慢  
- `hnsw:M`、`ef_construction`：建库时的图密度/构建精度，越高 recall 越好、查询可更快，但占内存

**做法**  
1. **先降 K（H1）后再看一致率**。若 top1 一致率下降：  
   - 适度 **提高 `ef_search`** 恢复 recall（比如 64→128→256）。  
2. 若你愿意重建 classification 库：  
   - 提高 `M` / `ef_construction`，换更高质量的索引，使查询在较低 ef_search 下仍高 recall。  

**收益预期**  
- 仅调 ef_search：用于“保准确性”，不是提速主手段  
- 重建高质量索引：查询侧可再提 1.2‑1.5×  

#### H4. 保证索引常驻内存 + 容器并发真正生效（准确性零损失）

**做法**  
- 给容器足够 RAM（确保 HNSW 全量驻留，避免 page‑fault）。  
- 启动多个 server worker（例如 uvicorn/gunicorn workers），让多路 query 并发不互相排队。  
- persist 目录放本地 NVMe；容器卷不要走远端/慢盘。  
- 关掉过度日志与 telemetry，降低 Python 端开销。  

**收益预期**  
1.2‑2×（取决于你现在容器的资源与 worker 配置）。

#### H5. 预热与长跑稳定性（准确性零损失）

**做法**  
- 运行前先发几百条 query 预热，确保索引加载、JIT/缓存到位。  
- 长跑时监控容器内存/CPU：一旦 swap，吞吐会掉一个数量级。  

**收益预期**  
主要提升稳定性，避免“跑几个小时后突然变慢”。

#### H6. 中期替代（需要改代码/不是两天内必做）

**做法**  
- 用本地 ANN 库（faiss/hnswlib）直接加载 classification 向量，去掉 Chroma HTTP/JSON 与 Python 端开销。  

**收益预期**  
2‑5×，但属于中期重构路线。

---

## 7. 两天内的推荐落地顺序（最小风险）

1. **先算清现实剩余量**  
   - 看各 CSV 的 `ignore.csv / count.txt`，估算过滤率  
   - 如果过滤率>70%，两天目标大概率靠 A‑F 就能完成  

2. **立即上缓存（方案 A）**  
   - 这是唯一“数量级”提升且无准确性风险的手段  

3. **做 Ollama 批量化（方案 B） + 并发甜点（方案 D）**  
   - 先把 embedding 吞吐打满  

4. **在小样本上验证降 K（方案 C），验证通过就上线**  
   - top1 一致率是唯一指标  

5. **补齐 SQLite 写入瓶颈（方案 E/F）**  
   - 避免后续被 IO 顶住  

6. 若仍不足，再考虑 **方案 G**（模型/量化切换）。

---

## 8. 预期提速组合与可达性

假设你当前端到端速度是 **X 条/秒**（按第 4 节从 log 算）。  
常见组合提速保守估计：

- A 缓存：×(1 / (1‑dupRate))，dupRate=0.4 ⇒ ×1.67  
- B 批量 embedding：×2‑4  
- C 降 K：×1.5‑3  
- D 并发甜点：×1.3  
- E 批 UPDATE：×1.1‑1.5（在 embedding 变快后）  

组合后典型可达 **×6‑15** 总提速。  
如果过滤率和重复率都高，再叠加“减少实际推理条数”，两天内完成是现实的。

---

## 9. 长期（非两天）更彻底的速度路线

这些能让你未来从根本上摆脱“逐条 HTTP 推理”的天花板：

1. **把分类库 embedding 载入 Go 进程，用本地 ANN（faiss/hnswlib）**  
   - 去掉 Chroma HTTP/JSON 开销  
2. **embedding 服务内嵌（或改用支持高吞吐 batch 的库）**  
   - 去掉 Ollama per‑request 开销  
3. **近似去重/聚类后再推理**  
   - 对模板化 JD 非常有效，但要慎重评估准确性

---

## 10. 你需要我进一步精算的内容

给我两段信息我就能把数字算得非常准：

1. 任意一段 `Query completed! ... processed=... elapsed=...` 日志  
2. 过滤率/重复率的粗估（比如任意几个 CSV 的 count.txt）

我会给出：  
- 你当前真实 X 条/秒  
- 两天目标需要的倍数  
- 按 6/7 节组合后的可达速度区间
