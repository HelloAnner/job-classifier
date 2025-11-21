# 职业分类 Excel 转 JSON 指南

## 1. 运行命令
```bash
cd /Users/anner/Downloads/ZP
# 默认解析根目录下 Excel，输出到 data/classification.json
go run ./cmd/classifier
# 如果需要指定其他文件/工作表
go run ./cmd/classifier -input 自定义.xlsx -sheet "Sheet1" -output data/custom.json
```
参数含义：
- `-input`：Excel 路径，默认 `中国职业分类大典_去掉一些大类_去掉99(1).xlsx`。
- `-output`：结果 JSON 路径，`-` 表示写入标准输出（便于管道处理）。
- `-sheet`：工作表名称，留空则取首个工作表。
- `-header`：表头所在行（从 1 开始），默认 1。

## 2. 数据展开逻辑
- 读取表头（大类/中类/小类/细类等 10 个字段），自动补齐重复或空列名，确保字段唯一。
- 自第二行起逐行扫描，如果单元格为空则继承上一条的同列值，保证层级信息完整。
- 忽略纯空行；其余每条数据写入一个 JSON 对象，并附加 `源行号` 字段便于追溯。
- 结果数组长度与 Excel 有效数据行数一致（当前为 1608 条）。

## 3. 校验方法
```bash
# 检查总数
jq length data/classification.json
# 查看某条记录
jq '.[100]' data/classification.json
```
若需再次生成，只需删除旧的 `data/classification.json` 或覆盖输出文件即可。
