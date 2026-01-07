# Codex / Responses API Relay (FastAPI + SQLite)

这是一个“中转服务器/反向代理”示例工程：
- 客户端（如 Codex CLI）把 base_url 指向本服务
- 本服务把请求转发到 OpenAI API（服务端注入 OPENAI_API_KEY）
- 统计每次请求的 token usage（含 cached / reasoning）并计算费用，落到 SQLite
- 支持 Responses API 的流式 SSE（严格按 SSE block 解析；最终 usage 在 response.completed 里）

## 目录结构
- app.py: 主服务
- users.json: 多用户 token + 限流/配额（热更新）
- prices.json: 模型单价表（USD / 1M tokens，热更新）
- relay.db: SQLite 数据库（运行后生成）

## 安装
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 配置
1) 设置 OpenAI API Key（只在服务器端）
```bash
export OPENAI_API_KEY="sk-..."
```

2) 编辑 users.json，给每个用户发一个 token（Bearer）
- 例如把 ming 的 token 改成随机长串
- 想封禁：删掉该用户或改 token

3) 如需修改价格：编辑 prices.json（会自动生效）

## 运行
```bash
export RELAY_DB="relay.db"
export USERS_PATH="users.json"
export PRICES_PATH="prices.json"

uvicorn app:app --host 0.0.0.0 --port 8080
```

## 使用
把客户端请求指到：
- Base URL: http://<relay-host>:8080/v1
- Header: Authorization: Bearer <users.json 里的 token>

本服务会把请求转发到 https://api.openai.com/v1/<path>

## 统计查询
- 最近请求明细：
  GET /requests/recent?limit=50
- 每日汇总（按 user）：
  GET /stats/daily?days=30

## 说明
- 当前限流/并发控制是“单机内存版”，如果你用 uvicorn 多 worker 或多机部署，需要把 bucket/concurrency 状态迁到 Redis。
