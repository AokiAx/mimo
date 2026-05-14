# MiMo Manager

小米 MiMo Studio (`aistudio.xiaomimimo.com`) 的自动化管理工具。纯 HTTP / WebSocket 实现，无需浏览器。

包含三块功能：
1. **Claw 自动化** — 登录、Cookie 管理、对话、定时部署。
2. **API Gateway** — 把 MiMo 包装成 OpenAI / Anthropic 兼容的 API 端点。
3. **管理面板** — Web UI，查看账号、部署、流量、节点状态。

---

## 快速开始

```bash
pip install -r requirements.txt

# 1. 登录小米账号（可重复加多个账号）
python claw/mimo_auth.py login

# 2. 启动管理面板（默认 8088）
python app.py
# 或
bash run.sh
```

打开 `http://localhost:8088`，面板密码见 `data/secrets.json` 的 `panel_password` 字段。
首次运行会自动生成 `data/secrets.json`，包含随机面板密码和 API token。

公开统计页（无需登录）：`http://localhost:8088/stats`

环境变量：
- `MIMO_EMAIL`、`MIMO_PASSWORD` — 跳过交互输入
- `MIMO_PANEL_PASSWORD` — 覆盖面板密码
- `MIMO_PUBLIC_API_TOKEN` — 覆盖公开 API Bearer token
- `MIMO_UPSTREAM_API_KEY` — 覆盖上游默认 API key
- `DISABLE_SCHEDULER=1` — 不启动 auto-deploy 调度器（面板仍可手动触发部署）
- `MIMO_JUMP_LOCAL=1` — 面板**就跑在跳板机本机**时启用。`auto_deploy` 的端口清理 / 加公钥等操作改走本地 `bash -c`，避免 root 自连 SSH
- `MIMO_DEBUG_CLAW=1` — auto_deploy 把 Claw 的 WS 回复**完整**写入日志（默认只记前 200 字符）

---

## 项目结构

```
mimo/
├── app.py                  # FastAPI 入口：管理面板 + /v1/* gateway 路由
├── run.sh                  # 启动脚本
├── requirements.txt
│
├── claw/                   # MiMo 自动化
│   ├── mimo_auth.py        # 小米 SSO 登录 + Cookie 管理
│   ├── mimo_chat.py        # HTTP/SSE 对话客户端
│   ├── mimo_ws_client.py   # WebSocket 对话客户端
│   ├── auto_deploy.py      # 定时部署调度器（10 步流程）
│   └── payload/
│       └── api-proxy.py    # 部署到 ECS 的独立 asyncio 代理（零依赖）
│
├── gateway/                # API gateway（OpenAI / Anthropic / Responses 兼容）
│   ├── runtime.py          # 进程级 singleton：从 backends.json 读后端，
│   │                       # 把 handler+adapters+routing+transport 串起来；
│   │                       # 提供 dispatch() 给 app.py，提供状态接口给面板
│   ├── handler.py          # 终端 handler：路由→协议编码→上游→响应解码
│   ├── transport.py        # httpx 上游传输（流式 + 非流式）
│   ├── metrics.py          # SQLite 指标存储 + 聚合查询
│   ├── secrets_store.py    # 从 data/secrets.json 读凭证（支持环境变量覆盖）
│   ├── backend_store.py    # CRUD 持久化 data/backends.json
│   │
│   ├── adapters/           # 协议双向适配器
│   │   ├── base.py             # ProtocolAdapter / UpstreamCodec 接口
│   │   ├── openai_chat.py      # OpenAI Chat ⇄ IES（同时也是上游 codec）
│   │   ├── anthropic.py        # Anthropic Messages ⇄ IES（含 SSE 帧重组）
│   │   └── openai_responses.py # OpenAI Responses ⇄ IES
│   ├── core/               # 协议无关的核心
│   │   ├── context.py          # RequestContext + 决策追踪
│   │   ├── errors.py           # GatewayError 家族
│   │   └── types.py            # InternalEvent (IES) 中间表示
│   ├── routing/            # 后端选择
│   │   ├── backend.py          # Backend：health/breaker/EWMA/in_flight/enabled
│   │   ├── registry.py         # BackendRegistry
│   │   ├── router.py           # 评分选择 score = lat*(1+inflight)/weight
│   │   └── decision_log.py     # 路由决策审计
│   └── config/             # APIKeyStore（SQLite）
│       └── api_keys.py
│
├── templates/
│   ├── index.html          # 管理面板（受密码保护）
│   └── stats.html          # 公开统计页（无需登录）
│
├── probe/                  # VPS 探针 agent（可选部署）
├── tests/                  # pytest
├── docs/
│   └── deploy/             # 每个账号一份部署文案（被 auto_deploy 读取）
├── data/                   # 运行时（已 gitignore）
│   ├── secrets.json        # 面板密码 + API token（首次运行自动生成）
│   ├── backends.json       # 后端配置（面板 CRUD 或手动编辑）
│   ├── api_keys.db         # API key 存储
│   ├── metrics.db          # 请求指标
│   ├── auto_deploy.json    # Claw 部署调度配置
│   ├── deploy_history/
│   └── deploy_logs/
└── accounts/               # Cookie 存储（已 gitignore）
    ├── _current.json       # 当前活跃账号
    └── <email>.json
```

---

## Gateway 架构

```
client request (OpenAI / Anthropic / Responses)
   ↓
app.py /v1/{path:path}
   ↓ Bearer / 面板 cookie 鉴权
gateway.runtime.dispatch(adapter_name, request)
   ↓
GatewayHandler.handle(ctx, adapter, body)
   ├─ adapter.parse_request(body)         ← 客户端协议 → IES
   ├─ Router.choose(model)                ← 评分选后端
   ├─ HttpxTransport.post_json/post_stream ← 上游 (MiMo OpenAI Chat)
   ├─ codec.parse_upstream_*              ← 上游响应 → IES
   ├─ adapter.serialize_response_*        ← IES → 客户端协议
   └─ metrics + decision log
```

**关键特性**：
- **手动后端管理**：后端配置存 `data/backends.json`，通过管理面板 CRUD 或手动编辑。面板支持增删改查、启停、热加载。
- **凭证隔离**：密码和 token 存 `data/secrets.json`，首次运行自动生成。支持 `MIMO_PANEL_PASSWORD` / `MIMO_PUBLIC_API_TOKEN` / `MIMO_UPSTREAM_API_KEY` 环境变量覆盖。
- **协议双向转换**：三种客户端协议都用 `InternalEvent` 中间表示双向编解码。Anthropic 的 `event: message_start / content_block_delta / message_stop` SSE 帧会正确生成。
- **评分路由**：`score = ewma_latency * (1 + in_flight) / weight`，更低更优。
- **熔断**：连续失败 3 次后冷却 30s。`enabled` 开关可独立禁用后端。
- **健康探测**：60s 一次 HTTP GET `/v1/models`（不消耗推理 token）。

---

## 主要 API 端点

### 兼容端点（gateway 提供，app.py 路由）
- `POST /v1/chat/completions` — OpenAI Chat
- `POST /v1/messages` — Anthropic Messages
- `POST /v1/responses` — OpenAI Responses
- 鉴权：`Authorization: Bearer sk-...`，key 在管理面板创建

### 管理面板（密码保护）
- `GET /` — 主面板
- `GET /api/accounts` / `POST /api/account/*` — 账号管理
- `GET /api/deploy/*` — 部署调度
- `GET /api/gateway/status` — 路由概览（uptime / qps / 后端数）
- `GET /api/gateway/backends` — 后端列表
- `POST /api/gateway/backends/{id}/toggle` — 启停某后端
- `POST /api/gateway/backends/reload` — 重读 auto_deploy.json
- `GET /api/gateway/metrics/{summary|hourly|backends|status}` — 指标
- `GET /api/gateway/vps` / `POST /api/gateway/vps/refresh` — 节点状态

### 公开端点（无需登录）
- `GET /stats` — 公开统计页
- `GET /api/public/stats` — 公开统计 JSON

---

## CLI 命令（claw）

```bash
python claw/mimo_auth.py status         # 查看 Cookie 状态
python claw/mimo_auth.py login          # 交互式登录
python claw/mimo_auth.py cookie-header  # 输出 Cookie Header
python claw/mimo_auth.py auto-refresh   # 自动续期（cron 友好）

python claw/mimo_chat.py "你好"         # 单轮对话
python claw/mimo_ws_client.py "你好"    # WebSocket 对话
```

---

## 登录流程（小米 SSO 逆向）

1. `GET /open-apis/v1/genLoginUrl` → 动态 callback URL
2. `GET /pass/serviceLogin` → 提取 `_sign`
3. `POST /pass/serviceLoginAuth2` → 提交账号密码
4. 若触发二次验证：`identity/list` → `verifyEmail` → `sendEmailTicket` → 输入验证码 → `result/check`
5. 跟随 302 跳转链拿到 `serviceToken`

Cookie 跨 `.account.xiaomi.com` / `.xiaomi.com` / `.xiaomimimo.com` 三个域，关键凭证：
| Cookie | 域 | 作用 |
|---|---|---|
| `serviceToken` | `.xiaomimimo.com` | API 鉴权 |
| `userId` | `.xiaomimimo.com` | 用户 ID |
| `xiaomichatbot_ph` | `.xiaomimimo.com` | 会话 |

---

## Change Log

### 2026-05-14 — MiMo reasoning_content 适配

- **思考模式兼容**：OpenAI Chat 适配器现在会在 assistant 历史消息、非流式响应与流式增量中保留并回传 `reasoning_content`，避免 MiMo 在“思考模式 + 工具调用 + 多轮对话”场景因缺失该字段返回 400。
- **中间表示扩展**：内部消息和事件流新增 reasoning 承载能力，使上游返回的 `reasoning_content` 可继续透传给 OpenAI 兼容客户端保存并在后续请求中回传。
- **强制兼容兜底**：Gateway 会按工具调用 ID 缓存上游 reasoning；当客户端/第三方项目丢弃该字段后，后续请求会自动补回，缓存未命中时也会为 assistant tool_call 消息补上空 `reasoning_content` 字段以避免缺字段错误。

### 2026-05-14 — 架构拆分与数据面增强

- **Gateway 路由拆分**：将 `/v1/*`、CORS、健康检查等数据面路由从 `app.py` 拆到 `gateway/routes.py`，降低主应用入口复杂度。
- **Gateway APIKeyStore 鉴权**：`/v1/*` Bearer token 现在优先兼容旧 public token，并支持 `gateway.config.APIKeyStore` 中的可撤销 API key 与模型白名单。
- **有限重试 / Failover**：非流式请求遇到可重试的上游 5xx、连接失败或超时，会在未向客户端返回前尝试下一个可用 backend。
- **异步指标队列**：gateway runtime 改用后台队列批量写入 SQLite 指标，减少请求热路径上的同步 commit 开销。

### 2026-05-14 — 稳定性与性能优化

- **账号状态兼容性**：移除对旧版单文件 `COOKIE_FILE` 的依赖，当前账号状态与自动切换账号统一使用 `accounts/<账号>.json` 和 `accounts/_current.json`。
- **Gateway 热路径优化**：`/v1/*` 转发入口不再提前读取未使用的请求体和 headers，减少大请求与高并发场景下的内存/CPU 开销。
- **上游错误观测增强**：上游 HTTP 4xx/5xx 状态由 handler 统一处理和记录，避免 transport 提前抛错导致指标里丢失真实 status。
- **流式响应稳定性**：流式请求仅在完整结束后标记 backend 成功；中途异常会记录 backend failure 与 metrics error，避免错误流被误判为成功。
- **健康探测并发化**：backend `/v1/models` 健康探测改为有界并发，减少多后端场景下的探测总耗时，同时保留原有熔断/延迟统计行为。
- **配置写入更安全**：`data/backends.json` 与 `data/model_groups.json` 采用临时文件 + `fsync` + 原子替换写入，降低进程中断导致 JSON 损坏的风险。
- **代理连接复用**：部署到 ECS 的 aiohttp 代理不再强制响应 `Connection: close`，允许客户端连接复用，降低频繁请求时的握手开销。

## 测试

```bash
python -m pytest tests/ -q     # 全部
python -m pytest tests/test_metrics.py tests/test_vps_probe.py -v
```

---

## 依赖

- Python 3.8+
- `fastapi`、`uvicorn`、`jinja2`、`httpx`、`requests`、`websockets`、`croniter`、`pyyaml`

详见 `requirements.txt`。
