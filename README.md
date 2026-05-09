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

打开 `http://localhost:8088`，默认密码 `Aoki-MiMo`。
公开统计页（无需登录）：`http://localhost:8088/stats`

环境变量：
- `MIMO_EMAIL`、`MIMO_PASSWORD` — 跳过交互输入
- `GATEWAY_CONFIG` — 新版 pipeline 的 yaml 配置路径（见下文「Gateway 状态」）

---

## 项目结构

```
mimo/
├── app.py                  # FastAPI 入口：管理面板 + /v1/* gateway 路由
├── run.sh                  # 启动脚本
├── requirements.txt
├── gateway.example.yaml    # 独立部署 gateway/server.py 时的配置示例
│
├── claw/                   # MiMo 自动化
│   ├── mimo_auth.py        # 小米 SSO 登录 + Cookie 管理
│   ├── mimo_chat.py        # HTTP/SSE 对话客户端
│   ├── mimo_ws_client.py   # WebSocket 对话客户端
│   ├── auto_deploy.py      # 定时部署调度器（10 步流程）
│   └── deploy.py           # 手动单次部署
│
├── gateway/                # API gateway（OpenAI / Anthropic / Responses 兼容）
│   ├── runtime.py          # 进程级 singleton：从 auto_deploy.json 自动发现后端，
│   │                       # 把 handler+adapters+routing+transport 串起来；
│   │                       # 提供 dispatch() 给 app.py，提供状态接口给面板
│   ├── server.py           # 独立 FastAPI 入口（含完整中间件栈，可单独部署）
│   ├── handler.py          # 终端 handler：路由→协议编码→上游→响应解码
│   ├── transport.py        # httpx 上游传输（流式 + 非流式）
│   ├── metrics.py          # SQLite 指标存储 + 聚合查询
│   ├── vps_probe.py        # VPS 节点 TCP 探针
│   │
│   ├── adapters/           # 协议双向适配器
│   │   ├── base.py             # ProtocolAdapter / UpstreamCodec 接口
│   │   ├── openai_chat.py      # OpenAI Chat ⇄ IES（同时也是上游 codec）
│   │   ├── anthropic.py        # Anthropic Messages ⇄ IES（含 SSE 帧重组）
│   │   └── openai_responses.py # OpenAI Responses ⇄ IES
│   ├── core/               # 协议无关的核心
│   │   ├── context.py          # RequestContext + 决策追踪
│   │   ├── pipeline.py         # Middleware ABC + Pipeline 折叠
│   │   ├── errors.py           # GatewayError 家族
│   │   └── types.py            # InternalEvent (IES) 中间表示
│   ├── middleware/         # pipeline 洋葱（仅 server.py 启用）
│   │   ├── logging.py / timing.py / auth.py / rate_limit.py
│   ├── routing/            # 后端选择
│   │   ├── backend.py          # Backend：health/breaker/EWMA/in_flight
│   │   ├── registry.py         # BackendRegistry
│   │   ├── router.py           # 评分选择 score = lat*(1+inflight)/weight
│   │   ├── probe.py            # chat-style 健康探测
│   │   └── decision_log.py     # 路由决策审计
│   └── config/             # gateway.yaml 加载 + APIKeyStore
│       ├── loader.py
│       └── api_keys.py
│
├── templates/
│   ├── index.html          # 管理面板（受密码保护）
│   └── stats.html          # 公开统计页（无需登录）
│
├── scripts/
│   └── api-proxy.py        # 独立 asyncio 代理（零依赖，遗留参考）
│
├── tests/                  # pytest，180 用例
├── docs/                   # 设计笔记
├── data/                   # 运行时（已 gitignore）
│   ├── api_keys.db         # API key 存储
│   ├── metrics.db          # 请求指标
│   ├── auto_deploy.json    # 部署配置 + 后端发现源
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
- **后端自动发现**：`gateway.runtime` 启动时从 `data/auto_deploy.json` 读 enabled 账号，每个账号 → 一个 `http://127.0.0.1:{api_port}` 后端。改完配置可调 `POST /api/gateway/backends/reload` 热加载。
- **协议双向转换**：三种客户端协议都用 `InternalEvent` 中间表示双向编解码。Anthropic 的 `event: message_start / content_block_delta / message_stop` SSE 帧会正确生成。
- **评分路由**：`score = ewma_latency * (1 + in_flight) / weight`，更低更优。
- **熔断**：连续失败 3 次后冷却 30s。
- **健康探测**：30s 一次 chat probe（实打 `/v1/chat/completions`，max_tokens=1）。

`gateway/server.py` 是另一种部署方式——独立 FastAPI 进程 + 完整中间件栈（Bearer key store、限流、决策日志），从 `gateway.yaml` 静态读后端。app.py 不用它，但测试仍覆盖。

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
