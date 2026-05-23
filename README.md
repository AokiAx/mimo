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
- `MIMO_LOG_DIR`、`MIMO_LOG_RETENTION_DAYS`、`MIMO_LOG_LEVEL` — 应用日志目录、保留天数和日志级别
- `MIMO_PROXY_AUTH_TOKEN` — auto-deploy 部署到 ECS 的 `api-proxy.py` Bearer token（默认 `sk-Aoki-MiMo`，公网部署建议覆盖）
- `MIMO_DEPLOY_CHAT_PROBE_TIMEOUT` — Step 8 真实 chat 探测单次超时（默认 `240` 秒）
- `MIMO_DEPLOY_CHAT_PROBE_THINKING_BUDGET` — Step 8 `mimo-v2.5-pro` 探测的 thinking budget（默认 `8000`）
- `MIMO_DEPLOY_CHAT_PROBE_MAX_TOKENS` — Step 8 chat 探测最大输出 token（默认 `2048`）
- `MIMO_DEPLOY_CHAT_PROBE_MAX_ITERS` — Step 8 chat 探测最多重试次数（默认 `3`）
- `GATEWAY_DRAIN_TIMEOUT_S`、`GATEWAY_DEPLOY_DRAIN_GRACE_S`、`GATEWAY_ROTATION_LOOP_INTERVAL_S` — Gateway 后端 drain、部署切换等待和轮换循环间隔
- `GATEWAY_PROBE_TIMEOUT_S` — Gateway 后台非流式 chat 健康/热身探测超时（默认 `20` 秒）
- `GATEWAY_READINESS_STREAM_TIMEOUT_S` — Gateway 流式热身探测超时（默认跟随 `GATEWAY_PROBE_TIMEOUT_S`）
- `GATEWAY_READINESS_MODEL` — Gateway 健康/热身探测优先使用的模型（默认 `mimo-v2-flash`，后端未配置时回退到该后端第一个模型）
- `MIMO_REASONING_CACHE_DB` — reasoning / thinking 兜底缓存 SQLite 路径（默认 `data/reasoning_cache.db`）
- `MIMO_PROBE_DUMP` — 调试用：把 gateway 入站/出站请求追加写入 JSONL 文件
- `MIMO_TRUST_PROXY_HEADERS=1` — 信任 `X-Forwarded-For` 头作为客户端 IP（默认 **关闭**，用 socket peer 地址）。仅在面板部署在 nginx / Cloudflare / Caddy 等反向代理之后时启用，否则攻击者可以伪造 IP 来污染审计日志（`login_failure` / `auth_bad_cookie` / `path_traversal_blocked` 等事件）。

---

## 项目结构

```
mimo/
├── app.py                  # FastAPI 入口：管理面板、认证中间件、注册 gateway 路由
├── run.sh                  # 启动脚本
├── requirements.txt
│
├── claw/                   # MiMo 自动化
│   ├── mimo_auth.py        # 小米 SSO 登录 + Cookie 管理
│   ├── mimo_chat.py        # HTTP/SSE 对话客户端
│   ├── mimo_ws_client.py   # WebSocket 对话客户端
│   ├── auto_deploy.py      # 定时部署调度器
│   └── payload/
│       ├── api-proxy.py         # 部署到 ECS 的独立 aiohttp 代理
│       ├── ecs_finalize.sh      # Step 7 远端 finalize 脚本
│       ├── reverse-tunnel.sh    # API 反向隧道模板
│       └── tunnel-keepalive.sh  # API 隧道保活模板
│
├── gateway/                # API gateway（OpenAI / Anthropic / Responses 兼容）
│   ├── routes.py           # /v1/* 数据面、CORS、/health、/gateway/status
│   ├── auth.py             # Gateway Bearer / x-api-key / api-key 鉴权
│   ├── runtime.py          # 进程级 singleton：从 backends.json 读后端，
│   │                       # 把 handler+adapters+routing+transport 串起来；
│   │                       # 提供 dispatch() 给 app.py，提供状态接口给面板
│   ├── handler.py          # 终端 handler：路由→协议编码→上游→响应解码
│   ├── transport.py        # httpx 上游传输（流式 + 非流式）
│   ├── metrics.py          # SQLite 指标存储 + 聚合查询
│   ├── secrets_store.py    # 从 data/secrets.json 读凭证（支持环境变量覆盖）
│   ├── backend_store.py    # CRUD 持久化 data/backends.json
│   ├── model_groups_store.py # 模型分组与 exposed_name → native_model 映射
│   ├── reasoning_cache.py  # reasoning_content / thinking 持久化兜底缓存
│   ├── probe_registry.py   # VPS probe 节点与最新心跳
│   ├── probe_dump.py       # MIMO_PROBE_DUMP 请求调试输出
│   ├── logging_setup.py    # 应用日志轮转、tail、列表
│   │
│   ├── adapters/           # 协议双向适配器
│   │   ├── base.py             # ProtocolAdapter / UpstreamCodec 接口
│   │   ├── openai_chat.py      # OpenAI Chat ⇄ IES（同时也是上游 codec）
│   │   ├── anthropic.py        # Anthropic Messages 适配器（兜底/错误封装）
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
│   ├── model_groups.json   # 模型分组 / 映射配置
│   ├── reasoning_cache.db  # thinking/reasoning 兜底缓存
│   ├── probe_nodes.json    # VPS probe 节点和最新状态
│   ├── deploy_history/
│   └── deploy_logs/
├── logs/                   # 应用运行日志（可用 MIMO_LOG_DIR 覆盖）
└── accounts/               # Cookie 存储（已 gitignore）
    ├── _current.json       # 当前活跃账号
    └── <email>.json
```

---

## Gateway 架构

OpenAI Chat / Responses 走 IES 适配链路：

```
client request
   ↓
app.py /v1/{path:path}
   ↓ Bearer / x-api-key / api-key / 面板 cookie 鉴权
gateway.runtime.dispatch(adapter_name, request)
   ↓
GatewayHandler.handle(ctx, adapter, body)
   ├─ adapter.parse_request(body)          ← 客户端协议 → IES
   ├─ model_groups_store.resolve()         ← exposed model → native model
   ├─ Router.choose(model)                 ← 评分选后端
   ├─ HttpxTransport.post_json/post_stream ← 上游 MiMo OpenAI Chat
   ├─ codec.parse_upstream_*               ← 上游响应 → IES
   ├─ adapter.serialize_response_*         ← IES → 客户端协议
   └─ metrics + decision log
```

Anthropic Messages 单独走原生透传链路：`/v1/messages` 会映射模型、补回缺失 `thinking` 后直连上游 `/anthropic/v1/messages`，响应原样返回并顺手抓取新的 thinking 到缓存。

**关键特性**：
- **手动后端管理**：后端配置存 `data/backends.json`，通过管理面板 CRUD 或手动编辑。面板支持增删改查、启停、热加载。
- **凭证隔离**：密码和 token 存 `data/secrets.json`，首次运行自动生成。支持 `MIMO_PANEL_PASSWORD` / `MIMO_PUBLIC_API_TOKEN` / `MIMO_UPSTREAM_API_KEY` 环境变量覆盖；Gateway 还支持 `data/api_keys.db` 中的可撤销 API key 和模型白名单。
- **协议适配**：OpenAI Chat / Responses 用 `InternalEvent` 中间表示双向编解码。
- **Anthropic 原生透传**：`/v1/messages` 直连 MiMo `/anthropic/v1/messages`，保留 `thinking` content block、`signature` 和 beta header。
- **reasoning 兜底缓存**：Gateway 会按会话/工具调用缓存 OpenAI `reasoning_content` 与 Anthropic `thinking`，客户端丢字段时自动补回，降低多轮工具调用 400 风险。
- **模型映射**：`data/model_groups.json` 管理对外模型名到 MiMo 原生模型的映射；`/v1/models` 会优先返回映射后的 exposed model。
- **评分路由**：`score = ewma_latency * (1 + in_flight) / weight`，更低更优。
- **熔断**：连续失败 3 次后冷却 30s。`enabled` 开关可独立禁用后端。
- **主动 chat 测活**：默认每 30s 并发调用后端 `/v1/chat/completions`；连续失败会进入 detection zone，以 10s 间隔快速复检。用户请求只写 metrics，不更新后端健康评级，避免客户端请求格式、取消流或侧边异常把健康后端降级。
- **后端热身**：standby/warming 后端会做非流式、流式、tool-call 三类 `/v1/chat/completions` readiness 检查，通过后才加入 active 路由池。
- **生命周期保护**：部署替换时优先 drain 旧后端；没有可接管 active peer 时不会强行下线唯一可用后端。

---

## 自动部署流程

`claw/auto_deploy.py` 按账号调度部署，并把运行历史写入 `data/deploy_history/`，详细日志写入 `data/deploy_logs/`。

核心流程：
1. 检查端口冲突，通知 Gateway 准备替换。
2. 销毁旧 Claw，清理跳板机旧 SSH/API 隧道端口。
3. 创建新 Claw；遇到容量 429 会在预算内抖动重试。
4. 新开 Claw 会话发送账号部署文案，提取 ECS SSH 公钥；安全拒绝或未输出公钥时会换新会话原文重发。
5. 将 ECS 公钥写入跳板机 `authorized_keys`，通知 Claw 建立 ECS SSH 反向隧道。
6. Step 7 通过跳板机 SSH 进 ECS：先检查 bootstrap key、清理 known_hosts、探测 SSH 端口，再 `scp api-proxy.py` 并执行 `ecs_finalize.sh`。
7. Step 8 先探测 `/health`，再用 `mimo-v2.5-pro` 调 `/v1/chat/completions` 做真实模型链路验证；通过后才通知 Gateway 完成部署。

Step 7/8 都有分类重试，但缺少 bootstrap 私钥、payload 文件缺失、脚本格式错误这类不可恢复问题会直接失败并落 incident 日志。

## 主要 API 端点

### 兼容端点（gateway 提供，app.py 路由）
- `POST /v1/chat/completions` — OpenAI Chat
- `POST /v1/messages` — Anthropic Messages
- `POST /v1/responses` — OpenAI Responses
- `GET /v1/models` — OpenAI SDK 兼容模型列表（公开，用于 SDK 探测）
- 鉴权：`Authorization: Bearer sk-...`、`x-api-key` 或 `api-key`。兼容 `MIMO_PUBLIC_API_TOKEN`，也支持管理面板创建的 API key。

### 管理面板（密码保护）
- `GET /` — 主面板
- `GET /api/accounts` / `POST /api/account/*` — 账号管理
- `GET /api/auto-deploy/*` / `POST /api/auto-deploy/*` — 部署调度、状态、历史、手动触发和取消
- `GET /api/logs` / `GET /api/logs/tail` — 应用日志查看
- `GET /api/gateway/status` — 路由概览（uptime / qps / 后端数）
- `GET /api/gateway/backends` — 后端列表
- `POST /api/gateway/backends/{id}/toggle` — 启停某后端
- `POST /api/gateway/backends/{id}/activate` — 手动激活某后端
- `POST /api/gateway/backends/add` — 新增后端
- `POST /api/gateway/backends/{id}/update` — 更新后端
- `POST /api/gateway/backends/{id}/delete` — 删除后端
- `POST /api/gateway/backends/reload` — 重读 `data/backends.json`
- `GET /api/model-groups` / `POST /api/model-groups/*` — 模型组和模型映射管理
- `GET /api/probe/nodes` / `POST /api/probe/nodes/*` — 探针节点管理
- `GET /api/gateway/metrics/{summary|hourly|backends|status}` — 指标
- `GET /api/gateway/vps` / `POST /api/gateway/vps/refresh` — 节点状态

### 公开端点（无需登录）
- `GET /stats` — 公开统计页
- `GET /api/public/stats` — 公开统计 JSON
- `GET /health` — Gateway 进程健康
- `GET /gateway/status` — 公开 Gateway 状态
- `GET /probe/agent.py` / `GET /probe/install.sh/{token}` — 探针安装资源

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

## 测试

```bash
python -m pytest tests/ -q     # 全部
python -m pytest tests/test_metrics.py tests/test_lifecycle_rotation.py -v
```

---

## 依赖

- Python 3.8+
- `fastapi`、`uvicorn`、`jinja2`、`httpx`、`requests`、`websockets`、`croniter`、`pyyaml`

详见 `requirements.txt`。
