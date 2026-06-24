# MiMo Manager

小米 MiMo Studio (`aistudio.xiaomimimo.com`) 的自动化管理工具。纯 HTTP / WebSocket 实现，无需浏览器。

包含三块功能：
1. **Claw 自动化** — 登录、Cookie 管理、对话、SSH 反向隧道部署、账号池接力。
2. **API Gateway** — 把 MiMo 包装成 OpenAI / Anthropic 兼容的 API 端点。
3. **管理面板** — Web UI，查看账号、部署、流量、后端和密钥状态。

---

## 安装教程

### 环境要求
- Python **3.9+**（面板/网关）；Linux / macOS / Windows 均可
- `git`；如需浏览器登录还要 Chromium（Playwright 自动安装）

### 步骤

```bash
# 1. 克隆
git clone https://github.com/Aoki2008/mimo.git
cd mimo

# 2. 建虚拟环境（推荐）
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. 装依赖
pip install -r requirements.txt

# 4. 首次启动（自动生成 data/secrets.json：随机面板密码 + API token）
python app.py                    # 或 bash run.sh，默认端口 8088
```

打开 `http://localhost:8088`，用 `data/secrets.json` 里的 `panel_password` 登录。登录后在面板「🔑 密钥管理」页可查看 / 修改 / 轮换所有密钥（即时生效，无需重启）；被环境变量锁定的字段只读。

### 登录小米账号（纯 HTTP，无需浏览器；可加多个账号）

```bash
python claw/mimo_auth.py login          # 交互式输入邮箱/密码，支持邮箱验证码 2FA
# 或用环境变量免交互：MIMO_EMAIL=... MIMO_PASSWORD=... python claw/mimo_auth.py login
```

> 登录走小米 SSO 的纯 HTTP 流程。若小米风控触发 geetest 图形验证码（常见于陌生 IP/设备），headless 无法自动通过——换干净 IP/已知设备重试，或在浏览器登录后把 `.xiaomimimo.com` 域的 `serviceToken` / `userId` / `xiaomichatbot_ph` 手动存进 `accounts/<标签>.json` 的 `cookies` 数组。

### （可选）配置 SSH 自动部署
要把 Claw 自动部署成 API 转发节点，按下文「自动部署流程（SSH 反向隧道 · 方案 B）」配置 `data/config.json` 的 `ssh_targets` 节 + 目标机一次性 `setup-target.sh`。

> 状态数据接口（需独立 key）：`GET /api/public/status`，鉴权用 `data/secrets.json` 里的 `status_api_token`（独立于 API / 面板 token），供外部独立部署的状态页拉取。本项目不再内置公开状态页。


<details>
<summary>环境变量</summary>

- `MIMO_EMAIL`、`MIMO_PASSWORD` — 跳过交互输入
- `MIMO_PANEL_PASSWORD` — 覆盖面板密码
- `MIMO_PUBLIC_API_TOKEN` — 覆盖公开 API Bearer token
- `MIMO_STATUS_API_TOKEN` — 覆盖状态页接口 `/api/public/status` 的独立 key
- `MIMO_UPSTREAM_API_KEY` — 覆盖上游默认 API key
- `DISABLE_SCHEDULER=1` — 不启动 Claw activity / relay 自动部署循环（面板仍可手动触发部署）
- `DISABLE_CLAW_ACTIVITY=1` — 只关闭 Claw activity loop；Gateway 后台测活仍会启动
- `MIMO_PIN_IP` — 把 `aistudio.xiaomimimo.com` 钉到指定边缘 IP（过地域风控；REST+WS 全覆盖，SNI/Host 仍是域名故证书照常校验）
- `MIMO_DEBUG_CLAW=1` — auto_deploy 把 Claw 的 WS 回复**完整**写入日志（默认只记前 200 字符）
- SSH 部署本身的目标机 / 端口 / 管理私钥配置在 `data/config.json` 的 `ssh_targets` 节（见「自动部署流程」），不走环境变量。
- `MIMO_LOG_DIR`、`MIMO_LOG_RETENTION_DAYS`、`MIMO_LOG_LEVEL` — 应用日志目录、保留天数和日志级别
- `MIMO_CONFIG`、`MIMO_DB`、`MIMO_MODEL_GROUPS` — 覆盖统一配置、SQLite 和模型映射文件路径
- `GATEWAY_DRAIN_TIMEOUT_S`、`GATEWAY_DEPLOY_DRAIN_GRACE_S`、`GATEWAY_ROTATION_LOOP_INTERVAL_S`、`GATEWAY_MODEL_SYNC_INTERVAL_S` — Gateway 后端 drain、部署切换等待、维护循环和模型同步间隔
- `GATEWAY_PROBE_TIMEOUT_S` — Gateway 后台非流式 chat 健康探测超时（默认 `20` 秒）
- `MIMO_REASONING_CACHE_DB` — reasoning / thinking 兜底缓存 SQLite 路径（默认走统一库 `data/mimo.db`）
- `MIMO_PROBE_DUMP` — 调试用：把 gateway 入站/出站请求追加写入 JSONL 文件
- `MIMO_TRUST_PROXY_HEADERS=1` — 信任 `X-Forwarded-For` 头作为客户端 IP（默认 **关闭**，用 socket peer 地址）。仅在面板部署在 nginx / Cloudflare / Caddy 等反向代理之后时启用，否则攻击者可以伪造 IP 来污染审计日志（`login_failure` / `auth_bad_cookie` / `path_traversal_blocked` 等事件）。

</details>

---

## 项目结构

<details>
<summary>展开目录树</summary>

```
mimo/
├── app.py                  # FastAPI 入口：管理面板、认证中间件、注册 gateway 路由
├── run.sh                  # 启动脚本
├── requirements.txt
├── register_mimo.py        # 可选：小米账号邮箱注册脚本（需额外依赖）
│
├── claw/                   # MiMo 自动化
│   ├── mimo_auth.py        # 小米 SSO 登录 + Cookie 管理
│   ├── mimo_chat.py        # HTTP/SSE 对话客户端
│   ├── mimo_ws_client.py   # WebSocket 对话客户端
│   ├── auto_deploy.py      # SSH 反向隧道部署引擎
│   ├── claw_activity.py    # Claw 保活、修复、账号池 relay 与风险隔离
│   ├── register_mimo.py    # 可选：同根目录注册脚本副本
│   ├── payload/            # 注入 Claw 的数据面脚本
│   │   ├── api-proxy.py        # 本机 OpenAI/Anthropic 兼容代理（读 /proc 的 MiMo key）
│   │   ├── reverse-tunnel.sh   # autossh/ssh -R 反向隧道（占位符注入目标机）
│   │   └── tunnel-keepalive.sh # 纯本地探活看门狗
│   └── target/             # 目标机一次性配置（最小权限授权模型）
│       ├── setup-target.sh         # 建 tunnel 用户 + 安装授权器 + 写面板公钥
│       └── authorize-tunnel-key.sh # 强制命令授权器：把 claw 公钥 permitlisten 锁死写入
│
├── gateway/                # API gateway（OpenAI / Anthropic / Responses 兼容）
│   ├── routes.py           # /v1/* 数据面、CORS、/health、/gateway/status
│   ├── auth.py             # Gateway Bearer / x-api-key / api-key 鉴权
│   ├── runtime.py          # 进程级 singleton：从 config.json 读后端，
│   │                       # 把 handler+adapters+routing+transport 串起来；
│   │                       # 提供 dispatch() 给 app.py，提供状态接口给面板
│   ├── handler.py          # 终端 handler：路由→协议编码→上游→响应解码
│   ├── transport.py        # httpx 上游传输（流式 + 非流式）
│   ├── metrics.py          # SQLite 指标存储 + 聚合查询
│   ├── secrets_store.py    # 从 data/secrets.json 读凭证（支持环境变量覆盖）
│   ├── config_store.py     # data/config.json 统一配置：分节读写 + 旧文件迁移
│   ├── db.py               # data/mimo.db 统一 SQLite 路径 + 旧库迁移
│   ├── backend_store.py    # CRUD 持久化 config.json 的 backends 节
│   ├── model_groups_store.py # CRUD 持久化 model_groups.json 的模型映射
│   ├── reasoning_cache.py  # reasoning_content / thinking 持久化兜底缓存
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
│   │   ├── router.py           # 单 active 后端选择 + 决策记录
│   │   └── decision_log.py     # 路由决策审计
│   └── config/             # APIKeyStore（SQLite）
│       └── api_keys.py
│
├── templates/
│   └── index.html          # 管理面板（受密码保护）
│
├── tests/                  # pytest
├── data/                   # 运行时（已 gitignore）
│   ├── config.json         # 统一配置：backends / auto_deploy /
│   │                       #   panel_acl / ssh_targets / pin / mimo_control 分节
│   │                       #   （面板 CRUD 或手动编辑；参考 config.json.example）
│   ├── model_groups.json   # 模型分组与 exposed_name → native_model 映射
│   ├── secrets.json        # 面板密码 + API token（单独留；首次运行自动生成）
│   ├── mimo.db             # 统一 SQLite：api_keys / metrics / reasoning_cache 三表
│   ├── panel_tunnel_key    # SSH 部署：面板管理私钥（ed25519，自行生成）
│   ├── deploy_history/
│   └── deploy_logs/
├── logs/                   # 应用运行日志（可用 MIMO_LOG_DIR 覆盖）
└── accounts/               # Cookie 存储（已 gitignore，一账号一文件）
    ├── _current.json       # 当前活跃账号
    └── <label>.json
```

</details>

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
   ├─ Router.choose(model)                 ← 选择当前 active 后端
   ├─ HttpxTransport.post_json/post_stream ← 上游 MiMo OpenAI Chat
   ├─ codec.parse_upstream_*               ← 上游响应 → IES
   ├─ adapter.serialize_response_*         ← IES → 客户端协议
   └─ metrics + decision log
```

Anthropic Messages 单独走原生透传链路：`/v1/messages` 会映射模型、补回缺失 `thinking` 后直连上游 `/anthropic/v1/messages`，响应原样返回并顺手抓取新的 thinking 到缓存。

---

## 关键特性

<details>
<summary>展开特性列表</summary>

- **手动后端管理**：后端配置存 `data/config.json` 的 `backends` 节，通过管理面板 CRUD 或手动编辑。面板支持增删改查、启停、热加载。
- **凭证隔离**：密码和 token 存 `data/secrets.json`（单独保留），首次运行自动生成。支持 `MIMO_PANEL_PASSWORD` / `MIMO_PUBLIC_API_TOKEN` / `MIMO_UPSTREAM_API_KEY` 环境变量覆盖；Gateway 还支持 `data/mimo.db` 中的可撤销 API key 和模型白名单。
- **协议适配**：OpenAI Chat / Responses 用 `InternalEvent` 中间表示双向编解码。
- **Anthropic 原生透传**：`/v1/messages` 直连 MiMo `/anthropic/v1/messages`，保留 `thinking` content block、`signature` 和 beta header。
- **模型能力预检**：图片输入只允许 MiMo 官方支持图片理解的 `mimo-v2.5` / `mimo-v2-omni`，其他模型在 Gateway 层直接返回 400，避免上游 404 刷屏。
- **reasoning 兜底缓存**：Gateway 会按会话/工具调用缓存 OpenAI `reasoning_content` 与 Anthropic `thinking`，客户端丢字段时自动补回，降低多轮工具调用 400 风险。
- **模型映射**：`data/model_groups.json` 管理对外模型名到 MiMo 原生模型的映射；`/v1/models` 会优先返回映射后的 exposed model。
- **openclaw p4 适配**：WebSocket operator 协议协商到 `maxProtocol=4`，默认会话键使用 `agent:main:main`，兼容 2026.5.27 之后的 Claw 事件。
- **三池账号模型**：activity loop 会把账号分为可用池、当天创建冷却池、风控风险池；`bannedStatus != NOT_BANNED` 的账号会被隔离，24h 后复检恢复。
- **账号接力部署**：MiMo 免费 Claw 约 4h TTL 且每账号每北京时间日只允许创建一次，系统不再对单账号做 24/7 轮换，而是在 TTL 前约 30 分钟冷启动下一个可用账号。
- **单 active 后端**：Gateway 同一时刻只让一个后端接新流量；部署完成的新后端会直接成为 active，旧 active 进入 draining。
- **熔断**：连续失败 3 次后冷却 30s。`enabled` 开关可独立禁用后端。
- **主动 chat 测活**：默认每 30s 并发调用后端 `/v1/chat/completions`；连续失败会进入 detection zone，以 10s 间隔快速复检。用户请求只写 metrics，不更新后端健康评级，避免客户端请求格式、取消流或侧边异常把健康后端降级。
- **SSH 反向隧道后端**：Claw 上的 `api-proxy.py` 经 `ssh -R` 把本机代理暴露到你指定目标机的 loopback 端口，Gateway 把该账号后端路由到 `http://<目标机>:<端口>`（普通 http 后端，直接转发）。详见下文「自动部署流程」。
- **生命周期保护**：部署替换时先 drain 待替换后端；部署完成后直接激活新后端，并让其它 active 后端退出新流量。
- **请求日志**：每条请求记入 `data/mimo.db` 的 `requests` 表（含模型、prompt/completion tokens、延迟、后端、状态、错误），面板「请求日志」页带**模型列**和**分页**查看（`?offset=&limit=`）。

</details>

---

## 自动部署流程（SSH 反向隧道 · 方案 B）

`claw/auto_deploy.py` 负责单次部署执行，`claw/claw_activity.py` 负责保活、修复、账号池 relay 和风险隔离。运行历史写入 `data/deploy_history/`，详细日志写入 `data/deploy_logs/`。

数据面：claw 上跑 `api-proxy.py`（读网关进程 `/proc` 里的 MiMo key，监听 `127.0.0.1:18800`，OpenAI/Anthropic 兼容）+ `reverse-tunnel.sh`（`ssh -R` 反向隧道，把代理暴露到**你指定的目标机** loopback 端口）。Gateway 直接把该账号后端路由到 `http://<目标机>:<端口>`。

当前 free-tier 生命周期按 openclaw 2026.5.27 行为处理：单个 Claw 约 4 小时 TTL，每账号每北京时间日约 1 次 create 额度。activity loop 会优先维持健康账号池：可用账号提前接力，已创建账号进入当天冷却，触发风控的账号进入 risk 池并定期复检。

<details>
<summary>部署步骤详解</summary>

1. 通知 Gateway 准备替换（drain 旧后端）。
2. 销毁旧 Claw（若有）。
3. 创建新 Claw；遇容量 429 在预算内抖动重试；遇 `7001`（当天免费创建额度已用完）直接停止并记录。
4. 等待 Claw `AVAILABLE`。
5. **中和提示词（确定性）**：用 operator `agents.files.set` 直写精简 `SOUL.md`/`AGENTS.md`，去掉默认会拒绝自管运维的"安全 CoT"。这步是网关直写、**不经 agent、不会被拒**，把后续注入从概率性变确定性。
6. **SSH 引导**：把 `api-proxy.py` / `reverse-tunnel.sh`（已注入目标机信息）/ `tunnel-keepalive.sh` 经 `genUploadInfo` 上传到 MiMo FDS，用受信任的 `<mimo-files>` 附件让 claw `curl` 下载（避开内联大小限制与下载拒绝），claw 装依赖、生成 `ed25519` 隧道密钥、起代理与隧道，并**回传公钥**。
7. **授权公钥**：面板用管理私钥（`data/panel_tunnel_key`）经目标机上的**强制命令授权器**把该公钥以 `restrict,permitlisten="127.0.0.1:<端口>"` 锁死写入——即便此 claw 泄露，该 key 也只能开这一个反向端口，无 shell、无其它转发。
8. 轮询目标机 `http://<上游>:<端口>/health` 就绪后，登记/刷新该账号后端，交给 Gateway 重载并切为唯一 active。

每账号一台目标机 + 一个独立端口（端口自动分配并持久化）。缺目标机配置 / 缺管理私钥 / payload 缺失 / Claw 安全拒绝等会失败并落 incident 日志。

面板还提供 `restart` / `repair` / `reset` 三个 openclaw lifecycle 操作。它们会在同一 4h 窗口内让 MiMo 侧生成新的空白 Claw 状态，操作后通常仍需要重新走 SOUL/AGENTS 中和和隧道 bootstrap；额度影响依上游实际行为为准。
</details>

<details>
<summary>目标机一次性配置 + config.json 的 ssh_targets 节</summary>

**1. 目标机（你的中转机器）跑一次**（创建 `tunnel` 用户 + 安装强制命令授权器 + 写入面板管理公钥）：

```bash
# 先在面板机生成管理密钥
ssh-keygen -t ed25519 -f data/panel_tunnel_key -N ''
# 把公钥拷到目标机，在目标机上：
sudo ./claw/target/setup-target.sh "$(cat panel_tunnel_key.pub)"
# 确保 sshd: AllowTcpForwarding remote(或 yes); GatewayPorts no（转发仅绑 loopback）
```

**2. 配置 `data/config.json` 的 `ssh_targets` 节**（面板「目标机 / 中转机配置」页可视化增删 targets / 账号分配 / 全局私钥路径，或手动编辑；参考 `data/config.json.example`）：

```json
{
  "ssh_targets": {
    "panel_key_path": "data/panel_tunnel_key",
    "default_target": "vps1",
    "targets": {
      "vps1": {"host": "1.2.3.4", "ssh_port": 22, "tunnel_user": "tunnel",
               "upstream_host": "127.0.0.1", "port_range": [19080, 19980]}
    },
    "assignments": {}
  }
}
```

- `host`/`ssh_port`/`tunnel_user`：claw 反连的目标机；`upstream_host`：Gateway 访问转发端口的地址（网关与目标机同机时为 `127.0.0.1`）。
- `assignments`：留空即可，部署时自动给账号分配并持久化端口；也可手动 `{"账号名": {"target": "vps1", "remote_api_port": 19080}}`。
- claw 上的隧道脚本优先用 `autossh`，装不上则自动退回纯 `ssh + while` 重连循环（claw 上 apt 常不通）。
- 代理无认证：仅绑 loopback、只经反向隧道在目标机 loopback 暴露，**请确保目标机为你专用**（否则其本地进程可白嫖额度）。
</details>

## 主要 API 端点

<details>
<summary>展开端点列表</summary>

### 兼容端点（gateway 提供，app.py 路由）
- `POST /v1/chat/completions` — OpenAI Chat
- `POST /v1/audio/speech` — OpenAI Audio Speech 兼容 TTS（映射到 MiMo TTS 模型）
- `POST /v1/messages` — Anthropic Messages
- `POST /v1/responses` — OpenAI Responses
- `GET /v1/models` — OpenAI SDK 兼容模型列表（公开，用于 SDK 探测）
- 鉴权：`Authorization: Bearer sk-...`、`x-api-key` 或 `api-key`。兼容 `MIMO_PUBLIC_API_TOKEN`，也支持管理面板创建的 API key。

### 管理面板（密码保护）
- `GET /` — 主面板
- `GET /api/accounts` / `POST /api/account/*` — 账号管理
- `GET /api/account/{filename}/claw/status` / `POST /api/account/{filename}/claw/{create|destroy|refresh|restart|repair|reset}` — openclaw lifecycle 操作（操作后通常需要重新部署隧道）
- `GET /api/auto-deploy/*` / `POST /api/auto-deploy/*` — 部署调度、状态、历史、手动触发和取消
- `GET /api/claw-activity/status` — activity loop、账号池和 relay 状态
- `GET /api/ssh-targets` / `POST /api/ssh-targets` — SSH 反向隧道部署的目标机配置（targets / 账号 assignments / 全局管理私钥路径），面板「目标机 / 中转机配置」页可视化增删改
- `GET /api/panel-acl` / `POST /api/panel-acl` — 面板 IP allowlist
- `GET /api/secrets` / `POST /api/secrets` / `POST /api/secrets/rotate` — 面板密码、API token、状态页 key 和上游 key 管理
- `GET /api/logs` / `GET /api/logs/tail` — 应用日志查看
- `GET /api/gateway/status` — 路由概览（uptime / qps / 后端数）
- `GET /api/gateway/backends` — 后端列表
- `POST /api/gateway/backends/{id}/toggle` — 启停某后端
- `POST /api/gateway/backends/{id}/activate` — 手动激活某后端
- `POST /api/gateway/backends/add` — 新增后端
- `POST /api/gateway/backends/{id}/update` — 更新后端
- `POST /api/gateway/backends/{id}/delete` — 删除后端
- `POST /api/gateway/backends/reload` — 重读 `data/config.json` 的 backends 节
- `GET /api/model-groups` / `POST /api/model-groups/*` — 模型组和模型映射管理
- `GET /api/gateway/metrics?offset=&limit=` — 请求日志 + 指标汇总；`recent` 列表分页（每条含 `model` / tokens / 延迟 / 后端 / 状态，`limit` 1–200，返回 `total_records`）
- `GET /api/gateway/metrics/{hourly|backends|status}` — 分时直方图 / 各后端统计 / 路由状态

### 公开端点
- `GET /api/public/status` — 状态页数据 JSON（**需独立 `status_api_token`**：`Authorization: Bearer <token>` / `X-Status-Key` / `?key=`），返回脱敏聚合（总量、成功率、tokens、状态码分布、延迟/TTFT 分位、带可用性状态的 48h 分时、Top routes/models、流式/非流式计数、operational、在线后端数；不含后端身份）
- `GET /health` — Gateway 进程健康
- `GET /gateway/status` — 公开 Gateway 状态

</details>

---

## CLI 命令（claw）

```bash
python claw/mimo_auth.py status         # 查看 Cookie 状态
python claw/mimo_auth.py login          # 交互式登录
python claw/mimo_auth.py cookie-header  # 输出 Cookie Header
python claw/mimo_auth.py auto-refresh   # 自动续期（cron 友好）

python claw/mimo_chat.py "你好"         # 单轮对话
python claw/mimo_ws_client.py "你好"    # WebSocket 对话

# 可选：注册新小米账号（额外依赖 curl_cffi pycryptodome）
python register_mimo.py --email X --password Y
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

- Python 3.9+
- `fastapi`、`uvicorn`、`jinja2`、`httpx`、`requests`、`websockets`、`croniter`、`pyyaml`
- 可选注册脚本：`curl_cffi`、`pycryptodome`

详见 `requirements.txt`。
