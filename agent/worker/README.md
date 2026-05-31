# MiMo claw-worker

在**中国大陆 IP** 的设备(NAS / 小主机)上,用 Docker 跑这一个小服务,专门负责
MiMo 里**被地域限制**的两件事:

1. **创建 claw**
2. **与 claw WS 对话**(发部署文案、拿 ECS 公钥、通知 claw 建反向隧道)

其余所有事(加公钥到跳板机、SSH 进 claw、finalize、验证、UI、调度)都在**主控面板**侧。

## 为什么要它

MiMo 的 ws-proxy 网关按**客户端源 IP 归属地**做准入:非大陆 IP 的 `chat.send` 会在
网关被拦截、替换成固定话术(根本不进 openclaw agent)。REST(建/查/ticket)任何 IP
都通,只有 WS 对话内容被卡。所以把这部分放到大陆 IP 的设备上跑。

## 边界

- **无本地状态**:cookies、部署文案、派哪个账号都由面板**按任务下发**;镜像里不含任何密钥。
- **只出站**:worker 只对面板发起 HTTPS(轮询 + 回报),NAT 后即可,无需公网/端口转发。
- 与面板的协议见下方"协议"。

## 部署

```bash
cp .env.example .env
# 编辑 .env:PANEL_URL、WORKER_TOKEN(面板"Claw Workers"页签发)
docker compose up -d
docker compose logs -f
```

升级:

```bash
docker compose pull && docker compose up -d   # 无本地状态,可随时回滚到旧 tag
```

镜像:`ghcr.io/aoki2008/mimo-claw-worker`(多架构 amd64/arm64)。

## 配置项(env)

| 变量 | 必填 | 说明 |
|---|---|---|
| `PANEL_URL` | ✅ | 面板地址,如 `https://panel.example` |
| `WORKER_TOKEN` | ✅ | 面板签发的 worker token |
| `WORKER_NAME` | | 面板显示名(默认 hostname) |
| `POLL_INTERVAL` | | 空闲轮询秒数,默认 60 |
| `CLAW_CREATE_BUDGET` | | 建 claw 最长重试秒数,默认 600 |
| `MIMO_PROXY` | | 本机非大陆出口时,指向同网段大陆出口 SOCKS |
| `VERIFY_TLS` | | 面板自签证书时设 `0` |

## 协议(worker ↔ 面板)

单端点 `POST {PANEL_URL}/api/claw-worker/sync`,头 `X-Worker-Token`。
每次请求都带心跳 `worker`(name/version/egress 出口归属地)。`phase` 区分意图:

| phase | worker 发 | 面板回 |
|---|---|---|
| `poll` | — | `{action:"idle"}` 或 `{action:"deploy", job:{job_id,account,deploy_text,cookies,ssh_port,api_port,notify_text}}` |
| `claw_ready` | `{job_id, public_key, log}` | `{ok:true}` (已加 key+清端口) 或 `{ok:false,error}` |
| `notified` | `{job_id, log}` | `{ok:true, detail}` (已 ssh 进 claw+finalize+验证) 或 `{ok:false,error}` |
| `report` | `{job_id, status, log}` | `{ok:true}` |

面板凭 `egress.cn` 决定是否派 claw 任务(出口非大陆则不派,避免被风控)。

## 本地调试(不进容器)

```bash
pip install -r requirements.txt
PANEL_URL=https://panel.example WORKER_TOKEN=xxx python worker.py
```
