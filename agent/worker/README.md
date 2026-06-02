# MiMo claw-worker

在**中国大陆 IP** 的设备(NAS / 小主机)上,用 Docker 跑这一个小服务,专门负责
MiMo 里**被地域限制**的两件事:

1. **创建 claw**
2. **与 claw WS 对话**:模板重置 + **注入 ws-bridge**(让 claw 主动连回面板的 `/ws`)

其余所有事(派任务、校验 bridge 节点是否上线、热身、UI、调度)都在**主控面板**侧。
不再有跳板机 / SSH / api-proxy —— claw 通过出站 WebSocket 主动连回面板。

## 为什么要它

MiMo 的 ws-proxy 网关按**客户端源 IP 归属地**做准入:非大陆 IP 的 `chat.send` 会在
网关被拦截、替换成固定话术(根本不进 openclaw agent)。REST(建/查/ticket)任何 IP
都通,只有 WS 对话内容被卡。所以把这部分放到大陆 IP 的设备上跑。

## 边界

- **无本地状态**:cookies、注入提示词、派哪个账号都由面板**按任务下发**;镜像里不含任何密钥或 payload。
- **只出站**:worker 只对面板发起 HTTPS(长轮询 + 回报),NAT 后即可,无需公网/端口转发。
- bridge 脚本由面板渲染好(已把 `wss://域名/ws?account=该账号&token=...` 烧进去)随任务下发,worker 只负责让 claw 把它跑起来。

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

> 面板侧需设 `MIMO_WS_PUBLIC_URL`(如 `wss://panel.example/ws`),否则面板不会下发任务(bridge 无处可连)。

## 配置项(env)

| 变量 | 必填 | 说明 |
|---|---|---|
| `PANEL_URL` | ✅ | 面板地址,如 `https://panel.example` |
| `WORKER_TOKEN` | ✅ | 面板签发的 worker token |
| `WORKER_NAME` | | 面板显示名(默认 hostname) |
| `POLL_INTERVAL` | | 一次空轮询后的等待秒数,默认 5(配合面板长轮询近实时取任务) |
| `CLAW_CREATE_BUDGET` | | 建 claw 最长重试秒数,默认 600 |
| `BRIDGE_VERIFY_BUDGET` | | 等 bridge 节点连回 `/ws` 的最长秒数,默认 240 |
| `MIMO_PROXY` | | 本机非大陆出口时,指向同网段大陆出口 SOCKS |
| `VERIFY_TLS` | | 面板自签证书时设 `0` |

## 协议(worker ↔ 面板)

单端点 `POST {PANEL_URL}/api/claw-worker/sync`,头 `X-Worker-Token`。
每次请求都带心跳 `worker`(name/version/egress 出口归属地)。`phase` 区分意图:

| phase | worker 发 | 面板回 |
|---|---|---|
| `poll` | — | **长轮询**:hold 住直到有任务或 ~25s。`{action:"idle"}` 或 `{action:"deploy", job:{job_id,account,cookies,reset_message,inject_prompt}}` |
| `verify` | `{job_id, log}` | `{ok:true, connected:bool}`(该账号 bridge 节点是否已连回 `/ws`) |
| `report` | `{job_id, status, log}` | `{ok:true}`(done 触发热身 / 失败释放后端) |

- 面板凭 `egress.cn` 决定是否派 claw 任务(出口非大陆则不派,避免被风控)。
- `verify` 与 `report` 都带累计 `log`,面板把增量行实时并入该账号的部署日志(面板"立即部署/日志"可见)。
- **面板指挥**:面板"立即部署"把账号入队,长轮询让 worker **近实时**取到;无需面板反向连 worker。

## 本地调试(不进容器)

```bash
pip install -r requirements.txt
PANEL_URL=https://panel.example WORKER_TOKEN=xxx python worker.py
```

