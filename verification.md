# 验证报告

- 日期：2026-05-19
- 执行者：Codex
- 范围：MiMo Studio gateway 生产错误日志审查、本地代码修复、Claw 轮换安全重构与自动化验证

## 结论

本地修复已覆盖生产 `error.log` 中最高频和可归因的错误组，并额外重构了 Claw/backend 轮换安全策略：没有可接管的健康后端时，自动部署会跳过销毁旧 Claw，避免在新 Claw 创建的 5-10 分钟内主动制造 `no backend` 窗口。云端仅用于只读拉取日志，没有修改远程代码。

## 生产问题与修复映射

| 生产日志问题 | 修复位置 | 验证 |
|---|---|---|
| `reasoning_content` 在 thinking 模式多轮工具调用中缺失，导致 400 | `gateway/adapters/openai_chat.py`, `gateway/handler.py` | `tests/test_openai_chat.py` |
| 上游 `401 Invalid API Key` 未作为 backend 故障处理 | `gateway/handler.py` | `tests/test_handler.py::test_non_stream_401_marks_backend_failure_and_retries` |
| Anthropic passthrough `tool_choice` 传入字符串导致 400 | `gateway/anthropic_passthrough.py`, `gateway/handler.py` | `tests/test_handler.py::test_anthropic_passthrough_normalizes_string_tool_choice` |
| TTS 请求缺少 assistant role 导致上游 400 | `gateway/handler.py` | `tests/test_handler.py::test_tts_requests_are_rejected_before_upstream` |
| 图片输入路由到非视觉模型导致 404 | `gateway/handler.py` | `tests/test_handler.py::test_image_requests_are_rejected_for_non_vision_models` |
| warming backend 与探针失败状态影响路由稳定性 | `gateway/routing/backend.py`, `gateway/routing/router.py`, `gateway/runtime.py` | `tests/test_lifecycle_rotation.py`, `tests/test_routing.py` |
| 自动部署在无接管后端时销毁唯一 Claw，造成 no backend 空窗 | `gateway/runtime.py`, `claw/auto_deploy.py` | `tests/test_auto_deploy_safety.py`, `tests/test_lifecycle_rotation.py` |

## Claw 轮换机制调整

- 默认 backend 轮换年龄从 50 分钟改为 40 分钟，可通过 `GATEWAY_ROTATION_INTERVAL_S` 覆盖。
- `prepare_account_deploy()` 只允许由当前可选的同模型 backend 接管；处于 detection、dead、breaker open、disabled、draining、未 ready warming 或饱和状态的 peer 不再视为可接管。
- `auto_deploy` 在 `safe_to_destroy=False` 时将本次部署标记为 `skipped` 并直接退出，不进入 Step 0 销毁旧 Claw。
- `skipped` 已加入部署终态，避免安全跳过后调度器/UI 误判为仍在运行。

## 全量 Active Claw 自适应轮换

- 所有 readiness 通过且健康的 backend 保持 `active/selectable`，由 Router 继续按 EWMA latency、in-flight、weight 与失败率惩罚做负载均衡，不再因为存在多个 peer 就主动放入 standby。
- `claw.auto_deploy` 新增全局 coordinator，每分钟按 enabled 账号数、active 可接管账号数、部署中账号数和账号 age 统一选择轮换批次。
- 轮换阈值为 40/50/55 分钟：40 分钟进入候选，50 分钟进入紧急优先级，55 分钟标记 hard expiry；容量不足时告警/跳过而不是制造 `no backend`。
- 当前 6 个账号时，正常下限为 5 个 active，紧急下限为 4 个 active；正常最多轮换 1 个，多个账号超过 50 分钟时最多并发轮换 2 个。
- 3 个账号时允许轮换 1 个，但必须剩余 2 个可接管；9 个账号时正常下限 8，紧急下限 6。
- 手动触发和 scheduler 触发都先经过 coordinator 容量门；真正销毁旧 Claw 前仍由 `prepare_account_deploy()` 做最后的 gateway 安全确认。
- `/api/auto-deploy/status` 与面板展示新增 active 数、enabled 数、deploying 数、normal/emergency min active、每账号 age、轮换原因与 `skipped_capacity`/`skipped_unmatched`/`queued` 等可见状态。

## 执行命令

```powershell
python -m pytest tests/test_lifecycle_rotation.py -q
python -m pytest tests/test_auto_deploy_safety.py -q
python -m pytest tests/test_auto_deploy_safety.py tests/test_lifecycle_rotation.py tests/test_routing.py -q
python -m pytest tests/ -q
git diff --check
```

最新结果：

```text
57 passed in 1.02s
261 passed, 4 warnings in 4.36s
git diff --check 通过，仅有 Windows LF/CRLF 提示
```

## 风险与后续

- 单账号、单 Claw 无法做到无感续命：如果上游不允许在同账号旧 Claw 存活时创建新 Claw，代码只能选择保留旧 Claw并跳过危险销毁，不能凭空产生接管容量。
- 要彻底避免 `no backend mimo-v2.5-pro`，生产上至少需要多个可用账号/后端同时覆盖 `mimo-v2.5-pro`；当前 6 账号目标状态是 6 个 healthy active，正常轮换期间通常保持 5 个可接管。
- FastAPI `on_event` 弃用警告不是本次缺陷路径，后续可单独迁移到 lifespan。
