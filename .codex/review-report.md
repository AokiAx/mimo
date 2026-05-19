# 审查报告

- 日期：2026-05-19
- 审查者：Codex
- 任务：生产环境 gateway 多缺陷修复与全量 Active Claw 自适应轮换审查
- 证据来源：本地代码 diff、pytest、只读生产 `error.log`

## 评分

- 需求符合性：29/30
- 技术质量：29/30
- 集成兼容性：19/20
- 性能与可扩展性：19/20
- 综合评分：96/100
- 建议：通过

## 关键发现

1. reasoning cache 修复覆盖了生产最高频错误。
   `gateway/adapters/openai_chat.py` 现在支持 scoped key 与 fallback key 双写/回查；当客户端下轮请求剥离 `thinking` 字段时，仍能按同一消息前缀和工具面恢复 MiMo 要求的 `reasoning_content`。

2. backend 健康状态处理更符合生产语义。
   `gateway/handler.py` 保持普通上游 4xx 不污染 backend 健康，但将 `401 Invalid API Key` 视为 backend 配置故障并触发重试，避免失效账号持续接流量。

3. 请求形态错误在 gateway 边界被提前阻断。
   TTS 缺少 assistant role、非视觉模型接收图片输入、Anthropic `tool_choice` 字符串简写，均在本地适配层处理，减少上游 400/404 噪声。

4. readiness 与路由策略更稳。
   warming backend 只有 readiness 成功后才可选择；探针失败单独计数；routing score 对已有足够样本的高失败率 backend 增加惩罚。

5. Claw 轮换不再主动制造无后端窗口。
   `claw/auto_deploy.py` 会在没有可接管健康后端时跳过销毁旧 Claw；`gateway/runtime.py` 将默认轮换年龄调至 40 分钟，并用 selectable peer 而不是单纯 active peer 判断接管能力。

6. 全量 Active Claw 策略已落地。
   新增 coordinator 按 40/50/55 分钟阈值、enabled 账号数、active 可接管账号数与部署中数量选择轮换批次；6 账号正常保持 5 个可接管，紧急至少 4 个可接管。手动触发和 scheduler 触发都走同一容量门，状态接口和面板展示 `skipped_capacity`、`skipped_unmatched`、`queued` 等原因。

## 审查清单

- 需求字段完整性：通过。目标、范围、验证方式、风险均已记录。
- 原始意图覆盖：通过。生产日志前 10 个错误组中的可代码修复项均有对应改动，且已覆盖用户补充的 6 账号全量负载均衡轮换要求。
- 交付物映射：通过。代码、测试、面板状态展示、`.codex/testing.md`、`verification.md` 均已生成。
- 依赖与风险评估：通过。未引入新依赖，主要残余风险是部署后生产观测与失效 key 更换。
- 留痕：通过。远程日志只读来源、测试命令、结果和清理动作均已记录。

## 验证结果

```text
python -m pytest tests/ -q
261 passed, 4 warnings in 4.36s
```

```text
python -m pytest tests/test_auto_deploy_safety.py tests/test_lifecycle_rotation.py tests/test_routing.py -q
57 passed in 1.02s
```

```text
git diff --check
通过，无 whitespace error；仅 Windows LF/CRLF 提示。
```

## 风险与阻塞

- 无阻塞。
- 未修改云端代码；仅通过 GitHub PR 分支交付本地代码变更。
- 已删除本地 `.scratch_remote_logs/id_ed25519` 临时私钥副本。
- `AGENTS.md` 与 `CLAUDE.md` 为进入本轮前已存在的未跟踪文件，本轮未纳入处理。
