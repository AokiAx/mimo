# 测试记录

- 日期：2026-05-19
- 执行者：Codex
- 任务：生产环境 gateway 缺陷审查与修复

## 生产日志输入

- 只读来源：`root@149.88.90.137:/root/mimo/logs/error.log`
- 本地原始副本：`C:\Users\sikuai\Desktop\develop\mimo\.scratch_remote_logs\20260519_143202_149.88.90.137_error.log_raw_error.log`
- 本地摘要：`C:\Users\sikuai\Desktop\develop\mimo\.scratch_remote_logs\20260519_143202_error_analysis.md`
- 时间范围：2026-05-19 00:04:54 至 2026-05-19 13:42:44
- 统计：284 个错误块，12 类唯一签名；HTTP 400=243，401=38，404=3

## 关键验证命令

```powershell
python -m pytest tests/ -q
```

结果：

```text
251 passed, 4 warnings in 4.39s
```

警告均为 FastAPI `on_event` 弃用警告，位于 `app.py:147` 与 `app.py:179`，不属于本次 gateway 缺陷修复路径。

```powershell
git diff --check
```

结果：通过，无 whitespace error。PowerShell 输出了 Git 在 Windows 上的 LF/CRLF 提示，不影响 diff 校验。

## 回归覆盖

- reasoning cache 在 OpenAI Chat thinking 字段被客户端剥离后的回填。
- OpenAI Chat reasoning cache 的不同 thinking 配置隔离。
- 上游 401 Invalid API Key 标记 backend failure 并重试下一 backend。
- 普通上游 400 仍不污染 backend 健康状态。
- Anthropic passthrough `tool_choice` 字符串简写归一化。
- TTS 请求在上游前拒绝无 assistant role 的错误消息形态。
- 非视觉模型在上游前拒绝图片输入。
- warming backend 仅在 readiness 成功后可参与路由。
- backend routing score 对高失败率 backend 增加惩罚。
- probe failure 与流量失败计数分开跟踪。
- 自动部署在没有可接管后端时跳过销毁旧 Claw，避免创建新 Claw 的 5-10 分钟内出现 no backend 空窗。
- 默认轮换时间改为 40 分钟，给 1 小时上游硬断前留出更保守的创建与热身窗口。

## 2026-05-19 全量 Active Claw 自适应轮换验证

新增验证命令：

```powershell
python -m pytest tests/test_auto_deploy_safety.py tests/test_lifecycle_rotation.py tests/test_routing.py -q
python -m pytest tests/ -q
git diff --check
```

结果：

```text
57 passed in 1.02s
261 passed, 4 warnings in 4.36s
git diff --check 通过，仅有 Windows LF/CRLF 提示
```

新增回归覆盖：

- 3/6/9 个账号的 desired active、normal/emergency min active 与并发轮换上限。
- 6 个账号正常 40 分钟轮换只选择 1 个，剩余 5 个可接管。
- 6 个账号多个超过 50 分钟时进入紧急模式，最多选择 2 个且剩余不少于 4 个。
- 健康 active 不足紧急下限时记录 `skipped_capacity`，不触发销毁。
- 启用账号缺少 gateway backend 时记录 `skipped_unmatched`。
- 3 个账号场景允许轮换 1 个，但必须剩余 2 个可接管。
- scheduler 每分钟只触发 coordinator 选中的账号。
- 手动触发部署也先经过 coordinator 容量门，并在启动线程前登记 `queued` 状态。

## 清理

- 已删除本地临时私钥副本：`C:\Users\sikuai\Desktop\develop\mimo\.scratch_remote_logs\id_ed25519`
- 未修改云端代码。
