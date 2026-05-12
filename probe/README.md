# MiMo VPS Probe

单文件 Python 探针，10s 上报 CPU/内存/磁盘/网络/load/uptime 到面板。

## 部署

```bash
# 1. 拷贝
sudo mkdir -p /opt/mimo-probe
sudo cp agent.py /opt/mimo-probe/

# 2. 写 systemd unit（改里面的 URL / TOKEN / NAME）
sudo cp mimo-probe.service /etc/systemd/system/
sudo nano /etc/systemd/system/mimo-probe.service

# 3. 启动
sudo systemctl daemon-reload
sudo systemctl enable --now mimo-probe
sudo systemctl status mimo-probe
journalctl -u mimo-probe -f
```

## 调试（前台跑）

```bash
python3 agent.py \
  --url https://panel.example/api/probe/report \
  --token <token> \
  --name vps-tokyo
```

## 要求

- Python 3.6+（标准库 only，无 pip 依赖）
- Linux（读 `/proc/*`）
- 能 HTTPS 访问面板
