#!/usr/bin/env python3
"""
账号号池 — 统一分类（单一事实源）
================================

两层别混：

┌─ A. 库存层（文件在哪）─────────────────────────────────
│  active   accounts/<email>.json     主池文件
│  archive  accounts/_archive/        死号/legacy 归档
│
└─ B. 部署调度层（config auto_deploy.accounts + 运行时）──
   互斥主标签 deploy_pool（面板徽章用）:

   risk       risk_blocked（ban / create_gate）
   dead       ck 探活失败（401 等）— 不能再部署
   disabled   enabled=false 且非 risk
   serving    已有 healthy+active 后端（正在扛流量/有 Claw）
   cooldown   今日北京时间已 create（无额度），暂不能再 create
   available  enabled + ck 活 + 未 risk + 未冷却 + 尚无 active 后端
              → 可被 activity relay 选中去 create

另有布尔位（可与主标签并存语义）:
   can_create   是否可被 _available_for_create 选中
   is_serving   是否有 healthy active backend
   ck_live      mi/get 是否成功
   in_cooldown  今日是否已 create
   risk_blocked 是否在风控隔离

自动注册号策略（当前运营）:
   - 只养 auto_reg 库存（tempmail 注册）
   - ck 死 → 归档 + 补新号（不重登）
   - 与 deploy 三池正交：补号后 enabled 才能进 available

用法:
  python claw/account_pool.py status
  python claw/account_pool.py status --json
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS = ROOT / "accounts"
ARCHIVE = ROOT / "accounts" / "_archive"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "claw"))

_BEIJING = timezone(timedelta(hours=8))

# domain markers for auto-reg free pool (same as ck_lifecycle)
_AUTO_DOMAIN_MARKERS = (
    "airfryersbg",
    "gardianwaves",
    "actionvspot",
    "web-library",
)
_AUTO_SOURCES = (
    "register_mimo.py",
    "ck_lifecycle.replace",
    "ck_lifecycle.maintain",
)


def _load_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(data, list):
        return {"name": path.stem, "cookies": data}
    return data if isinstance(data, dict) else {}


def is_auto_reg(data: dict, path: Path | None = None) -> bool:
    src = str(data.get("source") or "")
    if any(s in src for s in _AUTO_SOURCES):
        return True
    mb = data.get("mailbox") if isinstance(data.get("mailbox"), dict) else {}
    if mb.get("kind") in ("tempmaillol", "tempmail", "lol"):
        return True
    if (data.get("lifecycle") or {}).get("created_via") in _AUTO_SOURCES:
        return True
    email = (data.get("name") or data.get("email") or (path.stem if path else "") or "").lower()
    dom = email.split("@")[-1] if "@" in email else ""
    return any(m in dom for m in _AUTO_DOMAIN_MARKERS)


def _cookie_header(cookies: list) -> str:
    parts = []
    for c in cookies or []:
        name = c.get("name") or ""
        if name in (
            "serviceToken",
            "userId",
            "cUserId",
            "xiaomichatbot_ph",
            "xiaomichatbot_slh",
        ) or "xiaomimimo" in (c.get("domain") or ""):
            parts.append(f"{name}={c.get('value', '')}")
    return "; ".join(parts)


def probe_ck(cookies: list) -> dict:
    """Live probe /user/mi/get. Returns {live, api_code, bannedStatus, userId}."""
    hdr = _cookie_header(cookies)
    if not hdr:
        return {"live": False, "api_code": None, "bannedStatus": None, "userId": None, "reason": "no_cookies"}
    try:
        from curl_cffi.requests import Session as CurlSession

        r = CurlSession(impersonate="chrome120").get(
            "https://aistudio.xiaomimimo.com/open-apis/user/mi/get",
            headers={"Cookie": hdr, "Content-Type": "application/json"},
            timeout=15,
        )
        j = r.json() if r.content else {}
        if r.status_code == 200 and j.get("code") == 0 and isinstance(j.get("data"), dict):
            d = j["data"]
            return {
                "live": True,
                "api_code": 0,
                "bannedStatus": d.get("bannedStatus"),
                "userId": d.get("userId"),
                "reason": "ok",
            }
        return {
            "live": False,
            "api_code": j.get("code"),
            "bannedStatus": None,
            "userId": None,
            "reason": f"api_{j.get('code')}",
        }
    except Exception as ex:
        return {
            "live": False,
            "api_code": None,
            "bannedStatus": None,
            "userId": None,
            "reason": f"err_{type(ex).__name__}",
            "error": str(ex),
        }


def _in_cooldown(last_create_at: int | float | None) -> bool:
    if not last_create_at:
        return False
    today = datetime.now(_BEIJING).date()
    created = datetime.fromtimestamp(float(last_create_at), _BEIJING).date()
    return created >= today


def _match_keys(name: str) -> set[str]:
    raw = (name or "").strip()
    if not raw:
        return set()
    keys = {raw, raw.casefold()}
    if raw.endswith(".json"):
        keys.add(raw[:-5])
        keys.add(raw[:-5].casefold())
    else:
        keys.add(raw + ".json")
        keys.add((raw + ".json").casefold())
    return keys


def _load_deploy_cfg() -> dict:
    try:
        from claw.auto_deploy import load_config

        return load_config() or {}
    except Exception:
        return {}


def _list_backends() -> list[dict]:
    try:
        from gateway import backend_store

        return backend_store.list_backends() or []
    except Exception:
        try:
            from gateway.runtime import get_all_backends

            return get_all_backends() or []
        except Exception:
            return []


def _serving_account_ids(backends: list[dict]) -> set[str]:
    """Account keys that have a healthy+active backend right now."""
    out: set[str] = set()
    for b in backends:
        if not (
            b.get("enabled", True)
            and b.get("healthy")
            and b.get("lifecycle") == "active"
        ):
            continue
        aid = str(b.get("account_id") or b.get("account") or "").strip()
        if aid:
            out |= _match_keys(aid)
    return out


def classify_one(
    *,
    email: str,
    file_path: Path | None,
    data: dict,
    acc_cfg: dict,
    ck: dict | None = None,
    serving_ids: set[str] | None = None,
    inventory: str = "active",
) -> dict[str, Any]:
    """Classify a single account into deploy_pool + flags."""
    serving_ids = serving_ids or set()
    cookies = data.get("cookies") or []
    if ck is None:
        ck = probe_ck(cookies) if cookies else {
            "live": False,
            "api_code": None,
            "bannedStatus": None,
            "userId": None,
            "reason": "no_cookies",
        }

    enabled = bool(acc_cfg.get("enabled"))
    risk_blocked = bool(acc_cfg.get("risk_blocked"))
    last_create = int(acc_cfg.get("last_create_at") or 0)
    in_cooldown = _in_cooldown(last_create)
    keys = _match_keys(email) | _match_keys(file_path.name if file_path else "")
    is_serving = bool(keys & serving_ids)
    ck_live = bool(ck.get("live"))
    banned = ck.get("bannedStatus")
    if banned and banned != "NOT_BANNED" and not risk_blocked:
        # live probe says banned but not yet tagged — still treat as risk-ish
        risk_effective = True
    else:
        risk_effective = risk_blocked

    # mutual-exclusive deploy_pool for UI
    if inventory == "archive":
        deploy_pool = "archive"
    elif risk_effective:
        deploy_pool = "risk"
    elif not ck_live:
        deploy_pool = "dead"
    elif not enabled:
        deploy_pool = "disabled"
    elif is_serving:
        deploy_pool = "serving"
    elif in_cooldown:
        deploy_pool = "cooldown"
    else:
        deploy_pool = "available"

    can_create = (
        inventory == "active"
        and enabled
        and not risk_effective
        and ck_live
        and not in_cooldown
        and not is_serving
    )

    return {
        "email": email,
        "file": file_path.name if file_path else "",
        "inventory": inventory,  # active | archive
        "auto_reg": is_auto_reg(data, file_path),
        "deploy_pool": deploy_pool,
        "can_create": can_create,
        "is_serving": is_serving,
        "ck_live": ck_live,
        "ck_reason": ck.get("reason"),
        "api_code": ck.get("api_code"),
        "bannedStatus": banned or (data.get("user_info") or {}).get("bannedStatus"),
        "userId": ck.get("userId")
        or data.get("user_id")
        or (data.get("user_info") or {}).get("userId"),
        "enabled": enabled,
        "risk_blocked": risk_blocked,
        "risk_kind": acc_cfg.get("risk_kind") or "",
        "risk_reason": acc_cfg.get("risk_blocked_reason") or "",
        "in_cooldown": in_cooldown,
        "last_create_at": last_create,
        "has_password": bool(data.get("password")),
        "mailbox": data.get("mailbox") if isinstance(data.get("mailbox"), dict) else {},
        "source": data.get("source") or "",
    }


def snapshot(*, probe: bool = True, include_archive: bool = False) -> dict:
    """Full fleet snapshot."""
    cfg = _load_deploy_cfg()
    accounts_cfg = cfg.get("accounts") or {}
    backends = _list_backends()
    serving_ids = _serving_account_ids(backends)

    rows: list[dict] = []
    seen_emails: set[str] = set()

    # active inventory files
    if ACCOUNTS.exists():
        for p in sorted(ACCOUNTS.glob("*.json")):
            if p.name.startswith("_"):
                continue
            data = _load_json(p)
            email = data.get("name") or p.stem
            seen_emails.add(email.casefold())
            acc_cfg = accounts_cfg.get(email) or accounts_cfg.get(p.name) or accounts_cfg.get(p.stem) or {}
            # also try .json suffix variants
            if not acc_cfg:
                for k, v in accounts_cfg.items():
                    if k.casefold() in {email.casefold(), p.stem.casefold(), p.name.casefold()}:
                        acc_cfg = v
                        break
            ck = probe_ck(data.get("cookies") or []) if probe else {
                "live": None,
                "api_code": None,
                "bannedStatus": None,
                "userId": None,
                "reason": "skipped",
            }
            rows.append(
                classify_one(
                    email=email,
                    file_path=p,
                    data=data,
                    acc_cfg=acc_cfg if isinstance(acc_cfg, dict) else {},
                    ck=ck,
                    serving_ids=serving_ids,
                    inventory="active",
                )
            )

    # config orphans (enabled in config but no file)
    for key, acc_cfg in accounts_cfg.items():
        if not isinstance(acc_cfg, dict):
            continue
        stem = key[:-5] if key.endswith(".json") else key
        if stem.casefold() in seen_emails or key.casefold() in seen_emails:
            continue
        rows.append(
            classify_one(
                email=stem,
                file_path=None,
                data={"name": stem, "cookies": []},
                acc_cfg=acc_cfg,
                ck={
                    "live": False,
                    "api_code": None,
                    "bannedStatus": None,
                    "userId": None,
                    "reason": "no_file",
                },
                serving_ids=serving_ids,
                inventory="active",
            )
        )
        rows[-1]["orphan_config"] = True

    if include_archive and ARCHIVE.exists():
        for p in sorted(ARCHIVE.glob("*.json")):
            data = _load_json(p)
            email = data.get("name") or p.stem.split(".20")[0]
            rows.append(
                classify_one(
                    email=email,
                    file_path=p,
                    data=data,
                    acc_cfg={},
                    ck={
                        "live": False,
                        "api_code": None,
                        "bannedStatus": None,
                        "userId": None,
                        "reason": "archive",
                    }
                    if not probe
                    else probe_ck(data.get("cookies") or []),
                    serving_ids=serving_ids,
                    inventory="archive",
                )
            )

    # counts by deploy_pool
    counts: dict[str, int] = {}
    for r in rows:
        if r.get("inventory") == "archive":
            counts["archive"] = counts.get("archive", 0) + 1
            continue
        p = r["deploy_pool"]
        counts[p] = counts.get(p, 0) + 1

    available = [r["email"] for r in rows if r.get("can_create")]
    serving = [r["email"] for r in rows if r.get("is_serving")]
    auto_live = [
        r["email"]
        for r in rows
        if r.get("auto_reg") and r.get("ck_live") and r.get("inventory") == "active"
    ]

    return {
        "ts": int(time.time()),
        "counts": counts,
        "available_for_create": available,
        "serving": serving,
        "auto_live": auto_live,
        "rows": rows,
        "legend": {
            "available": "可用池：可被 relay 选中 create",
            "serving": "服役中：已有 healthy active 后端",
            "cooldown": "冷却池：今日北京时间已 create",
            "risk": "风控池：risk_blocked / ban",
            "dead": "ck 已死：401 等，需归档补号",
            "disabled": "停用：enabled=false",
            "archive": "归档目录",
        },
    }


def available_for_create(*, require_auto_reg: bool = True) -> list[str]:
    """Eligible create list for activity relay (ck-live checked)."""
    snap = snapshot(probe=True, include_archive=False)
    out = []
    for r in snap["rows"]:
        if not r.get("can_create"):
            continue
        if require_auto_reg and not r.get("auto_reg"):
            continue
        out.append(r["email"])
    # fairest: prefer never-created (last_create_at=0) then oldest
    by_create = {r["email"]: int(r.get("last_create_at") or 0) for r in snap["rows"]}
    out.sort(key=lambda e: by_create.get(e, 0))
    return out


def pool_state_for_panel(filename: str) -> dict:
    """Drop-in richer replacement for app._account_pool_state."""
    stem = filename[:-5] if filename.endswith(".json") else filename
    path = ACCOUNTS / f"{stem}.json"
    if not path.exists():
        path = ACCOUNTS / filename
    data = _load_json(path) if path.exists() else {"name": stem, "cookies": []}
    cfg = _load_deploy_cfg()
    accounts_cfg = cfg.get("accounts") or {}
    acc_cfg = accounts_cfg.get(stem) or accounts_cfg.get(filename) or {}
    if not acc_cfg:
        for k, v in accounts_cfg.items():
            if k.casefold() in {stem.casefold(), filename.casefold()}:
                acc_cfg = v
                break
    backends = _list_backends()
    row = classify_one(
        email=data.get("name") or stem,
        file_path=path if path.exists() else None,
        data=data,
        acc_cfg=acc_cfg if isinstance(acc_cfg, dict) else {},
        ck=probe_ck(data.get("cookies") or []),
        serving_ids=_serving_account_ids(backends),
        inventory="active" if path.exists() else "active",
    )
    # panel-facing fields (compat + new)
    return {
        "pool": row["deploy_pool"],
        "risk_blocked": row["risk_blocked"],
        "risk_kind": row["risk_kind"],
        "risk_reason": row["risk_reason"],
        "in_cooldown": row["in_cooldown"],
        "last_create_at": row["last_create_at"],
        "can_create": row["can_create"],
        "is_serving": row["is_serving"],
        "ck_live": row["ck_live"],
        "ck_reason": row["ck_reason"],
        "auto_reg": row["auto_reg"],
        "userId": row.get("userId"),
        "bannedStatus": row.get("bannedStatus"),
    }


def print_status(snap: dict) -> None:
    print("=== 号池快照 ===")
    print("counts:", json.dumps(snap.get("counts") or {}, ensure_ascii=False))
    print("available_for_create:", snap.get("available_for_create"))
    print("serving:", snap.get("serving"))
    print("auto_live:", snap.get("auto_live"))
    print()
    print(f"{'pool':10} {'auto':4} {'ck':4} {'srv':3} {'cd':2} {'email':42} uid")
    print("-" * 90)
    order = {"risk": 0, "dead": 1, "disabled": 2, "serving": 3, "cooldown": 4, "available": 5, "archive": 6}
    rows = sorted(snap["rows"], key=lambda r: (order.get(r["deploy_pool"], 9), r["email"]))
    for r in rows:
        print(
            f"{r['deploy_pool']:10} "
            f"{'Y' if r.get('auto_reg') else '.':4} "
            f"{'Y' if r.get('ck_live') else 'N':4} "
            f"{'Y' if r.get('is_serving') else '.':3} "
            f"{'Y' if r.get('in_cooldown') else '.':2} "
            f"{r['email'][:42]:42} "
            f"{r.get('userId') or '-'}"
        )
    print()
    for k, v in (snap.get("legend") or {}).items():
        print(f"  {k:10} {v}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print(__doc__)
        return
    cmd = sys.argv[1]
    if cmd == "status":
        include_arch = "--archive" in sys.argv
        no_probe = "--no-probe" in sys.argv
        snap = snapshot(probe=not no_probe, include_archive=include_arch)
        if "--json" in sys.argv:
            print(json.dumps(snap, ensure_ascii=False, indent=2))
        else:
            print_status(snap)
        out = ROOT / "tmp" / "captures" / "full_flow" / "pool_status.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
        print("wrote", out)
    elif cmd == "available":
        print(json.dumps(available_for_create(), ensure_ascii=False, indent=2))
    else:
        print("unknown:", cmd)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
