#!/usr/bin/env python3
"""
MiMo Cookie (ck) 生命周期管理
============================

当前运营模式默认 **只养全自动注册号**（tempmail.lol + register_mimo）:

  - ck 约 30 天；temp 收件箱约 1h → **不重登，只补号**
  - 旧 Outlook / 手导号：归档到 accounts/_archive，不参与 deploy
  - recover / maintain：死号归档 + 免费 re-register 顶到目标库存

用法:
  python claw/ck_lifecycle.py scan
  python claw/ck_lifecycle.py auto-only          # 归档非自动号，deploy 只开自动号
  python claw/ck_lifecycle.py maintain --target 3
  python claw/ck_lifecycle.py recover            # 自动号死了就补
  python claw/ck_lifecycle.py replace --count 2
  python claw/ck_lifecycle.py probe EMAIL

环境:
  MIMO_CK_WARN_DAYS=7
  MIMO_CK_AUTO_ONLY=1     # 默认 1：scan/recover 只看自动注册号
  MIMO_CK_REPLACE=1
  MIMO_CK_RELOGIN=0       # 自动号模式默认不重登
  MIMO_CK_POOL_TARGET=3   # maintain 默认目标活号数
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS = ROOT / "accounts"
ARCHIVE = ROOT / "accounts" / "_archive"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "claw"))

from curl_cffi.requests import Session as CurlSession  # noqa: E402

MIMO_BASE = "https://aistudio.xiaomimimo.com"
WARN_DAYS = float(os.environ.get("MIMO_CK_WARN_DAYS") or "7")
AUTO_ONLY = os.environ.get("MIMO_CK_AUTO_ONLY", "1").lower() not in ("0", "false", "no")
# 24h / 2h open ≈ 12 creates/day; keep that many live auto accounts by default.
POOL_TARGET = int(os.environ.get("MIMO_CK_POOL_TARGET") or "12")

# domains / sources that count as full-auto free registration pool
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


def _load_account(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {"name": path.stem, "cookies": data}
    if not isinstance(data, dict):
        return {"name": path.stem, "cookies": []}
    data.setdefault("name", path.stem)
    return data


def _save_account(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _cookie_header(cookies: list) -> str:
    parts = []
    for c in cookies or []:
        if "xiaomimimo" in (c.get("domain") or "") or c.get("name") in (
            "serviceToken",
            "userId",
            "cUserId",
            "xiaomichatbot_ph",
            "xiaomichatbot_slh",
        ):
            parts.append(f"{c['name']}={c['value']}")
    return "; ".join(parts)


def _min_expiry(cookies: list) -> float | None:
    exps = []
    for c in cookies or []:
        exp = c.get("expires")
        if exp is None:
            continue
        try:
            exp_f = float(exp)
        except (TypeError, ValueError):
            continue
        if exp_f > 0:
            exps.append(exp_f)
    return min(exps) if exps else None


def probe_live(cookies: list) -> dict:
    """Hit /open-apis/user/mi/get. Returns status dict."""
    hdr = _cookie_header(cookies)
    if not hdr:
        return {"live": False, "reason": "no_cookies", "http": None, "api_code": None, "user": {}}
    try:
        s = CurlSession(impersonate="chrome120")
        r = s.get(
            f"{MIMO_BASE}/open-apis/user/mi/get",
            headers={"Cookie": hdr, "Content-Type": "application/json"},
            timeout=15,
        )
        try:
            j = r.json()
        except Exception:
            return {"live": False, "reason": f"bad_json_http_{r.status_code}", "http": r.status_code, "api_code": None, "user": {}}
        if r.status_code == 200 and j.get("code") == 0 and isinstance(j.get("data"), dict):
            return {
                "live": True,
                "reason": "ok",
                "http": r.status_code,
                "api_code": 0,
                "user": j["data"],
            }
        return {
            "live": False,
            "reason": f"api_code_{j.get('code')}",
            "http": r.status_code,
            "api_code": j.get("code"),
            "user": {},
        }
    except Exception as ex:
        return {
            "live": False,
            "reason": f"err_{type(ex).__name__}",
            "http": None,
            "api_code": None,
            "user": {},
            "error": str(ex),
        }


def classify_mailbox(data: dict) -> dict:
    """Decide recovery strategy from account file."""
    mb = data.get("mailbox")
    if isinstance(mb, dict) and mb:
        return {
            "kind": mb.get("kind") or "unknown",
            "recoverable": bool(mb.get("recoverable")),
            "strategy": mb.get("strategy")
            or ("relogin" if mb.get("recoverable") else "replace"),
        }

    email = (data.get("name") or data.get("email") or "").lower()
    dom = email.split("@")[-1] if "@" in email else ""
    free_markers = (
        "airfryersbg",
        "gardianwaves",
        "actionvspot",
        "web-library",
        "guerrillamail",
        "sharklasers",
        "yopmail",
        "mailnesia",
        "maildrop",
    )
    if any(m in dom for m in free_markers):
        return {"kind": "tempmaillol", "recoverable": False, "strategy": "replace"}
    if data.get("password") and ("outlook." in dom or "hotmail." in dom or "gmail." in dom):
        # durable freemail — relogin possible if user can still open inbox
        return {"kind": "freemail", "recoverable": True, "strategy": "relogin"}
    if data.get("password"):
        return {"kind": "unknown", "recoverable": True, "strategy": "relogin"}
    return {"kind": "unknown", "recoverable": False, "strategy": "manual"}


def scan_account(path: Path) -> dict:
    data = _load_account(path)
    cookies = data.get("cookies") or []
    live = probe_live(cookies)
    exp = _min_expiry(cookies)
    now = time.time()
    days_left = (exp - now) / 86400 if exp else None
    mb = classify_mailbox(data)

    if live["live"]:
        if days_left is not None and days_left < WARN_DAYS:
            state = "expiring"
        else:
            state = "ok"
    else:
        state = "dead"

    row = {
        "file": path.name,
        "email": data.get("name") or path.stem,
        "state": state,
        "live": live["live"],
        "reason": live["reason"],
        "api_code": live.get("api_code"),
        "days_left": round(days_left, 2) if days_left is not None else None,
        "has_password": bool(data.get("password")),
        "mailbox": mb,
        "userId": (live.get("user") or {}).get("userId")
        or data.get("user_id")
        or (data.get("user_info") or {}).get("userId"),
        "bannedStatus": (live.get("user") or {}).get("bannedStatus")
        or (data.get("user_info") or {}).get("bannedStatus"),
    }

    # persist lifecycle probe snapshot
    lc = dict(data.get("lifecycle") or {})
    lc["last_probe_at"] = int(now)
    lc["last_state"] = state
    if live["live"]:
        lc["last_ok_at"] = int(now)
        # refresh user_info when live
        if live.get("user"):
            data["user_info"] = live["user"]
            data["user_id"] = live["user"].get("userId") or data.get("user_id")
    if "mailbox" not in data:
        data["mailbox"] = mb
    data["lifecycle"] = lc
    try:
        _save_account(path, data)
    except Exception:
        pass
    return row


def is_auto_reg_account(data: dict, path: Path | None = None) -> bool:
    """True if account belongs to full-auto free registration pool."""
    if not isinstance(data, dict):
        return False
    src = str(data.get("source") or "")
    if any(s in src for s in _AUTO_SOURCES):
        return True
    mb = data.get("mailbox") if isinstance(data.get("mailbox"), dict) else {}
    if mb.get("kind") in ("tempmaillol", "tempmail", "lol") and mb.get("strategy") == "replace":
        return True
    if (data.get("lifecycle") or {}).get("created_via") in (
        "ck_lifecycle.replace",
        "ck_lifecycle.maintain",
    ):
        return True
    email = (data.get("name") or data.get("email") or (path.stem if path else "") or "").lower()
    dom = email.split("@")[-1] if "@" in email else ""
    if any(m in dom for m in _AUTO_DOMAIN_MARKERS):
        return True
    return False


def list_account_files(*, auto_only: bool | None = None) -> list[Path]:
    if not ACCOUNTS.exists():
        return []
    use_auto = AUTO_ONLY if auto_only is None else auto_only
    out = []
    for p in sorted(ACCOUNTS.glob("*.json")):
        if p.name.startswith("_") or not p.is_file():
            continue
        if use_auto:
            try:
                data = _load_account(p)
            except Exception:
                continue
            if not is_auto_reg_account(data, p):
                continue
        out.append(p)
    return out


def cmd_scan(*, all_accounts: bool = False) -> list[dict]:
    rows = []
    files = list_account_files(auto_only=False if all_accounts else None)
    mode = "all" if all_accounts else ("auto-only" if AUTO_ONLY else "all")
    print(f"[scan] mode={mode} files={len(files)}", flush=True)
    for p in files:
        row = scan_account(p)
        row["auto_reg"] = is_auto_reg_account(_load_account(p), p)
        rows.append(row)
        days = row["days_left"]
        days_s = f"{days:.1f}d" if days is not None else "?"
        tag = "auto" if row["auto_reg"] else "legacy"
        print(
            f"[{row['state']:8}] [{tag:6}] {row['email']:45} "
            f"live={row['live']} left={days_s:6} "
            f"pwd={row['has_password']} strategy={row['mailbox'].get('strategy')} "
            f"uid={row.get('userId') or '-'}",
            flush=True,
        )
    summary = {
        "mode": mode,
        "ok": sum(1 for r in rows if r["state"] == "ok"),
        "expiring": sum(1 for r in rows if r["state"] == "expiring"),
        "dead": sum(1 for r in rows if r["state"] == "dead"),
        "total": len(rows),
        "auto_ok": sum(1 for r in rows if r.get("auto_reg") and r["state"] in ("ok", "expiring")),
    }
    print("\nSUMMARY", json.dumps(summary, ensure_ascii=False))
    out = ROOT / "tmp" / "captures" / "full_flow" / "ck_scan.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote", out)
    return rows


def cmd_auto_only(dry_run: bool = False) -> dict:
    """Archive non-auto accounts; enable deploy only for live auto-reg accounts."""
    kept = []
    archived = []
    for p in list_account_files(auto_only=False):
        data = _load_account(p)
        email = data.get("name") or p.stem
        if is_auto_reg_account(data, p):
            # stamp pool membership
            data.setdefault(
                "mailbox",
                {"kind": "tempmaillol", "recoverable": False, "strategy": "replace"},
            )
            data["mailbox"]["recoverable"] = False
            data["mailbox"]["strategy"] = "replace"
            data.setdefault("pool", "auto_reg")
            data["pool"] = "auto_reg"
            if not dry_run:
                _save_account(p, data)
            kept.append(email)
            print(f"[keep]   {email}", flush=True)
        else:
            print(f"[archive]{email} (legacy)", flush=True)
            if not dry_run:
                dest = archive_account(p, "legacy_not_auto")
                archived.append({"email": email, "path": str(dest)})
            else:
                archived.append({"email": email, "path": "(dry-run)"})

    # auto_deploy: only enable live auto accounts
    deploy_enabled = []
    if not dry_run:
        try:
            from claw.auto_deploy import load_config, save_config

            cfg = load_config()
            accs = cfg.setdefault("accounts", {})
            # disable everything first
            for key, acc in list(accs.items()):
                if not isinstance(acc, dict):
                    continue
                acc["enabled"] = False
            for p in list_account_files(auto_only=True):
                data = _load_account(p)
                live = probe_live(data.get("cookies") or [])
                key = data.get("name") or p.stem
                acc = accs.setdefault(key, {})
                acc["enabled"] = bool(live.get("live"))
                acc.setdefault("cron", "0 * * * *")
                if acc["enabled"]:
                    deploy_enabled.append(key)
                # clear stale risk on fresh auto pool optionally leave as-is
            save_config(cfg)
        except Exception as ex:
            print(f"[auto-only] deploy config warn: {ex}", flush=True)

    result = {
        "kept": kept,
        "archived": archived,
        "deploy_enabled": deploy_enabled,
        "dry_run": dry_run,
    }
    print("AUTO_ONLY", json.dumps(result, ensure_ascii=False, indent=2))
    return result


def cmd_maintain(target: int | None = None, dry_run: bool = False) -> dict:
    """Keep auto-reg pool at ``target`` live accounts (default MIMO_CK_POOL_TARGET)."""
    target = int(target if target is not None else POOL_TARGET)
    rows = [scan_account(p) for p in list_account_files(auto_only=True)]
    live = [r for r in rows if r["state"] in ("ok", "expiring")]
    dead = [r for r in rows if r["state"] == "dead"]
    print(
        f"[maintain] target={target} live={len(live)} dead={len(dead)} dry_run={dry_run}",
        flush=True,
    )
    actions = []
    # archive dead auto accounts
    for r in dead:
        path = ACCOUNTS / r["file"]
        if path.exists():
            if dry_run:
                actions.append({"status": "dry_run", "action": "archive", "email": r["email"]})
            else:
                dest = archive_account(path, "ck_dead")
                actions.append(
                    {"status": "archived", "action": "archive", "email": r["email"], "path": str(dest)}
                )
    need = max(0, target - len(live))
    if need:
        actions.extend(try_replace(count=need, dry_run=dry_run, enable_deploy=True))
    else:
        print("[maintain] pool full, no register needed", flush=True)

    # re-enable deploy for live autos
    if not dry_run:
        try:
            from claw.auto_deploy import load_config, save_config

            cfg = load_config()
            accs = cfg.setdefault("accounts", {})
            live_emails = set()
            for p in list_account_files(auto_only=True):
                data = _load_account(p)
                if probe_live(data.get("cookies") or []).get("live"):
                    key = data.get("name") or p.stem
                    live_emails.add(key)
                    acc = accs.setdefault(key, {})
                    acc["enabled"] = True
                    acc.setdefault("cron", "0 * * * *")
            for key, acc in accs.items():
                if isinstance(acc, dict) and key not in live_emails:
                    # don't force-disable unknown historical keys except when clearly not live autos
                    if key in {r["email"] for r in rows}:
                        acc["enabled"] = False
            save_config(cfg)
        except Exception as ex:
            print(f"[maintain] deploy sync warn: {ex}", flush=True)

    out = {
        "target": target,
        "live_before": len(live),
        "dead": len(dead),
        "registered": need,
        "actions": actions,
        "dry_run": dry_run,
    }
    path = ROOT / "tmp" / "captures" / "full_flow" / "ck_maintain.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("MAINTAIN", json.dumps({k: v for k, v in out.items() if k != "actions"}, ensure_ascii=False))
    print("wrote", path)
    return out


def try_relogin(path: Path, dry_run: bool = False) -> dict:
    """Password re-login. 2FA needs durable mailbox — free temp will fail."""
    from mimo_auth import do_login, save_cookies, _fetch_user_info

    data = _load_account(path)
    email = data.get("name") or path.stem
    password = data.get("password")
    if not password:
        return {"status": "skip", "email": email, "error": "no_password"}
    mb = classify_mailbox(data)
    if not mb.get("recoverable") and mb.get("strategy") == "replace":
        return {
            "status": "skip",
            "email": email,
            "error": "mailbox_not_recoverable_use_replace",
            "mailbox": mb,
        }
    if dry_run:
        return {"status": "dry_run", "email": email, "action": "relogin", "mailbox": mb}

    # Optional IMAP wait if mailbox.kind == imap and config present
    email_code_fn = None
    if mb.get("kind") == "imap" and isinstance(data.get("mailbox"), dict):
        cfg = data["mailbox"]
        if cfg.get("host") and cfg.get("user") and cfg.get("password"):
            import importlib.util

            spec = importlib.util.spec_from_file_location(
                "mimo_mailbox", ROOT / "claw" / "mailbox.py"
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            box = mod.make_mailbox(
                "imap",
                address=cfg.get("address") or email,
                host=cfg["host"],
                user=cfg["user"],
                password=cfg["password"],
                port=int(cfg.get("port") or 993),
            )

            def email_code_fn():
                return box.wait_code(timeout=180)

    print(f"[recover] relogin {email} ...", flush=True)
    try:
        cookies = do_login(email, password, email_code_fn=email_code_fn)
        info = _fetch_user_info(cookies)
        save_cookies(email, cookies, info, password=password)
        live = probe_live(cookies)
        return {
            "status": "ok" if live["live"] else "error",
            "email": email,
            "action": "relogin",
            "live": live["live"],
            "userId": (info or {}).get("userId"),
        }
    except Exception as ex:
        return {
            "status": "error",
            "email": email,
            "action": "relogin",
            "error": f"{type(ex).__name__}: {ex}",
        }


def archive_account(path: Path, reason: str) -> Path:
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%dT%H%M%S")
    dest = ARCHIVE / f"{path.stem}.{ts}.{reason}.json"
    shutil.move(str(path), str(dest))
    # also drop from auto_deploy if present
    try:
        from claw.auto_deploy import load_config, save_config

        cfg = load_config()
        accs = cfg.get("accounts") or {}
        for key in list(accs.keys()):
            if key == path.name or key == path.stem or key == path.stem + ".json":
                accs.pop(key, None)
        save_config(cfg)
    except Exception:
        pass
    return dest


def try_replace(count: int = 1, dry_run: bool = False, enable_deploy: bool = True) -> list[dict]:
    """Register new free accounts via tempmail.lol to refill pool."""
    if dry_run:
        return [{"status": "dry_run", "action": "replace", "count": count}]

    # Load root register_mimo (not claw/ shim side-effects)
    import importlib.util

    _reg_path = ROOT / "register_mimo.py"
    _spec = importlib.util.spec_from_file_location("mimo_reg_root", _reg_path)
    reg = importlib.util.module_from_spec(_spec)
    assert _spec and _spec.loader
    _spec.loader.exec_module(reg)

    results = []
    i = 0
    consecutive_rate = 0
    while i < count:
        print(f"[replace] free reg {i+1}/{count}", flush=True)
        try:
            res = reg.register_auto(
                region="HK",
                mailbox_kind="tempmaillol",
                mail_timeout=300,
                enable_deploy=enable_deploy,
            )
        except Exception as ex:
            res = {"status": "error", "error": f"{type(ex).__name__}: {ex}"}
        # ensure mailbox meta
        if res.get("status") == "ok" and res.get("path"):
            consecutive_rate = 0
            p = Path(res["path"])
            if p.exists():
                data = _load_account(p)
                data["mailbox"] = {
                    "kind": "tempmaillol",
                    "recoverable": False,
                    "strategy": "replace",
                }
                if res.get("password"):
                    data["password"] = res["password"]
                data["lifecycle"] = {
                    "last_ok_at": int(time.time()),
                    "last_probe_at": int(time.time()),
                    "last_state": "ok",
                    "created_via": "ck_lifecycle.replace",
                }
                _save_account(p, data)
        err = str(res.get("error") or "")
        if res.get("status") != "ok" and (
            "85005" in err or "限流" in err or "请求被拒绝" in err
        ):
            consecutive_rate += 1
            wait_s = min(600, 60 * consecutive_rate)
            print(f"[replace] rate-limit, sleep {wait_s}s then retry same slot", flush=True)
            time.sleep(wait_s)
            if consecutive_rate <= 8:
                continue  # retry same i
        else:
            consecutive_rate = 0
        results.append(
            {
                "status": res.get("status"),
                "email": res.get("email"),
                "error": res.get("error"),
                "path": str(res.get("path") or ""),
                "userId": (res.get("user_info") or {}).get("userId"),
                "password": res.get("password") if res.get("status") == "ok" else None,
                "action": "replace",
            }
        )
        print(
            "  ->",
            results[-1]["status"],
            results[-1].get("email"),
            results[-1].get("error") or results[-1].get("path"),
            flush=True,
        )
        i += 1
        if i < count and res.get("status") == "ok":
            time.sleep(15)  # space successes to avoid 85005
        elif i < count and res.get("status") != "ok":
            time.sleep(8)
    return results


def cmd_recover(dry_run: bool = False) -> dict:
    """Auto-only default: dead/expiring → archive + free replace. No relogin."""
    allow_replace = os.environ.get("MIMO_CK_REPLACE", "1").lower() not in ("0", "false", "no")
    # auto-only pool: relogin off by default
    allow_relogin = os.environ.get("MIMO_CK_RELOGIN", "0").lower() in ("1", "true", "yes")

    rows = [scan_account(p) for p in list_account_files(auto_only=True if AUTO_ONLY else None)]
    actions = []
    need_replace = 0

    for row in rows:
        if row["state"] == "ok":
            continue
        path = ACCOUNTS / row["file"]
        if not path.exists():
            continue
        # free auto pool always replace
        strategy = "replace" if AUTO_ONLY else ((row.get("mailbox") or {}).get("strategy") or "replace")
        print(f"\n[recover] {row['email']} state={row['state']} strategy={strategy}", flush=True)

        if strategy == "relogin" and allow_relogin and row["has_password"]:
            r = try_relogin(path, dry_run=dry_run)
            actions.append(r)
            if r.get("status") == "ok":
                continue
            strategy = "replace"

        if strategy == "replace" and allow_replace:
            need_replace += 1
            if not dry_run:
                if row["state"] in ("dead", "expiring"):
                    # only archive when fully dead; expiring still live — just top up
                    if row["state"] == "dead":
                        dest = archive_account(path, "ck_dead")
                        actions.append(
                            {
                                "status": "archived",
                                "email": row["email"],
                                "path": str(dest),
                                "action": "archive",
                            }
                        )
                    else:
                        # expiring: keep until dead, but still count toward replace top-up optional
                        actions.append(
                            {
                                "status": "keep_until_dead",
                                "email": row["email"],
                                "action": "note",
                            }
                        )
                        need_replace -= 1  # don't double-count expiring as full replace of self
            else:
                actions.append(
                    {
                        "status": "dry_run",
                        "email": row["email"],
                        "action": "archive+replace" if row["state"] == "dead" else "watch",
                    }
                )
        else:
            actions.append(
                {
                    "status": "manual",
                    "email": row["email"],
                    "action": "manual",
                    "hint": "replace disabled; run maintain/replace manually",
                }
            )

    # also top up to pool target if short
    live_ok = sum(1 for r in rows if r["state"] in ("ok", "expiring"))
    # after archiving dead, live_ok unchanged; dead will be replaced
    shortfall = max(0, POOL_TARGET - live_ok)
    # need_replace is "one per dead"; also ensure pool target
    total_reg = max(need_replace, shortfall) if allow_replace else 0

    if total_reg and not dry_run:
        actions.extend(try_replace(count=total_reg, dry_run=False))
    elif total_reg and dry_run:
        actions.append({"status": "dry_run", "action": "replace", "count": total_reg})

    out = {
        "mode": "auto-only" if AUTO_ONLY else "mixed",
        "actions": actions,
        "need_replace": total_reg,
        "pool_target": POOL_TARGET,
        "live_ok": live_ok,
        "dry_run": dry_run,
    }
    path = ROOT / "tmp" / "captures" / "full_flow" / "ck_recover.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nRECOVER", json.dumps({k: v for k, v in out.items() if k != "actions"}, ensure_ascii=False, indent=2))
    print("wrote", path)
    return out


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print(__doc__)
        return
    cmd = sys.argv[1]
    dry = "--dry-run" in sys.argv
    if cmd == "scan":
        cmd_scan(all_accounts="--all" in sys.argv)
    elif cmd == "auto-only":
        cmd_auto_only(dry_run=dry)
    elif cmd == "maintain":
        target = None
        for i, a in enumerate(sys.argv):
            if a == "--target" and i + 1 < len(sys.argv):
                target = int(sys.argv[i + 1])
        cmd_maintain(target=target, dry_run=dry)
    elif cmd == "probe":
        if len(sys.argv) < 3:
            print("usage: probe EMAIL_OR_FILENAME")
            sys.exit(1)
        key = sys.argv[2]
        path = ACCOUNTS / key
        if not path.exists():
            path = ACCOUNTS / f"{key}.json"
        if not path.exists():
            matches = [p for p in list_account_files(auto_only=False) if key in p.name]
            if not matches:
                print("not found", key)
                sys.exit(1)
            path = matches[0]
        print(json.dumps(scan_account(path), ensure_ascii=False, indent=2))
    elif cmd == "recover":
        cmd_recover(dry_run=dry)
    elif cmd == "replace":
        count = 1
        for i, a in enumerate(sys.argv):
            if a == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])
        print(json.dumps(try_replace(count=count, dry_run=dry), ensure_ascii=False, indent=2))
    else:
        print("unknown command", cmd)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
