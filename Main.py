# -*- coding: utf-8 -*-
import os
import sys
import json
import asyncio
import threading
import time
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Store bot state
BOT_STATE = {
    "running": False,
    "accounts": [],  # list of {uid, password, region, status, level, nickname, last_log}
    "logs": [],      # list of log strings
    "started_at": None,
    "total_matches": 0,
    "active_matches": 0,
}

# Account file
ACCOUNTS_FILE = "accounts.json"

def load_accounts():
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_accounts(accounts):
    with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(accounts, f, indent=2, ensure_ascii=False)

def add_log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    BOT_STATE["logs"].append(line)
    if len(BOT_STATE["logs"]) > 500:
        BOT_STATE["logs"] = BOT_STATE["logs"][-500:]
    print(line, flush=True)


# ---------------- ROUTES ----------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/accounts", methods=["GET"])
def get_accounts():
    return jsonify(load_accounts())


@app.route("/api/accounts", methods=["POST"])
def add_account():
    data = request.json or {}
    uid = str(data.get("uid", "")).strip()
    password = str(data.get("password", "")).strip()
    region = str(data.get("region", "ME")).strip().upper() or "ME"

    if not uid or not password:
        return jsonify({"ok": False, "error": "UID & Password required"}), 400

    accounts = load_accounts()
    for acc in accounts:
        if acc["uid"] == uid:
            return jsonify({"ok": False, "error": "Account already exists"}), 400

    accounts.append({
        "uid": uid,
        "password": password,
        "region": region,
        "status": "idle",
        "level": "-",
        "nickname": "-",
        "matches": 0,
        "added_at": datetime.now().isoformat()
    })
    save_accounts(accounts)

    # sync to BOT_STATE
    BOT_STATE["accounts"] = accounts
    add_log(f"[+] Account added: {uid} | Region: {region}")
    return jsonify({"ok": True, "accounts": accounts})


@app.route("/api/accounts/<uid>", methods=["DELETE"])
def delete_account(uid):
    accounts = load_accounts()
    accounts = [a for a in accounts if a["uid"] != uid]
    save_accounts(accounts)
    BOT_STATE["accounts"] = accounts
    add_log(f"[-] Account removed: {uid}")
    return jsonify({"ok": True, "accounts": accounts})


@app.route("/api/status")
def get_status():
    accounts = load_accounts()
    running_count = sum(1 for a in accounts if a.get("status") in ("running", "searching", "in_match"))
    return jsonify({
        "running": BOT_STATE["running"],
        "total_accounts": len(accounts),
        "running_accounts": running_count,
        "active_matches": BOT_STATE["active_matches"],
        "total_matches": BOT_STATE["total_matches"],
        "started_at": BOT_STATE["started_at"],
        "uptime": int(time.time() - BOT_STATE["started_at"]) if BOT_STATE["started_at"] else 0,
        "accounts": accounts,
    })


@app.route("/api/logs")
def get_logs():
    since = int(request.args.get("since", 0))
    return jsonify({"logs": BOT_STATE["logs"][since:], "total": len(BOT_STATE["logs"])})


@app.route("/api/start", methods=["POST"])
def start_bot():
    if BOT_STATE["running"]:
        return jsonify({"ok": False, "error": "Bot already running"}), 400

    accounts = load_accounts()
    if not accounts:
        return jsonify({"ok": False, "error": "No accounts added"}), 400

    BOT_STATE["running"] = True
    BOT_STATE["started_at"] = time.time()
    BOT_STATE["logs"] = []
    add_log(f"[*] Starting bot with {len(accounts)} accounts...")

    # launch bot in background thread
    t = threading.Thread(target=run_bot_thread, daemon=True)
    t.start()

    return jsonify({"ok": True})


@app.route("/api/stop", methods=["POST"])
def stop_bot():
    BOT_STATE["running"] = False
    add_log("[!] Stop requested. Shutting down...")
    # signal to bot main loop
    global STOP_FLAG
    STOP_FLAG = True
    return jsonify({"ok": True})


# ---------------- BOT RUNNER ----------------

STOP_FLAG = False

def run_bot_thread():
    """Runs the existing lvl.py bot in a dedicated thread with its own event loop."""
    global STOP_FLAG
    STOP_FLAG = False
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        accounts = load_accounts()
        add_log(f"[*] Launching {len(accounts)} workers...")

        # run the bot
        loop.run_until_complete(run_bot_multi(accounts))

    except Exception as e:
        add_log(f"[-] Bot thread error: {e}")
    finally:
        BOT_STATE["running"] = False
        BOT_STATE["active_matches"] = 0
        add_log("[!] Bot stopped.")
        for a in load_accounts():
            a["status"] = "idle"
        save_accounts(load_accounts())


async def run_bot_multi(accounts):
    """Runs each account in parallel using lvl.py's account_loop_guest."""
    # import here to avoid circular issues
    import lvl

    tasks = []
    for acc in accounts:
        uid = acc["uid"]
        password = acc["password"]
        region = acc.get("region", "ME")

        # update status in shared file
        _update_account_status(uid, "starting")

        # Patch region per-account by running a wrapper
        t = asyncio.create_task(account_wrapper(uid, password, region))
        tasks.append(t)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()


async def account_wrapper(uid, password, region):
    """Wrapper that runs lvl.account_loop_guest with region override."""
    import lvl

    # override region
    lvl.ACCOUNT_REGION = region

    _update_account_status(uid, "running")
    add_log(f"[*] Worker started | UID: {uid} | Region: {region}")

    # monkey-patch to capture logs and status
    original_log = lvl.log

    def patched_log(msg):
        original_log(msg)
        add_log(f"[{uid}] {msg}")
        # update status from log hints
        if "Login OK" in msg:
            _update_account_status(uid, "running")
        if "Match found" in msg or "injected" in msg:
            _update_account_status(uid, "in_match")
            _inc_matches(uid)
        if "Match search sent" in msg or "Next: BR" in msg:
            _update_account_status(uid, "searching")
        if "Session ended" in msg or "Login failed" in msg:
            _update_account_status(uid, "reconnecting")
        # parse level
        if "Lvl" in msg:
            try:
                import re
                m = re.search(r"Lvl\s+(\d+)", msg)
                if m:
                    _update_account_level(uid, int(m.group(1)))
            except Exception:
                pass
        # parse nickname
        if "Login OK" in msg and "|" in msg:
            try:
                parts = msg.split("|")
                if len(parts) >= 2:
                    nick = parts[1].strip()
                    _update_account_nickname(uid, nick)
            except Exception:
                pass

    lvl.log = patched_log

    try:
        await lvl.account_loop_guest(uid, password)
    except asyncio.CancelledError:
        _update_account_status(uid, "stopped")
        raise
    except Exception as e:
        add_log(f"[-] Worker error for {uid}: {e}")
        _update_account_status(uid, "error")


def _update_account_status(uid, status):
    accounts = load_accounts()
    for a in accounts:
        if a["uid"] == uid:
            a["status"] = status
            break
    save_accounts(accounts)


def _update_account_level(uid, level):
    accounts = load_accounts()
    for a in accounts:
        if a["uid"] == uid:
            a["level"] = level
            break
    save_accounts(accounts)


def _update_account_nickname(uid, nickname):
    accounts = load_accounts()
    for a in accounts:
        if a["uid"] == uid:
            a["nickname"] = nickname
            break
    save_accounts(accounts)


def _inc_matches(uid):
    accounts = load_accounts()
    for a in accounts:
        if a["uid"] == uid:
            a["matches"] = int(a.get("matches", 0)) + 1
            break
    BOT_STATE["total_matches"] += 1
    save_accounts(accounts)


# ---------------- MAIN ----------------

if __name__ == "__main__":
    # ensure lvl.py is importable
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    port = int(os.environ.get("PORT", 5000))
    print(f"\n🔥 CSR YEAMIN UPD Dashboard running on http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)