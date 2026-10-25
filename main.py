"""
Dekho Hatke - step-gate backend (Flask) for Render free web service.

Flow
  Telegram user sends a video -> bot creates a link (random pages + random waits)
  -> user opens link on the blog site -> every step is tracked SERVER SIDE
  -> last step "Generate" is accepted only if real elapsed time >= all waits combined
  -> bot sends "All steps verified".

Run:  gunicorn main:app --workers 1 --threads 8 --timeout 60 --bind 0.0.0.0:$PORT
"""
import hmac
import json
import os
import random
import re
import secrets
import sqlite3
import threading
import time
from collections import defaultdict, deque

import requests
from flask import Flask, Response, jsonify, request

# =====================================================================
#  CONFIG  (edit here; env vars override the important ones)
# =====================================================================
SITE_URL = os.getenv("SITE_URL", "https://dekhohatke.com").rstrip("/")

# Blog pages used for steps. Each step = one random page from this list.
# (paths on SITE_URL; every one of these pages must load /assets/gate.js)
BLOG_PAGES = [
    "/blog/automate-daily-tasks-python/",
    "/blog/schedule-python-scripts-render/",
    "/blog/telegram-bot-report-python/",
    "/blog/flutter-setup-first-app/",
    "/blog/flutter-state-management-basics/",
    "/blog/cpp-pointers-explained/",
    "/blog/javascript-fetch-api-guide/",
    "/blog/android-java-recyclerview/",
    "/blog/python-web-scraping-beautifulsoup/",
]

STEPS_RANGE = (3, 4)      # random number of steps per link  (min, max)
TOP_WAIT = (15, 25)       # seconds to wait at TOP of each page (min, max)
BOTTOM_WAIT = (10, 20)    # seconds to wait at BOTTOM of each page (min, max)

LINK_TTL_MIN = int(os.getenv("LINK_TTL_MIN", "45"))   # whole link expires after N minutes
MAX_ACTIVE_PER_USER = 3                                # active links one Telegram user may hold
USER_COOLDOWN_SEC = 5                                  # min gap between link creations per user
MIN_SCROLL_GAP = 1.2      # min seconds between top "Continue" and bottom "Continue"
GRACE = 0.35              # network/timer jitter allowed on every timing check (seconds)

# =====================================================================
#  ENV
# =====================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
PROXY_SECRET = os.getenv("PROXY_SECRET", "")          # shared with the Cloudflare worker
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "") or (secrets.token_hex(16) if BOT_TOKEN else "")
BACKEND_URL = (os.getenv("BACKEND_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").rstrip("/")
DB_PATH = os.getenv("DB_PATH", "/tmp/dh.sqlite3")
DELIVER_VIDEO = os.getenv("DELIVER_VIDEO", "0") == "1"  # also send the video after verification
TURNSTILE_SITEKEY = os.getenv("TURNSTILE_SITEKEY", "")  # optional bot protection on Verify/Generate
TURNSTILE_SECRET = os.getenv("TURNSTILE_SECRET", "")

TG = f"https://api.telegram.org/bot{BOT_TOKEN}"
CODE_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024
_db_lock = threading.RLock()
_bot_username = ""

# =====================================================================
#  DATABASE  (SQLite; /tmp is wiped on redeploy - acceptable for temporary links)
# =====================================================================
def db():
    c = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


_conn = db()
_conn.execute("PRAGMA journal_mode=WAL")
_conn.execute(
    """CREATE TABLE IF NOT EXISTS links(
        code TEXT PRIMARY KEY, tg_user INTEGER, chat_id INTEGER,
        file_id TEXT, file_kind TEXT, steps TEXT NOT NULL,
        cur INTEGER DEFAULT 0, status TEXT DEFAULT 'active',
        created REAL, expires REAL, first_visit REAL,
        step_loaded REAL, top_click REAL, bottom_click REAL,
        nonce TEXT, spent REAL DEFAULT 0, done_at REAL, flags INTEGER DEFAULT 0)"""
)
_conn.execute("CREATE INDEX IF NOT EXISTS ix_user ON links(tg_user, status)")
_conn.commit()


def q(sql, args=(), one=False):
    with _db_lock:
        cur = _conn.execute(sql, args)
        rows = cur.fetchall()
        _conn.commit()
    return (rows[0] if rows else None) if one else rows


# =====================================================================
#  HELPERS
# =====================================================================
def now():
    return time.time()


def build_steps():
    n = random.randint(*STEPS_RANGE)
    pages, last = [], None
    for _ in range(n):
        choices = [p for p in BLOG_PAGES if p != last] or BLOG_PAGES
        last = random.choice(choices)
        pages.append(
            {"path": last, "top": random.randint(*TOP_WAIT), "bottom": random.randint(*BOTTOM_WAIT)}
        )
    return pages


def step_url(code, step):
    """Tracking URL for a step: the blog page + the link code."""
    return f"{step['path']}?t={code}"


def full_url(code, step):
    return SITE_URL + step_url(code, step)


def client_ip():
    if PROXY_SECRET:  # header only trusted when the proxy secret matched
        ip = request.headers.get("X-DH-IP", "")
        if ip:
            return ip.split(",")[0].strip()
    return (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()


_hits = defaultdict(deque)


def rate_limited(key, limit, per=60):
    t, dq = now(), _hits[key]
    while dq and dq[0] < t - per:
        dq.popleft()
    if len(dq) >= limit:
        return True
    dq.append(t)
    return False


def tg(method, **payload):
    """Call the Telegram Bot API. Errors are LOGGED (see Render -> Logs), never silent."""
    if not BOT_TOKEN:
        return None
    try:
        r = requests.post(f"{TG}/{method}", json=payload, timeout=15)
        data = r.json()
        if not data.get("ok"):
            print(f"telegram NOT OK: {method} chat={payload.get('chat_id')} -> {data}")
        return data
    except Exception as e:  # noqa
        print("telegram error:", method, e)
        return None


_bot_checked = 0.0


def bot_url():
    """https://t.me/<bot>; looked up lazily and retried (max once/minute) if getMe failed earlier."""
    global _bot_username, _bot_checked
    if not _bot_username and BOT_TOKEN and now() - _bot_checked > 60:
        _bot_checked = now()
        _bot_username = ((tg("getMe") or {}).get("result") or {}).get("username", "")
    return f"https://t.me/{_bot_username}" if _bot_username else None


def bg(fn, *a):
    threading.Thread(target=fn, args=a, daemon=True).start()


def no_store(resp):
    resp.headers["Cache-Control"] = "no-store"
    return resp


def bad():
    """Same answer for invalid / expired / unknown links - gives away nothing."""
    return no_store(jsonify(ok=False))


# =====================================================================
#  PROXY GUARD  (only the Cloudflare worker may call /api/*)
# =====================================================================
@app.before_request
def guard():
    if request.path.startswith("/api/") and request.path != "/api/health":
        if PROXY_SECRET and not hmac.compare_digest(request.headers.get("X-DH-Proxy", ""), PROXY_SECRET):
            return Response("Not found", 404)
        if rate_limited("ip:" + client_ip(), 90):
            return no_store(jsonify(ok=False, err="slow_down")), 429


@app.after_request
def headers(resp):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp


# =====================================================================
#  HEALTH  (UptimeRobot + browser wake-up ping)
# =====================================================================
@app.route("/", methods=["GET", "HEAD"])
@app.route("/health", methods=["GET", "HEAD"])
@app.route("/api/health", methods=["GET", "HEAD"])
def health():
    return no_store(jsonify(status="ok"))


@app.get("/api/ping")
def ping():
    """Reachable only with the right PROXY_SECRET (see guard) -> proves proxy setup is correct."""
    return no_store(jsonify(ok=True, proxy="ok", bot=bool(BOT_TOKEN), site=SITE_URL))


# =====================================================================
#  STEP API   (called by /assets/gate.js through the Cloudflare proxy)
# =====================================================================
def load_link(code):
    """Return (row, steps) for a valid, active, unexpired link - else (None, None)."""
    if not code or not CODE_RE.match(code):
        return None, None
    row = q("SELECT * FROM links WHERE code=?", (code,), one=True)
    if not row or row["status"] != "active" or now() > row["expires"]:
        return None, None
    return row, json.loads(row["steps"])


def state_payload(row, steps):
    """Everything the page needs, computed from SERVER clock (survives reloads)."""
    i, t = row["cur"], now()
    st = steps[i]
    if row["bottom_click"]:
        phase, remaining = "bottom", max(0, st["bottom"] - (t - row["bottom_click"]))
    elif row["top_click"]:
        phase, remaining = "scroll", 0
    else:
        phase, remaining = "top", max(0, st["top"] - (t - row["step_loaded"]))
    if phase == "bottom" and remaining <= 0:
        phase = "verify"
    return {
        "ok": True, "step": i + 1, "total": len(steps), "last": i == len(steps) - 1,
        "top": st["top"], "bottom": st["bottom"], "phase": phase,
        "remaining": round(remaining, 2), "n": row["nonce"],
        "turnstile": TURNSTILE_SITEKEY or None,
        "bot": bot_url(),
    }


def rotate_nonce(code):
    n = secrets.token_urlsafe(12)
    q("UPDATE links SET nonce=? WHERE code=?", (n, code))
    return n


@app.post("/api/init")
def api_init():
    d = request.get_json(silent=True) or {}
    code, path = str(d.get("t", "")), str(d.get("p", ""))
    row, steps = load_link(code)
    if not row:
        return bad()
    cur = steps[row["cur"]]
    # opened on the wrong page (e.g. old step link) -> send to the current step page
    if path.rstrip("/") != cur["path"].rstrip("/"):
        return no_store(jsonify(ok=True, redirect=step_url(code, cur)))
    t = now()
    if not row["step_loaded"]:
        q("UPDATE links SET step_loaded=?, first_visit=COALESCE(first_visit,?) WHERE code=?", (t, t, code))
    n = rotate_nonce(code)
    row = q("SELECT * FROM links WHERE code=?", (code,), one=True)
    return no_store(jsonify(state_payload(row, steps)))


@app.post("/api/event")
def api_event():
    d = request.get_json(silent=True) or {}
    code, ev = str(d.get("t", "")), str(d.get("ev", ""))
    row, steps = load_link(code)
    if not row or not row["step_loaded"]:
        return bad()
    if not hmac.compare_digest(str(d.get("n", "")), row["nonce"] or "x"):
        return no_store(jsonify(ok=False, err="resync"))
    st, t = steps[row["cur"]], now()
    sig = d.get("sig") or {}
    if sig.get("wd") or sig.get("trusted") is False:  # webdriver flag / synthetic click
        q("UPDATE links SET flags=flags+1 WHERE code=?", (code,))
        return no_store(jsonify(ok=False, err="resync"))

    if ev == "top_click":
        if row["top_click"] or t - row["step_loaded"] < st["top"] - GRACE:
            return no_store(jsonify(ok=False, err="resync"))
        q("UPDATE links SET top_click=? WHERE code=?", (t, code))
    elif ev == "bottom_click":
        if (not row["top_click"] or row["bottom_click"] or not sig.get("bottom")
                or t - row["top_click"] < MIN_SCROLL_GAP):
            return no_store(jsonify(ok=False, err="resync"))
        q("UPDATE links SET bottom_click=? WHERE code=?", (t, code))
    else:
        return bad()
    n = rotate_nonce(code)
    row = q("SELECT * FROM links WHERE code=?", (code,), one=True)
    return no_store(jsonify(state_payload(row, steps)))


def turnstile_ok(token):
    if not TURNSTILE_SECRET:
        return True
    try:
        r = requests.post(
            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
            data={"secret": TURNSTILE_SECRET, "response": token or "", "remoteip": client_ip()},
            timeout=10,
        ).json()
        return bool(r.get("success"))
    except Exception:
        return False


@app.post("/api/verify")
def api_verify():
    """'Verify' on steps 1..N-1 and 'Generate' on the last step."""
    d = request.get_json(silent=True) or {}
    code = str(d.get("t", ""))
    row, steps = load_link(code)
    if not row or not row["step_loaded"]:
        return bad()
    if not hmac.compare_digest(str(d.get("n", "")), row["nonce"] or "x"):
        return no_store(jsonify(ok=False, err="resync"))
    sig = d.get("sig") or {}
    if sig.get("wd") or sig.get("trusted") is False:
        q("UPDATE links SET flags=flags+1 WHERE code=?", (code,))
        return no_store(jsonify(ok=False, err="resync"))

    i, t = row["cur"], now()
    st = steps[i]
    # ---- server-side timing rules ----
    if not row["top_click"] or not row["bottom_click"]:
        return no_store(jsonify(ok=False, err="resync"))
    if t - row["step_loaded"] < st["top"] + st["bottom"] - GRACE:        # whole step
        return no_store(jsonify(ok=False, err="resync"))
    if t - row["bottom_click"] < st["bottom"] - GRACE:                   # bottom wait
        return no_store(jsonify(ok=False, err="resync"))
    if not turnstile_ok(d.get("cf")):
        return no_store(jsonify(ok=False, err="captcha"))

    spent = (row["spent"] or 0) + (t - row["step_loaded"])
    last = i == len(steps) - 1
    if not last:
        q(
            "UPDATE links SET cur=cur+1, spent=?, step_loaded=NULL, top_click=NULL, "
            "bottom_click=NULL, nonce=? WHERE code=?",
            (spent, secrets.token_urlsafe(12), code),
        )
        return no_store(jsonify(ok=True, done=False, next=step_url(code, steps[i + 1])))

    # ---- LAST STEP: total real time since first visit must exceed ALL waits combined ----
    required = sum(s["top"] + s["bottom"] for s in steps)
    if t - row["first_visit"] < required - GRACE or spent < required - GRACE * len(steps):
        return no_store(jsonify(ok=False, err="resync"))
    q("UPDATE links SET status='done', done_at=?, spent=? WHERE code=?", (t, spent, code))
    bg(notify_done, dict(row))
    return no_store(jsonify(ok=True, done=True, bot=bot_url()))


def notify_done(row):
    """Tell the user in Telegram. Retries, because one failed call must not lose the message."""
    sent = False
    for attempt in range(4):
        r = tg("sendMessage", chat_id=row["chat_id"],
               text="✅ All steps verified!\n\nThank you for completing every step.")
        if r and r.get("ok"):
            sent = True
            break
        if r and r.get("error_code") in (400, 403):   # chat not found / bot blocked: retrying is pointless
            break
        time.sleep(1.5 * (attempt + 1))
    print(f"notify_done chat={row['chat_id']} delivered={sent}")
    if DELIVER_VIDEO and row["file_id"]:
        method = "sendVideo" if row["file_kind"] == "video" else "sendDocument"
        field = "video" if row["file_kind"] == "video" else "document"
        tg(method, chat_id=row["chat_id"], **{field: row["file_id"]}, protect_content=True)


# =====================================================================
#  TELEGRAM BOT  (webhook)
# =====================================================================
def create_link(user_id, chat_id, file_id, kind):
    steps = build_steps()
    code = secrets.token_urlsafe(18)
    t = now()
    q(
        "INSERT INTO links(code,tg_user,chat_id,file_id,file_kind,steps,created,expires,nonce) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (code, user_id, chat_id, file_id, kind, json.dumps(steps), t, t + LINK_TTL_MIN * 60, secrets.token_urlsafe(12)),
    )
    return code, steps


def handle_update(u):
    m = u.get("message") or {}
    if not m:
        return
    chat_id, user = m["chat"]["id"], (m.get("from") or {}).get("id", m["chat"]["id"])
    text = (m.get("text") or "").strip()

    media = None
    if m.get("video"):
        media = (m["video"]["file_id"], "video")
    elif m.get("document") and str(m["document"].get("mime_type", "")).startswith("video/"):
        media = (m["document"]["file_id"], "document")

    if not media:
        tg("sendMessage", chat_id=chat_id, parse_mode="HTML",
           text="👋 <b>Welcome to Dekho Hatke!</b>\n\nSend me a <b>video</b> and I will create your "
                "link. Complete all the steps on the website to verify.")
        return

    if rate_limited(f"tgu:{user}", 1, USER_COOLDOWN_SEC):
        tg("sendMessage", chat_id=chat_id, text="⏳ Please wait a few seconds before sending another video.")
        return
    active = q("SELECT COUNT(*) c FROM links WHERE tg_user=? AND status='active' AND expires>?", (user, now()), one=True)["c"]
    if active >= MAX_ACTIVE_PER_USER:
        tg("sendMessage", chat_id=chat_id, text="⚠️ You already have active links. Finish them first or wait for them to expire.")
        return
    code, steps = create_link(user, chat_id, *media)
    link = full_url(code, steps[0])
    tg("sendMessage", chat_id=chat_id, parse_mode="HTML", disable_web_page_preview=True,
       text=f"🎬 <b>Your link is ready!</b>\n\nComplete all <b>{len(steps)} steps</b> to verify.\n"
            f"⏱ Valid for {LINK_TTL_MIN} minutes.\n\n{link}",
       reply_markup={"inline_keyboard": [[{"text": "🚀 Open Link", "url": link}]]})


@app.post("/tg/<secret>")
def tg_webhook(secret):
    if not WEBHOOK_SECRET or not hmac.compare_digest(secret, WEBHOOK_SECRET):
        return Response("Not found", 404)
    hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(hdr, WEBHOOK_SECRET):
        return Response("Not found", 404)
    try:
        handle_update(request.get_json(silent=True) or {})
    except Exception as e:  # never make Telegram retry forever
        print("update error:", e)
    return "ok"


# =====================================================================
#  STARTUP  (set webhook, bot username, purge old links)
# =====================================================================
def startup():
    try:
        q("DELETE FROM links WHERE expires < ?", (now() - 86400,))
        if BOT_TOKEN:
            bot_url()
            if BACKEND_URL:
                r = tg("setWebhook", url=f"{BACKEND_URL}/tg/{WEBHOOK_SECRET}",
                       secret_token=WEBHOOK_SECRET, allowed_updates=["message"], drop_pending_updates=False)
                print("webhook:", r)
            else:
                print("Set BACKEND_URL (or deploy on Render) so the webhook can be registered.")
    except Exception as e:
        print("startup error:", e)


def janitor():
    while True:
        time.sleep(3600)
        try:
            q("DELETE FROM links WHERE expires < ?", (now() - 86400,))
        except Exception:
            pass


threading.Thread(target=startup, daemon=True).start()
threading.Thread(target=janitor, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
