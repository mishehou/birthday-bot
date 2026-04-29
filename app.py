try:
    from gevent import monkey as _gm; _gm.patch_all()
except ImportError:
    pass

import os
import re
import json
import logging
import sqlite3
import urllib.parse
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from flask import Flask, Response, flash, jsonify, redirect, render_template, request, session, url_for
from pyluach.dates import HebrewDate

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "birthday-bot-secret")
app.config["TEMPLATES_AUTO_RELOAD"] = True


DATABASE   = os.environ.get("DATABASE_PATH", "/data/birthdays.db")
BACKUP_DIR = os.environ.get("BACKUP_DIR", "")
NOTIFICATION_HOUR = int(os.environ.get("NOTIFICATION_HOUR", "9"))
NOTIFICATION_MINUTE = int(os.environ.get("NOTIFICATION_MINUTE", "0"))
TIMEZONE = os.environ.get("TIMEZONE", "Asia/Jerusalem")

def _today() -> date:
    """Current date in the configured local timezone (not UTC)."""
    return datetime.now(tz=ZoneInfo(TIMEZONE)).date()

GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI  = os.environ.get("GOOGLE_REDIRECT_URI", "https://birthday.mishehou.org/auth/google/callback")
_GOOGLE_AUTH_URL     = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL    = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
_https_pool          = ThreadPoolExecutor(max_workers=4)

# Month numbering: Nisan=1, Iyar=2, ..., Tishrei=7, ..., Adar=12, Adar II=13
HEBREW_MONTHS = {
    1: "Nisan",
    2: "Iyar",
    3: "Sivan",
    4: "Tammuz",
    5: "Av",
    6: "Elul",
    7: "Tishrei",
    8: "Cheshvan",
    9: "Kislev",
    10: "Tevet",
    11: "Shvat",
    12: "Adar",
    13: "Adar II",
}

HEBREW_MONTHS_HE = {
    1: "ניסן",
    2: "אייר",
    3: "סיוון",
    4: "תמוז",
    5: "אב",
    6: "אלול",
    7: "תשרי",
    8: "חשון",
    9: "כסלו",
    10: "טבת",
    11: "שבט",
    12: "אדר",
    13: "אדר ב'",
}

# Per-month day info sent to the frontend for the dynamic day selector.
# max  = maximum possible days that month can ever have in any year type.
# variable = True when the month does NOT always reach its max
#   Cheshvan (8): 29 in deficient/regular years, 30 in complete years
#   Kislev   (9): 29 in deficient years, 30 in regular/complete years
#   Adar    (12): 29 in non-leap years (plain Adar), 30 in leap years (Adar I)
MONTH_DAYS_INFO = {
    1:  {"max": 30, "variable": False},  # Nisan   — always 30
    2:  {"max": 29, "variable": False},  # Iyar    — always 29
    3:  {"max": 30, "variable": False},  # Sivan   — always 30
    4:  {"max": 29, "variable": False},  # Tammuz  — always 29
    5:  {"max": 30, "variable": False},  # Av      — always 30
    6:  {"max": 29, "variable": False},  # Elul    — always 29
    7:  {"max": 30, "variable": False},  # Tishrei — always 30
    8:  {"max": 30, "variable": True},   # Cheshvan — 29 or 30
    9:  {"max": 30, "variable": True},   # Kislev  — 29 or 30
    10: {"max": 29, "variable": False},  # Tevet   — always 29
    11: {"max": 30, "variable": False},  # Shvat   — always 30
    12: {"max": 30, "variable": True},   # Adar    — 29 (non-leap) / 30 as Adar I (leap)
    13: {"max": 29, "variable": False},  # Adar II — always 29
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(os.path.dirname(DATABASE), exist_ok=True)
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT    NOT NULL UNIQUE,
            name       TEXT,
            picture    TEXT,
            role       TEXT    NOT NULL DEFAULT 'viewer',
            last_login TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS people (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            first_name  TEXT    NOT NULL DEFAULT '',
            last_name   TEXT    NOT NULL DEFAULT '',
            hebrew_day  INTEGER NOT NULL,
            hebrew_month INTEGER NOT NULL,
            hebrew_year INTEGER,
            notes       TEXT,
            gender      TEXT    DEFAULT '',
            group_type  TEXT    DEFAULT '',
            event_type  TEXT    DEFAULT 'יום הולדת',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migrations: add columns to existing databases
    for col_def in [
        "ALTER TABLE people ADD COLUMN gender TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN first_name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE people ADD COLUMN last_name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE people ADD COLUMN group_type TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN hebrew_year INTEGER",
        "ALTER TABLE people ADD COLUMN event_type TEXT DEFAULT 'יום הולדת'",
    ]:
        try:
            conn.execute(col_def)
        except Exception:
            pass  # Column already exists
    # Migrate: split existing name into first_name / last_name where not yet done
    conn.execute("""
        UPDATE people SET
            first_name = CASE WHEN instr(name, ' ') > 0
                              THEN substr(name, 1, instr(name, ' ') - 1)
                              ELSE name END,
            last_name  = CASE WHEN instr(name, ' ') > 0
                              THEN substr(name, instr(name, ' ') + 1)
                              ELSE '' END
        WHERE first_name = '' OR first_name IS NULL
    """)
    conn.commit()
    conn.close()
    logger.info("Database initialised at %s", DATABASE)


# ---------------------------------------------------------------------------
# Auth helpers + decorators
# ---------------------------------------------------------------------------

def get_user_by_email(email: str) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    conn.close()
    return dict(row) if row else None


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_email"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_email"):
            return redirect(url_for("login_page"))
        if session.get("user_role") != "administrator":
            flash("נדרשות הרשאות מנהל.", "danger")
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated


def editor_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_email"):
            return redirect(url_for("login_page"))
        if session.get("user_role") not in ("administrator", "editor"):
            if request.method in ("POST", "PUT", "DELETE", "PATCH"):
                return jsonify({"error": "read-only role"}), 403
            flash("Your role is view-only. Contact an administrator to make changes.", "warning")
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Hebrew calendar helpers
# ---------------------------------------------------------------------------

def today_hebrew():
    h = HebrewDate.from_pydate(_today())
    return h.year, h.month, h.day


def is_leap_year(hebrew_year: int) -> bool:
    # Standard Hebrew calendar formula: years 3,6,8,11,14,17,19 of each 19-year cycle
    return (7 * hebrew_year + 1) % 19 < 7


def month_name(month: int) -> str:
    return HEBREW_MONTHS.get(month, f"Month {month}")


def format_hebrew_date(day: int, month: int, year: int | None = None) -> str:
    name = HEBREW_MONTHS.get(month, str(month))
    if year:
        return f"{day} {name} {year}"
    return f"{day} {name}"


_HEB_ONES   = ['', 'א', 'ב', 'ג', 'ד', 'ה', 'ו', 'ז', 'ח', 'ט']
_HEB_TENS   = ['', 'י', 'כ', 'ל', 'מ', 'נ', 'ס', 'ע', 'פ', 'צ']
_HEB_HUNDREDS = ['', 'ק', 'ר', 'ש', 'ת', 'תק', 'תר', 'תש', 'תת', 'תתק']

def int_to_hebrew_numeral(n: int, geresh: bool = True) -> str:
    """Convert a positive integer to Hebrew numeral letters (drop thousands)."""
    n = n % 1000  # drop thousands (e.g. 5786 → 786)
    hundreds, rem = divmod(n, 100)
    result = _HEB_HUNDREDS[hundreds]
    if rem == 15:
        result += 'טו'
    elif rem == 16:
        result += 'טז'
    else:
        tens, ones = divmod(rem, 10)
        result += _HEB_TENS[tens] + _HEB_ONES[ones]
    if geresh:
        if len(result) > 1:
            result = result[:-1] + '״' + result[-1]  # gershayim ״ before last letter
        elif len(result) == 1:
            result += '׳'  # geresh ׳
    return result


def format_hebrew_date_he(day: int, month: int, year: int) -> str:
    """Return a fully Hebrew-lettered date, e.g. כ״ח אייר תשפ״ו"""
    return f"{int_to_hebrew_numeral(day, geresh=False)} {HEBREW_MONTHS_HE[month]} {int_to_hebrew_numeral(year)}"


def compute_days_until(hday: int, hmonth: int) -> int:
    """Return days until the next occurrence of this Hebrew date (0 = today)."""
    today = _today()
    h_today = HebrewDate.from_pydate(today)
    for year_offset in range(2):
        check_year = h_today.year + year_offset
        month_to_use = hmonth
        if hmonth == 12 and is_leap_year(check_year):
            month_to_use = 13
        elif hmonth == 13 and not is_leap_year(check_year):
            month_to_use = 12
        try:
            next_date = HebrewDate(check_year, month_to_use, hday).to_pydate()
            delta = (next_date - today).days
            if delta >= 0:
                return delta
        except Exception:
            pass
    return 365


def get_people_with_birthday_on(hday: int, hmonth: int, hyear: int) -> list[dict]:
    """Return all people whose Hebrew birthday falls on (hday, hmonth) in hyear."""
    months_to_check = {hmonth}

    # Adar II (13) in a leap year also covers people stored with plain Adar (12)
    if hmonth == 13 and is_leap_year(hyear):
        months_to_check.add(12)
    # Plain Adar (12) in a non-leap year also covers people stored as Adar II (13)
    elif hmonth == 12 and not is_leap_year(hyear):
        months_to_check.add(13)

    conn = get_db()
    placeholders = ",".join("?" for _ in months_to_check)
    rows = conn.execute(
        f"SELECT * FROM people WHERE hebrew_day = ? AND hebrew_month IN ({placeholders})",
        [hday, *months_to_check],
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_upcoming_birthdays(days_ahead: int = 30) -> list[dict]:
    """Scan the next *days_ahead* days and return upcoming birthdays with days_until."""
    upcoming = []
    today = _today()

    for delta in range(1, days_ahead + 1):
        check_date = today + timedelta(days=delta)
        h = HebrewDate.from_pydate(check_date)
        people = get_people_with_birthday_on(h.day, h.month, h.year)
        for p in people:
            p["days_until"] = delta
            p["next_hebrew_date"] = format_hebrew_date(h.day, h.month, h.year)
            p["next_hebrew_date_he"] = format_hebrew_date_he(h.day, h.month, h.year)
            p["next_gregorian"] = check_date.strftime("%d/%m/%Y")
            if p.get("hebrew_year") is not None:
                p["years_count"] = h.year - p["hebrew_year"]
            else:
                p["years_count"] = None
            upcoming.append(p)

    return upcoming


# ---------------------------------------------------------------------------
# Notification — WhatsApp via WAHA (self-hosted, linked device)
# ---------------------------------------------------------------------------

WAHA_URL          = os.environ.get("WAHA_URL", "http://waha:3000")
WAHA_API_KEY      = os.environ.get("WAHA_API_KEY", "birthday-bot-key")
WAHA_SESSION      = "default"
WEBHOOK_URL       = os.environ.get("WEBHOOK_URL", "http://birthday-bot:5000/webhook")
COMMAND_GROUP_NAME = os.environ.get("COMMAND_GROUP_NAME", "Group for Test")

# ── Shared contacts store ────────────────────────────────────────────────────

CONTACTS_FILE = os.environ.get("CONTACTS_FILE", "/data/contacts.json")

def load_contacts() -> list:
    try:
        with open(CONTACTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_contacts(contacts: list) -> None:
    Path(CONTACTS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(CONTACTS_FILE, "w", encoding="utf-8") as f:
        json.dump(contacts, f, indent=2, ensure_ascii=False)

# ── Phone / contacts cache ───────────────────────────────────────────────────

_DATA_DIR       = os.path.dirname(os.environ.get("DATABASE_PATH", "/data/birthdays.db"))
GOOGLE_CACHE_FILE = os.path.join(_DATA_DIR, "google_cache.json")

def normalize_phone(raw: str) -> str:
    """Strip all non-digits (including non-breaking spaces), return last 9 digits."""
    digits = re.sub(r"[^\d]", "", raw)   # strips spaces, dashes, Â, \u00a0, etc.
    return digits[-9:] if len(digits) >= 9 else digits

def load_google_cache() -> dict:
    try:
        with open(GOOGLE_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _waha_headers() -> dict:
    return {"X-Api-Key": WAHA_API_KEY, "Content-Type": "application/json"}


_NOWEB_SESSION_CONFIG = {
    "noweb": {"markOnline": True, "store": {"enabled": True, "fullSync": True}},
    "webhooks": [{"url": WEBHOOK_URL, "events": ["*"]}],
}


def _config_needs_update(config: dict) -> bool:
    store_ok   = (config.get("noweb") or {}).get("store", {}).get("enabled", False)
    webhook_ok = any(w.get("url") == WEBHOOK_URL for w in (config.get("webhooks") or []))
    return not store_ok or not webhook_ok


def ensure_whatsapp_instance() -> bool:
    """Start the WAHA session if not already running."""
    try:
        r = requests.get(f"{WAHA_URL}/api/sessions/{WAHA_SESSION}", headers=_waha_headers(), timeout=10)
        if r.ok:
            data = r.json()
            status = data.get("status", "STOPPED")
            config = data.get("config") or {}
            if _config_needs_update(config):
                requests.put(
                    f"{WAHA_URL}/api/sessions/{WAHA_SESSION}",
                    headers=_waha_headers(),
                    json={"config": _NOWEB_SESSION_CONFIG},
                    timeout=10,
                )
            elif status == "STOPPED":
                requests.post(
                    f"{WAHA_URL}/api/sessions/{WAHA_SESSION}/start",
                    headers=_waha_headers(),
                    timeout=10,
                )
            return True
        # Session doesn't exist — create it
        requests.post(
            f"{WAHA_URL}/api/sessions",
            headers=_waha_headers(),
            json={"name": WAHA_SESSION, "config": _NOWEB_SESSION_CONFIG},
            timeout=10,
        )
        return True
    except Exception as exc:
        logger.warning("Could not ensure WAHA session: %s", exc)
        return False


def whatsapp_connection_status() -> dict:
    """Return {'state': 'open'|'connecting'|'unknown', 'qr': '<base64 or None>'}."""
    try:
        r = requests.get(f"{WAHA_URL}/api/sessions/{WAHA_SESSION}", headers=_waha_headers(), timeout=8)
        if not r.ok:
            return {"state": "unknown", "qr": None}
        status = r.json().get("status", "STOPPED")

        if status == "WORKING":
            return {"state": "open", "qr": None}

        qr_b64 = None
        if status == "SCAN_QR_CODE":
            qr_r = requests.get(
                f"{WAHA_URL}/api/{WAHA_SESSION}/auth/qr",
                headers=_waha_headers(),
                timeout=8,
            )
            if qr_r.ok:
                import base64
                qr_b64 = "data:image/png;base64," + base64.b64encode(qr_r.content).decode()

        return {"state": "connecting", "qr": qr_b64}
    except Exception as exc:
        logger.warning("Could not fetch WAHA status: %s", exc)
        return {"state": "unknown", "qr": None}


def send_whatsapp(message: str) -> bool:
    """Send *message* to every contact flagged birthday=True in contacts.json."""
    recipients = [c for c in load_contacts() if c.get("birthday")]

    # Fallback: legacy WHATSAPP_RECIPIENTS env var
    if not recipients:
        for number in os.environ.get("WHATSAPP_RECIPIENTS", "").split(","):
            number = number.strip()
            if number:
                recipients.append({"label": number, "chatId": f"{number}@c.us"})

    if not recipients:
        logger.warning("No birthday recipients configured — skipping notification")
        return False

    all_ok = True
    for c in recipients:
        chat_id = c["chatId"]
        label   = c.get("label", chat_id)
        try:
            resp = requests.post(
                f"{WAHA_URL}/api/sendText",
                headers=_waha_headers(),
                json={"chatId": chat_id, "text": message, "session": WAHA_SESSION},
                timeout=15,
            )
            if resp.ok:
                logger.info("WhatsApp sent to %s (%s)", label, chat_id)
            else:
                logger.error("WAHA error for %s: %s", label, resp.text[:200])
                all_ok = False
        except Exception as exc:
            logger.error("WhatsApp send failed for %s: %s", label, exc)
            all_ok = False

    return all_ok


def run_daily_check():
    """Called by the scheduler every day at the configured time."""
    hyear, hmonth, hday = today_hebrew()
    date_str = format_hebrew_date(hday, hmonth, hyear)
    logger.info("Running daily birthday check for %s", date_str)

    people = get_people_with_birthday_on(hday, hmonth, hyear)
    if not people:
        logger.info("No birthdays today.")
        return

    for person in people:
        lines = [
            "Happy Birthday! 🎂",
            f"Today ({date_str}) is the Hebrew birthday of {person['name']}!",
        ]
        if person.get("notes"):
            lines.append(f"Note: {person['notes']}")
        send_whatsapp("\n".join(lines))


# ---------------------------------------------------------------------------
# PWA assets
# ---------------------------------------------------------------------------

_ICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
  <rect width="100" height="100" fill="white"/>
  <ellipse cx="50" cy="12" rx="5" ry="7" fill="#FF8C00"/>
  <ellipse cx="50" cy="14" rx="3" ry="4" fill="#FFD700"/>
  <line x1="50" y1="19" x2="50" y2="14" stroke="#555" stroke-width="1.5"/>
  <rect x="46" y="19" width="8" height="15" rx="2" fill="#FFE066"/>
  <path d="M25 34 Q31 28 37 34 Q43 28 49 34 Q55 28 61 34 Q67 28 73 34 L73 54 L25 54 Z" fill="#FF85A1"/>
  <path d="M25 34 Q31 28 37 34 Q43 28 49 34 Q55 28 61 34 Q67 28 73 34" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round"/>
  <path d="M12 54 Q19 47 26 54 Q33 47 40 54 Q47 47 54 54 Q61 47 68 54 Q75 47 82 54 Q87 49 88 54 L88 84 L12 84 Z" fill="#FF6B9D"/>
  <path d="M12 54 Q19 47 26 54 Q33 47 40 54 Q47 47 54 54 Q61 47 68 54 Q75 47 82 54 Q87 49 88 54" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round"/>
  <circle cx="30" cy="70" r="3" fill="#FFE066"/>
  <circle cx="50" cy="70" r="3" fill="#FFE066"/>
  <circle cx="70" cy="70" r="3" fill="#FFE066"/>
  <ellipse cx="50" cy="86" rx="40" ry="4" fill="#E0E0E0"/>
</svg>"""

@app.route("/icon.svg")
def app_icon():
    return Response(_ICON_SVG, mimetype="image/svg+xml")

@app.route("/manifest.json")
def pwa_manifest():
    return jsonify({
        "name": "ציון תאריכים לפי תאריך עברי",
        "short_name": "ימי הולדת",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#ffffff",
        "theme_color": "#212529",
        "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml"}],
    })


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/login")
def login_page():
    if session.get("user_email"):
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/auth/google")
def auth_google():
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope":         "openid email profile",
        "access_type":   "online",
    }
    return redirect(_GOOGLE_AUTH_URL + "?" + urllib.parse.urlencode(params))


@app.route("/auth/google/callback")
def auth_google_callback():
    code  = request.args.get("code")
    error = request.args.get("error")
    if error or not code:
        flash("Google sign-in cancelled or failed.", "danger")
        return redirect(url_for("login_page"))

    def _exchange(c):
        tok = requests.post(_GOOGLE_TOKEN_URL, data={
            "code": c, "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI, "grant_type": "authorization_code",
        }, timeout=10)
        tok.raise_for_status()
        ui = requests.get(_GOOGLE_USERINFO_URL,
                          headers={"Authorization": f"Bearer {tok.json()['access_token']}"},
                          timeout=10)
        ui.raise_for_status()
        return ui.json()

    try:
        info = _https_pool.submit(_exchange, code).result(timeout=15)
    except Exception as exc:
        logger.error("Google OAuth error: %s", exc)
        flash("Google sign-in failed — please try again.", "danger")
        return redirect(url_for("login_page"))

    email   = info.get("email", "").lower().strip()
    name    = info.get("name", "")
    picture = info.get("picture", "")

    if not email:
        flash("Could not retrieve email from Google.", "danger")
        return redirect(url_for("login_page"))

    user = get_user_by_email(email)
    if user is None:
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if count == 0:
            conn.execute(
                "INSERT INTO users (email, name, picture, role, last_login) VALUES (?, ?, ?, 'administrator', CURRENT_TIMESTAMP)",
                (email, name, picture),
            )
            conn.commit()
            conn.close()
            user = {"role": "administrator"}
        else:
            conn.close()
            return render_template("access_denied.html", email=email)
    else:
        conn = get_db()
        conn.execute(
            "UPDATE users SET name=?, picture=?, last_login=CURRENT_TIMESTAMP WHERE email=?",
            (name, picture, email),
        )
        conn.commit()
        conn.close()

    session.permanent = True
    session["user_email"]   = email
    session["user_name"]    = name
    session["user_picture"] = picture
    session["user_role"]    = user["role"]
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/admin/users")
@admin_required
def admin_users():
    conn = get_db()
    users = [dict(r) for r in conn.execute("SELECT * FROM users ORDER BY role, email").fetchall()]
    conn.close()
    return render_template("admin_users.html", users=users,
                           roles=["administrator", "editor", "viewer"],
                           current_email=session.get("user_email", ""))


@app.route("/admin/users/add", methods=["POST"])
@admin_required
def admin_users_add():
    email = request.form.get("email", "").lower().strip()
    role  = request.form.get("role", "viewer")
    if role not in ("administrator", "editor", "viewer"):
        role = "viewer"
    if not email:
        flash("Email is required.", "danger")
        return redirect(url_for("admin_users"))
    try:
        conn = get_db()
        conn.execute("INSERT OR IGNORE INTO users (email, role) VALUES (?, ?)", (email, role))
        conn.commit()
        conn.close()
        flash(f"Added {email} as {role}.", "success")
    except Exception as exc:
        flash(f"Error: {exc}", "danger")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/update-role", methods=["POST"])
@admin_required
def admin_users_update_role():
    uid  = request.form.get("id", type=int)
    role = request.form.get("role", "viewer")
    if role not in ("administrator", "editor", "viewer"):
        role = "viewer"
    conn = get_db()
    conn.execute("UPDATE users SET role=? WHERE id=?", (role, uid))
    conn.commit()
    conn.close()
    flash("Role updated.", "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/delete/<int:uid>", methods=["POST"])
@admin_required
def admin_users_delete(uid):
    conn = get_db()
    row = conn.execute("SELECT email FROM users WHERE id=?", (uid,)).fetchone()
    conn.execute("DELETE FROM users WHERE id=?", (uid,))
    conn.commit()
    conn.close()
    if row:
        flash(f"Removed {row['email']}.", "success")
    return redirect(url_for("admin_users"))


@app.route("/")
@login_required
def index():
    hyear, hmonth, hday = today_hebrew()
    today_str = format_hebrew_date_he(hday, hmonth, hyear)

    conn = get_db()
    rows = conn.execute("SELECT * FROM people").fetchall()
    conn.close()
    people_dicts = [dict(r) for r in rows]
    for p in people_dicts:
        p["days_until"] = compute_days_until(p["hebrew_day"], p["hebrew_month"])
    people = sorted(people_dicts, key=lambda p: p["days_until"])

    todays_birthdays = get_people_with_birthday_on(hday, hmonth, hyear)
    for p in todays_birthdays:
        p["years_count"] = (hyear - p["hebrew_year"]) if p.get("hebrew_year") is not None else None
    upcoming = get_upcoming_birthdays(days_ahead=5)

    return render_template(
        "index.html",
        people=people,
        today_str=today_str,
        todays_birthdays=todays_birthdays,
        upcoming=upcoming,
        hebrew_months=HEBREW_MONTHS,
        hebrew_months_he=HEBREW_MONTHS_HE,
        month_days_info=MONTH_DAYS_INFO,
        current_hebrew_year=hyear,
    )


@app.route("/add", methods=["POST"])
@editor_required
def add_person():
    first_name = request.form.get("first_name", "").strip()
    last_name = request.form.get("last_name", "").strip()
    name = f"{first_name} {last_name}".strip()
    hebrew_day = request.form.get("hebrew_day", type=int)
    hebrew_month = request.form.get("hebrew_month", type=int)
    notes = request.form.get("notes", "").strip()
    gender = request.form.get("gender", "").strip()
    group_type = request.form.get("group_type", "").strip()
    event_type = request.form.get("event_type", "יום הולדת").strip()
    hebrew_year = request.form.get("hebrew_year_ui", type=int)

    if not first_name or not hebrew_day or not hebrew_month:
        flash("Please fill in first name, day, and month.", "danger")
        return redirect(url_for("index"))

    conn = get_db()
    conn.execute(
        "INSERT INTO people (name, first_name, last_name, hebrew_day, hebrew_month, hebrew_year, notes, gender, group_type, event_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (name, first_name, last_name, hebrew_day, hebrew_month, hebrew_year, notes, gender, group_type, event_type),
    )
    conn.commit()
    conn.close()
    flash(f"Added {name}!", "success")
    return redirect(url_for("index"))


@app.route("/edit/<int:pid>", methods=["GET", "POST"])
@editor_required
def edit_person(pid):
    conn = get_db()
    if request.method == "POST":
        first_name = request.form.get("first_name", "").strip()
        last_name = request.form.get("last_name", "").strip()
        name = f"{first_name} {last_name}".strip()
        hebrew_day = request.form.get("hebrew_day", type=int)
        hebrew_month = request.form.get("hebrew_month", type=int)
        notes = request.form.get("notes", "").strip()
        gender = request.form.get("gender", "").strip()
        group_type = request.form.get("group_type", "").strip()
        event_type = request.form.get("event_type", "יום הולדת").strip()
        hebrew_year = request.form.get("hebrew_year_ui", type=int)
        conn.execute(
            "UPDATE people SET name=?, first_name=?, last_name=?, hebrew_day=?, hebrew_month=?, hebrew_year=?, notes=?, gender=?, group_type=?, event_type=? WHERE id=?",
            (name, first_name, last_name, hebrew_day, hebrew_month, hebrew_year, notes, gender, group_type, event_type, pid),
        )
        conn.commit()
        conn.close()
        flash(f"Updated {name}.", "success")
        return redirect(url_for("index"))

    person = conn.execute("SELECT * FROM people WHERE id=?", (pid,)).fetchone()
    conn.close()
    if not person:
        flash("Person not found.", "danger")
        return redirect(url_for("index"))
    return render_template(
        "edit.html",
        person=dict(person),
        hebrew_months=HEBREW_MONTHS,
        month_days_info=MONTH_DAYS_INFO,
        current_hebrew_year=today_hebrew()[0],
    )


@app.route("/delete/<int:pid>", methods=["POST"])
@editor_required
def delete_person(pid):
    conn = get_db()
    row = conn.execute("SELECT name FROM people WHERE id=?", (pid,)).fetchone()
    conn.execute("DELETE FROM people WHERE id=?", (pid,))
    conn.commit()
    conn.close()
    if row:
        flash(f"Deleted {row['name']}.", "success")
    return redirect(url_for("index"))


@app.route("/whatsapp-setup")
@login_required
def whatsapp_setup():
    ensure_whatsapp_instance()
    status = whatsapp_connection_status()
    return render_template("whatsapp_setup.html", status=status)


@app.route("/api/year-info/<int:year>")
def api_year_info(year):
    """Return month list with exact day counts for a specific Hebrew year."""
    if not (5700 <= year <= 5800):
        return jsonify({"error": "Year out of range"}), 400
    try:
        leap = is_leap_year(year)

        # Year length determines Cheshvan/Kislev days:
        # deficient (mod 10 == 3): Cheshvan=29, Kislev=29
        # regular   (mod 10 == 4): Cheshvan=29, Kislev=30
        # complete  (mod 10 == 5): Cheshvan=30, Kislev=30
        d1 = HebrewDate(year, 7, 1).to_pydate()
        d2 = HebrewDate(year + 1, 7, 1).to_pydate()
        year_days = (d2 - d1).days
        remainder = year_days % 10
        cheshvan_days = 30 if remainder == 5 else 29
        kislev_days   = 29 if remainder == 3 else 30

        day_counts = {
            1: 30, 2: 29, 3: 30, 4: 29, 5: 30, 6: 29,
            7: 30, 8: cheshvan_days, 9: kislev_days,
            10: 29, 11: 30,
            12: 30 if leap else 29,
            13: 29,
        }
        month_nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        if leap:
            month_nums.append(13)

        months = [{"num": m, "name": HEBREW_MONTHS[m], "days": day_counts[m]}
                  for m in month_nums]
        return jsonify({"year": year, "leap": leap, "months": months})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/check-today")
@login_required
def api_check_today():
    """Manually trigger today's birthday check (also sends WhatsApp if configured)."""
    run_daily_check()
    hyear, hmonth, hday = today_hebrew()
    people = get_people_with_birthday_on(hday, hmonth, hyear)
    return jsonify({"date": format_hebrew_date(hday, hmonth, hyear), "birthdays": people})


_EVENT_ICONS = {
    "יום הולדת":  "🎂",
    "יום נישואין": "💍",
    "יום פטירה":  "🕯️",
    "אחר":        "📌",
}


def build_upcoming_message(days_ahead: int = 30) -> str | None:
    """Return the upcoming-events WhatsApp message, or None if no events."""
    upcoming = get_upcoming_birthdays(days_ahead=days_ahead)
    if not upcoming:
        return None
    lines = [f"📅 *אירועים ל-{days_ahead} הימים הבאים*", ""]
    for p in upcoming:
        event    = p.get("event_type") or "יום הולדת"
        icon     = _EVENT_ICONS.get(event, "📌")
        name     = f"{p['first_name']} {p.get('last_name') or ''}".strip()
        years    = p.get("years_count")
        year_str = f" (שנה {years})" if years is not None and years >= 0 else ""
        if p["days_until"] == 0:
            when = "היום! 🎉"
        elif p["days_until"] == 1:
            when = "מחר"
        else:
            when = f"בעוד {p['days_until']} ימים"
        lines.append(f"{icon} *{name}* — {event}{year_str}")
        lines.append(f"   {p['next_hebrew_date_he']}  |  {p['next_gregorian']}")
        lines.append(f"   {when}")
        lines.append("")
    return "\n".join(lines).rstrip()


def build_month_message(month: int) -> str | None:
    """Return a WhatsApp message for all events in Hebrew month *month*, or None if none."""
    today = _today()
    h_today = HebrewDate.from_pydate(today)
    current_leap = is_leap_year(h_today.year)

    if not current_leap and month in (12, 13):
        months_to_query = (12, 13)
    else:
        months_to_query = (month,)

    conn = get_db()
    placeholders = ",".join("?" for _ in months_to_query)
    rows = conn.execute(
        f"SELECT * FROM people WHERE hebrew_month IN ({placeholders}) ORDER BY hebrew_day",
        months_to_query,
    ).fetchall()
    conn.close()
    items = []
    for row in rows:
        p = dict(row)
        next_hdate = None
        for year_offset in range(2):
            cy = h_today.year + year_offset
            m = p["hebrew_month"]
            if m == 12 and is_leap_year(cy):
                m = 13
            elif m == 13 and not is_leap_year(cy):
                m = 12
            try:
                hd = HebrewDate(cy, m, p["hebrew_day"])
                if (hd.to_pydate() - today).days >= 0:
                    next_hdate = hd
                    break
            except Exception:
                pass
        if next_hdate is None:
            continue
        greg = next_hdate.to_pydate()
        days_until = (greg - today).days
        years = (next_hdate.year - p["hebrew_year"]) if p.get("hebrew_year") is not None else None
        items.append({**p, "days_until": days_until,
                      "next_hebrew_date_he": format_hebrew_date_he(next_hdate.day, next_hdate.month, next_hdate.year),
                      "next_gregorian": greg.strftime("%d/%m/%Y"), "years_count": years})

    if not items:
        return None

    items.sort(key=lambda x: x["days_until"])
    month_name_he = HEBREW_MONTHS_HE.get(month, str(month))
    lines = [f"📅 *אירועים בחודש {month_name_he}*", ""]
    for p in items:
        event    = p.get("event_type") or "יום הולדת"
        icon     = _EVENT_ICONS.get(event, "📌")
        name     = f"{p['first_name']} {p.get('last_name') or ''}".strip()
        years    = p.get("years_count")
        year_str = f" (שנה {years})" if years is not None and years >= 0 else ""
        if p["days_until"] == 0:
            when = "היום! 🎉"
        elif p["days_until"] == 1:
            when = "מחר"
        else:
            when = f"בעוד {p['days_until']} ימים"
        lines.append(f"{icon} *{name}* — {event}{year_str}")
        lines.append(f"   {p['next_hebrew_date_he']}  |  {p['next_gregorian']}")
        lines.append(f"   {when}")
        lines.append("")
    return "\n".join(lines).rstrip()


@app.route("/api/send-month-events/<int:month>", methods=["POST"])
@login_required
def send_month_events(month):
    if month < 1 or month > 13:
        return jsonify({"ok": False, "error": "invalid month"}), 400
    message = build_month_message(month)
    if not message:
        return jsonify({"ok": False, "error": "אין אירועים בחודש זה"})
    recipients = [c for c in load_contacts() if c.get("birthday")]
    if not recipients:
        return jsonify({"ok": False, "error": "לא הוגדרו נמענים לרשימת יום הולדת"})
    all_ok = True
    for c in recipients:
        try:
            resp = requests.post(
                f"{WAHA_URL}/api/sendText",
                headers=_waha_headers(),
                json={"chatId": c["chatId"], "text": message, "session": WAHA_SESSION},
                timeout=15,
            )
            if not resp.ok:
                logger.error("send-month-events error for %s: %s", c.get("label"), resp.text[:200])
                all_ok = False
        except Exception as exc:
            logger.error("send-month-events failed for %s: %s", c.get("label"), exc)
            all_ok = False
    return jsonify({"ok": all_ok, "count": len(recipients)})


@app.route("/api/send-upcoming-list", methods=["POST"])
@login_required
def send_upcoming_list():
    days = request.args.get("days", 30, type=int)
    message = build_upcoming_message(days_ahead=days)
    if not message:
        return jsonify({"ok": False, "error": "אין אירועים בשלושים הימים הבאים"})

    recipients = [c for c in load_contacts() if c.get("birthday")]
    if not recipients:
        return jsonify({"ok": False, "error": "לא הוגדרו נמענים לרשימת יום הולדת"})

    all_ok = True
    for c in recipients:
        try:
            resp = requests.post(
                f"{WAHA_URL}/api/sendText",
                headers=_waha_headers(),
                json={"chatId": c["chatId"], "text": message, "session": WAHA_SESSION},
                timeout=15,
            )
            if not resp.ok:
                logger.error("send-upcoming-list error for %s: %s", c.get("label"), resp.text[:200])
                all_ok = False
        except Exception as exc:
            logger.error("send-upcoming-list failed for %s: %s", c.get("label"), exc)
            all_ok = False

    return jsonify({"ok": all_ok, "count": len(recipients)})


@app.route("/webhook", methods=["POST"])
def webhook():
    data    = request.get_json(silent=True) or {}
    event   = data.get("event", "")
    payload = data.get("payload", {})
    # WAHA uses "message.any" for all messages (including sent by linked device)
    if event not in ("message", "message.any"):
        return jsonify({"ok": True})

    body    = (payload.get("body") or "").strip()
    chat_id = payload.get("from", "") or payload.get("chatId", "")

    logger.info("WEBHOOK event=%s chat=%s body=%r fromMe=%s", event, chat_id, body[:50], payload.get("fromMe"))

    # Match /upcoming or /upcoming<N>
    m = re.match(r'^/upcoming(\d+)?$', body, re.IGNORECASE)
    if not m or not chat_id.endswith("@g.us"):
        return jsonify({"ok": True})
    suffix = m.group(1)
    days_ahead = int(suffix) if suffix and int(suffix) < 30 else 30

    # Optionally verify the group name — if lookup fails or returns empty, allow through
    try:
        rg = requests.get(
            f"{WAHA_URL}/api/{WAHA_SESSION}/groups",
            headers=_waha_headers(), timeout=10,
        )
        if rg.ok:
            groups = rg.json() or {}
            group_info = groups.get(chat_id, {}) if isinstance(groups, dict) else {}
            subject = (group_info.get("subject", "") if isinstance(group_info, dict) else "")
            if subject and COMMAND_GROUP_NAME.lower() not in subject.lower():
                logger.info("Ignoring /upcoming from unrecognised group: %s (%s)", chat_id, subject)
                return jsonify({"ok": True})
    except Exception as exc:
        logger.warning("Could not verify group name: %s — proceeding anyway", exc)

    message = build_upcoming_message(days_ahead=days_ahead) or "📅 אין אירועים בטווח הימים המבוקש"
    try:
        requests.post(
            f"{WAHA_URL}/api/sendText",
            headers=_waha_headers(),
            json={"chatId": chat_id, "text": message, "session": WAHA_SESSION},
            timeout=15,
        )
        logger.info("Sent /upcoming reply to group %s", chat_id)
    except Exception as exc:
        logger.error("Webhook reply failed: %s", exc)

    return jsonify({"ok": True})


@app.route("/test")
@login_required
def test_send():
    """Send a test birthday message to all birthday-flagged contacts."""
    recipients = [c for c in load_contacts() if c.get("birthday")]
    if not recipients:
        return ("No contacts flagged for Birthday. "
                "<a href='/admin/contacts'>Go to Contacts</a>"), 200

    hyear, hmonth, hday = today_hebrew()
    date_str = format_hebrew_date(hday, hmonth, hyear)
    test_msg = f"🧪 Test message from Birthday Bot\nToday is {date_str}."

    results = []
    for c in recipients:
        try:
            resp = requests.post(
                f"{WAHA_URL}/api/sendText",
                headers=_waha_headers(),
                json={"chatId": c["chatId"], "text": test_msg, "session": WAHA_SESSION},
                timeout=15,
            )
            ok = resp.ok
        except Exception as exc:
            ok = False
            logger.error("Test send failed for %s: %s", c.get("label"), exc)
        results.append(f"{c.get('label', c['chatId'])}: {'✓ sent' if ok else '✗ FAILED'}")

    return (f"<pre>{'<br>'.join(results)}</pre>"
            f"<br><a href='/admin/contacts'>Back to Contacts</a>"
            f"&nbsp; <a href='/'>Back to Birthdays</a>")


@app.route("/api/test-network")
@login_required
def api_test_network():
    """Test if this container can reach the internet."""
    try:
        r = requests.get("https://www.google.com", timeout=5)
        return jsonify({"internet": True, "status": r.status_code})
    except Exception as e:
        return jsonify({"internet": False, "error": str(e)})


@app.route("/api/upcoming")
@login_required
def api_upcoming():
    days = request.args.get("days", 30, type=int)
    return jsonify(get_upcoming_birthdays(days_ahead=days))


@app.route("/api/month-events/<int:month>")
@login_required
def api_month_events(month):
    """Return all people whose Hebrew event month equals *month*, ordered by day."""
    if month < 1 or month > 13:
        return jsonify({"error": "invalid month"}), 400

    today = _today()
    h_today = HebrewDate.from_pydate(today)
    current_leap = is_leap_year(h_today.year)

    # In a non-leap year there is only one Adar; show both Adar I and Adar II stored people
    if not current_leap and month in (12, 13):
        months_to_query = (12, 13)
    else:
        months_to_query = (month,)

    conn = get_db()
    placeholders = ",".join("?" for _ in months_to_query)
    rows = conn.execute(
        f"SELECT * FROM people WHERE hebrew_month IN ({placeholders}) ORDER BY hebrew_day",
        months_to_query,
    ).fetchall()
    conn.close()
    result = []
    for row in rows:
        p = dict(row)
        next_hdate = None
        for year_offset in range(2):
            cy = h_today.year + year_offset
            m = p["hebrew_month"]
            if m == 12 and is_leap_year(cy):
                m = 13
            elif m == 13 and not is_leap_year(cy):
                m = 12
            try:
                hd = HebrewDate(cy, m, p["hebrew_day"])
                if (hd.to_pydate() - today).days >= 0:
                    next_hdate = hd
                    break
            except Exception:
                pass

        if next_hdate is None:
            continue

        greg = next_hdate.to_pydate()
        days_until = (greg - today).days
        years = (next_hdate.year - p["hebrew_year"]) if p.get("hebrew_year") is not None else None
        result.append({
            "id": p["id"],
            "first_name": p["first_name"],
            "last_name": p.get("last_name") or "",
            "event_type": p.get("event_type") or "יום הולדת",
            "days_until": days_until,
            "next_hebrew_date_he": format_hebrew_date_he(next_hdate.day, next_hdate.month, next_hdate.year),
            "next_gregorian": greg.strftime("%d/%m/%Y"),
            "years_count": years,
        })

    result.sort(key=lambda x: x["days_until"])
    return jsonify(result)


# ---------------------------------------------------------------------------
# Contacts admin — unified management for birthday + omer recipients
# ---------------------------------------------------------------------------

@app.route("/admin/contacts")
@admin_required
def admin_contacts():
    contacts     = load_contacts()
    contact_map  = {c["chatId"]: c for c in contacts}
    google_cache = load_google_cache()

    all_chats, chats_error = [], ""
    try:
        r = requests.get(f"{WAHA_URL}/api/{WAHA_SESSION}/chats",
                         headers=_waha_headers(), timeout=15,
                         params={"limit": 1000, "offset": 0})
        all_chats = r.json() or []
        if isinstance(all_chats, dict):
            chats_error = all_chats.get("message", str(all_chats))
            all_chats = []
    except Exception as exc:
        chats_error = str(exc)

    # Also fetch WAHA contacts list to catch people not in recent chats
    all_wa_contacts = []
    try:
        rc = requests.get(f"{WAHA_URL}/api/contacts/all",
                          headers=_waha_headers(), timeout=15,
                          params={"session": WAHA_SESSION})
        if rc.ok:
            all_wa_contacts = rc.json() or []
            if isinstance(all_wa_contacts, dict):
                all_wa_contacts = []
    except Exception:
        pass

    # Fetch lids (phone number mappings) — available even when chats/contacts are empty
    all_lids = []
    try:
        rl = requests.get(f"{WAHA_URL}/api/{WAHA_SESSION}/lids",
                          headers=_waha_headers(), timeout=15,
                          params={"limit": 500})
        if rl.ok:
            all_lids = rl.json() or []
            if isinstance(all_lids, dict):
                all_lids = []
    except Exception:
        pass

    # Build a set of chat IDs already covered by chats list
    chat_ids_seen = {c.get("id", "") for c in all_chats if c.get("id")}

    # Add contacts not in chat list as stub entries
    for wc in all_wa_contacts:
        cid = wc.get("id", "")
        if not cid or cid in chat_ids_seen or cid.endswith("@broadcast") or cid.endswith("@g.us"):
            continue
        all_chats.append({"id": cid, "name": wc.get("name", ""), "isGroup": False})
        chat_ids_seen.add(cid)

    # Add lids (phone contacts) not yet covered
    for lid_entry in all_lids:
        pn = lid_entry.get("pn", "")
        if not pn or pn in chat_ids_seen or pn.endswith("@broadcast") or pn.endswith("@g.us"):
            continue
        all_chats.append({"id": pn, "name": "", "isGroup": False})
        chat_ids_seen.add(pn)

    # Fetch groups separately — groups API returns a dict keyed by group ID
    try:
        rg = requests.get(f"{WAHA_URL}/api/{WAHA_SESSION}/groups",
                          headers=_waha_headers(), timeout=15,
                          params={"limit": 500})
        if rg.ok:
            groups_data = rg.json() or {}
            if isinstance(groups_data, dict):
                for gid, ginfo in groups_data.items():
                    if gid in chat_ids_seen:
                        continue
                    subject = ginfo.get("subject", "") if isinstance(ginfo, dict) else ""
                    all_chats.append({"id": gid, "name": subject, "isGroup": True})
                    chat_ids_seen.add(gid)
    except Exception:
        pass

    enriched = []
    for chat in all_chats:
        cid = chat.get("id", "")
        if not cid or cid.endswith("@broadcast"):
            continue
        phone     = cid.split("@")[0]
        phone_key = normalize_phone(phone)
        is_group  = chat.get("isGroup", False) or cid.endswith("@g.us")

        google_name = google_cache.get(phone_key, "")
        waha_name   = chat.get("name", "")
        display     = google_name or waha_name or ""

        existing = contact_map.get(cid, {})
        manual_label = existing.get("label", "")
        enriched.append({
            "chatId":      cid,
            "displayName": manual_label or display,
            "autoName":    display,
            "manualLabel": manual_label,
            "phone":       phone,
            "isGroup":     is_group,
            "birthday":    existing.get("birthday", False),
            "omer":        existing.get("omer", False),
        })

    # Always show contacts already saved in contacts.json, even when WAHA returns nothing
    enriched_ids = {e["chatId"] for e in enriched}
    for saved in contacts:
        cid = saved.get("chatId", "")
        if not cid or cid in enriched_ids:
            continue
        phone    = cid.split("@")[0]
        is_group = cid.endswith("@g.us")
        label    = saved.get("label", "")
        enriched.append({
            "chatId":      cid,
            "displayName": label or phone,
            "autoName":    "",
            "manualLabel": label,
            "phone":       phone,
            "isGroup":     is_group,
            "birthday":    saved.get("birthday", False),
            "omer":        saved.get("omer", False),
        })

    # Groups first, then named contacts A-Z by first word (= last name in "Last, First" format)
    # then unnamed contacts (raw phone numbers) at the bottom
    def _sort_key(x):
        name = x["displayName"]
        is_unnamed = not name or re.match(r'^[\d\s\+\-\(\)]+$', name)
        if is_unnamed:
            return (not x["isGroup"], 2, "", "")
        words = name.strip().split()
        primary   = words[0].lower().rstrip(",")   # last name (before the comma)
        secondary = words[1].lower().rstrip(",") if len(words) > 1 else ""
        return (not x["isGroup"], 1, primary, secondary)

    enriched.sort(key=_sort_key)

    return render_template(
        "contacts.html",
        chats=enriched,
        chats_error=chats_error,
    )


@app.route("/admin/contacts/toggle", methods=["POST"])
@admin_required
def toggle_contact():
    data    = request.get_json() or {}
    chat_id = data.get("chatId", "")
    flag    = data.get("flag", "")
    value   = bool(data.get("value"))
    label   = data.get("label") or chat_id

    if flag not in ("birthday", "omer") or not chat_id:
        return jsonify({"ok": False, "error": "invalid"}), 400

    contacts = load_contacts()
    existing = next((c for c in contacts if c["chatId"] == chat_id), None)

    if existing:
        existing[flag] = value
        if not existing.get("birthday") and not existing.get("omer") and not existing.get("label"):
            contacts = [c for c in contacts if c["chatId"] != chat_id]
    elif value:
        contacts.append({"label": label, "chatId": chat_id,
                         "birthday": flag == "birthday", "omer": flag == "omer"})

    save_contacts(contacts)
    return jsonify({"ok": True})


@app.route("/admin/contacts/set-label", methods=["POST"])
@admin_required
def set_contact_label():
    data    = request.get_json() or {}
    chat_id = data.get("chatId", "")
    label   = data.get("label", "").strip()

    if not chat_id:
        return jsonify({"ok": False, "error": "no chatId"}), 400

    contacts = load_contacts()
    existing = next((c for c in contacts if c["chatId"] == chat_id), None)

    if existing:
        existing["label"] = label
        if not existing.get("birthday") and not existing.get("omer") and not label:
            contacts = [c for c in contacts if c["chatId"] != chat_id]
    elif label:
        contacts.append({"label": label, "chatId": chat_id, "birthday": False, "omer": False})

    save_contacts(contacts)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def waha_health_check():
    """Ensure the WAHA session is running with the correct config. Called every 5 minutes."""
    try:
        r = requests.get(f"{WAHA_URL}/api/sessions/{WAHA_SESSION}", headers=_waha_headers(), timeout=10)
        if not r.ok:
            # Session gone — recreate it
            requests.post(
                f"{WAHA_URL}/api/sessions",
                headers=_waha_headers(),
                json={"name": WAHA_SESSION, "config": _NOWEB_SESSION_CONFIG},
                timeout=10,
            )
            logger.warning("WAHA session missing — recreated")
            return
        data   = r.json()
        status = data.get("status", "STOPPED")
        config = data.get("config") or {}
        if _config_needs_update(config):
            requests.put(
                f"{WAHA_URL}/api/sessions/{WAHA_SESSION}",
                headers=_waha_headers(),
                json={"config": _NOWEB_SESSION_CONFIG},
                timeout=10,
            )
            logger.warning("WAHA session config updated (store/webhook)")
        elif status == "STOPPED":
            requests.post(
                f"{WAHA_URL}/api/sessions/{WAHA_SESSION}/start",
                headers=_waha_headers(),
                timeout=10,
            )
            logger.warning("WAHA session was STOPPED — restarted")
    except Exception as exc:
        logger.warning("WAHA health check failed: %s", exc)


def backup_database():
    """Copy the live DB to BACKUP_DIR using SQLite's online backup API. Keep 30 daily copies."""
    if not BACKUP_DIR:
        return
    backup_dir = Path(BACKUP_DIR)
    backup_dir.mkdir(parents=True, exist_ok=True)
    dest = backup_dir / f"birthdays-{_today().isoformat()}.db"
    try:
        src = sqlite3.connect(DATABASE)
        dst = sqlite3.connect(str(dest))
        src.backup(dst)
        dst.close()
        src.close()
        logger.info("DB backed up to %s", dest)
        for old in sorted(backup_dir.glob("birthdays-*.db"))[:-30]:
            old.unlink()
            logger.info("Removed old backup: %s", old)
    except Exception as exc:
        logger.error("DB backup failed: %s", exc)


def start_scheduler():
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        run_daily_check,
        CronTrigger(hour=NOTIFICATION_HOUR, minute=NOTIFICATION_MINUTE, timezone=TIMEZONE),
        id="daily_birthday_check",
        replace_existing=True,
    )
    from apscheduler.triggers.interval import IntervalTrigger
    scheduler.add_job(
        waha_health_check,
        IntervalTrigger(minutes=5),
        id="waha_health_check",
        replace_existing=True,
    )
    if BACKUP_DIR:
        scheduler.add_job(
            backup_database,
            CronTrigger(hour=3, minute=0, timezone=TIMEZONE),
            id="db_backup_night",
            replace_existing=True,
        )
        scheduler.add_job(
            backup_database,
            CronTrigger(hour=10, minute=30, timezone=TIMEZONE),
            id="db_backup_morning",
            replace_existing=True,
        )
        logger.info("DB backup scheduled at 03:00 and 10:30 → %s", BACKUP_DIR)
    scheduler.start()
    if BACKUP_DIR:
        backup_database()
    logger.info(
        "Scheduler started — daily check at %02d:%02d %s",
        NOTIFICATION_HOUR, NOTIFICATION_MINUTE, TIMEZONE,
    )
    return scheduler


if __name__ == "__main__":
    init_db()
    scheduler = start_scheduler()
    app.run(host="0.0.0.0", port=5000, debug=False)
