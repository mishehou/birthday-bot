import os
import re
import json
import logging
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from pyluach.dates import HebrewDate

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "birthday-bot-secret")


DATABASE = os.environ.get("DATABASE_PATH", "/data/birthdays.db")
NOTIFICATION_HOUR = int(os.environ.get("NOTIFICATION_HOUR", "9"))
NOTIFICATION_MINUTE = int(os.environ.get("NOTIFICATION_MINUTE", "0"))
TIMEZONE = os.environ.get("TIMEZONE", "Asia/Jerusalem")

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
        CREATE TABLE IF NOT EXISTS people (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            first_name  TEXT    NOT NULL DEFAULT '',
            last_name   TEXT    NOT NULL DEFAULT '',
            hebrew_day  INTEGER NOT NULL,
            hebrew_month INTEGER NOT NULL,
            notes       TEXT,
            gender      TEXT    DEFAULT '',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migrations: add columns to existing databases
    for col_def in [
        "ALTER TABLE people ADD COLUMN gender TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN first_name TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE people ADD COLUMN last_name TEXT NOT NULL DEFAULT ''",
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
# Hebrew calendar helpers
# ---------------------------------------------------------------------------

def today_hebrew():
    h = HebrewDate.today()
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


def get_people_with_birthday_on(hday: int, hmonth: int, hyear: int) -> list[dict]:
    """Return all people whose Hebrew birthday falls on (hday, hmonth) in hyear."""
    months_to_check = {hmonth}

    # Adar II (13) in a leap year also covers people stored with plain Adar (12)
    if hmonth == 13 and is_leap_year(hyear):
        months_to_check.add(12)

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
    today = date.today()

    for delta in range(1, days_ahead + 1):
        check_date = today + timedelta(days=delta)
        h = HebrewDate.from_pydate(check_date)
        people = get_people_with_birthday_on(h.day, h.month, h.year)
        for p in people:
            p["days_until"] = delta
            p["next_hebrew_date"] = format_hebrew_date(h.day, h.month, h.year)
            p["next_gregorian"] = check_date.strftime("%Y-%m-%d")
            upcoming.append(p)

    return upcoming


# ---------------------------------------------------------------------------
# Notification — WhatsApp via WAHA (self-hosted, linked device)
# ---------------------------------------------------------------------------

WAHA_URL     = os.environ.get("WAHA_URL", "http://waha:3000")
WAHA_API_KEY = os.environ.get("WAHA_API_KEY", "birthday-bot-key")
WAHA_SESSION = "default"

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
    "noweb": {"markOnline": True, "store": {"enabled": True, "fullSync": True}}
}


def ensure_whatsapp_instance() -> bool:
    """Start the WAHA session if not already running."""
    try:
        r = requests.get(f"{WAHA_URL}/api/sessions/{WAHA_SESSION}", headers=_waha_headers(), timeout=10)
        if r.ok:
            data = r.json()
            status = data.get("status", "STOPPED")
            config = data.get("config") or {}          # null config → treat as empty
            store_enabled = (config.get("noweb") or {}).get("store", {}).get("enabled", False)
            if not store_enabled:
                # Config missing or store not enabled — update config (WAHA will restart session)
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
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    hyear, hmonth, hday = today_hebrew()
    today_str = format_hebrew_date(hday, hmonth, hyear)

    conn = get_db()
    people = conn.execute(
        "SELECT * FROM people ORDER BY last_name, first_name"
    ).fetchall()
    conn.close()

    todays_birthdays = get_people_with_birthday_on(hday, hmonth, hyear)
    upcoming = get_upcoming_birthdays(days_ahead=30)

    return render_template(
        "index.html",
        people=people,
        today_str=today_str,
        todays_birthdays=todays_birthdays,
        upcoming=upcoming,
        hebrew_months=HEBREW_MONTHS,
        month_days_info=MONTH_DAYS_INFO,
        current_hebrew_year=hyear,
    )


@app.route("/add", methods=["POST"])
def add_person():
    first_name = request.form.get("first_name", "").strip()
    last_name = request.form.get("last_name", "").strip()
    name = f"{first_name} {last_name}".strip()
    hebrew_day = request.form.get("hebrew_day", type=int)
    hebrew_month = request.form.get("hebrew_month", type=int)
    notes = request.form.get("notes", "").strip()
    gender = request.form.get("gender", "").strip()

    if not first_name or not hebrew_day or not hebrew_month:
        flash("Please fill in first name, day, and month.", "danger")
        return redirect(url_for("index"))

    conn = get_db()
    conn.execute(
        "INSERT INTO people (name, first_name, last_name, hebrew_day, hebrew_month, notes, gender) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, first_name, last_name, hebrew_day, hebrew_month, notes, gender),
    )
    conn.commit()
    conn.close()
    flash(f"Added {name}!", "success")
    return redirect(url_for("index"))


@app.route("/edit/<int:pid>", methods=["GET", "POST"])
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
        conn.execute(
            "UPDATE people SET name=?, first_name=?, last_name=?, hebrew_day=?, hebrew_month=?, notes=?, gender=? WHERE id=?",
            (name, first_name, last_name, hebrew_day, hebrew_month, notes, gender, pid),
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
def api_check_today():
    """Manually trigger today's birthday check (also sends WhatsApp if configured)."""
    run_daily_check()
    hyear, hmonth, hday = today_hebrew()
    people = get_people_with_birthday_on(hday, hmonth, hyear)
    return jsonify({"date": format_hebrew_date(hday, hmonth, hyear), "birthdays": people})


@app.route("/test")
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
def api_test_network():
    """Test if this container can reach the internet."""
    try:
        r = requests.get("https://www.google.com", timeout=5)
        return jsonify({"internet": True, "status": r.status_code})
    except Exception as e:
        return jsonify({"internet": False, "error": str(e)})


@app.route("/api/upcoming")
def api_upcoming():
    days = request.args.get("days", 30, type=int)
    return jsonify(get_upcoming_birthdays(days_ahead=days))


# ---------------------------------------------------------------------------
# Contacts admin — unified management for birthday + omer recipients
# ---------------------------------------------------------------------------

@app.route("/admin/contacts")
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
        store_ok = (config.get("noweb") or {}).get("store", {}).get("enabled", False)
        if not store_ok:
            requests.put(
                f"{WAHA_URL}/api/sessions/{WAHA_SESSION}",
                headers=_waha_headers(),
                json={"config": _NOWEB_SESSION_CONFIG},
                timeout=10,
            )
            logger.warning("WAHA session config was missing store — restored")
        elif status == "STOPPED":
            requests.post(
                f"{WAHA_URL}/api/sessions/{WAHA_SESSION}/start",
                headers=_waha_headers(),
                timeout=10,
            )
            logger.warning("WAHA session was STOPPED — restarted")
    except Exception as exc:
        logger.warning("WAHA health check failed: %s", exc)


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
    scheduler.start()
    logger.info(
        "Scheduler started — daily check at %02d:%02d %s",
        NOTIFICATION_HOUR, NOTIFICATION_MINUTE, TIMEZONE,
    )
    return scheduler


if __name__ == "__main__":
    init_db()
    scheduler = start_scheduler()
    app.run(host="0.0.0.0", port=5000, debug=False)
