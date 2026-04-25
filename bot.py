# FULL FINAL BOT.PY
# Market Hisab Bot + AI Bangla Chat + Bazar Assistant + Need List + Auto Memory

import os
import json
import re
import asyncio
import traceback
import requests
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

# =========================================================
# ENV
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "180"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "60"))
SEND_LOW_PERSONAL = os.getenv("SEND_LOW_PERSONAL", "true").strip().lower() == "true"
SEND_ADMIN_DETAILS = os.getenv("SEND_ADMIN_DETAILS", "true").strip().lower() == "true"
ADMIN_USER_IDS = os.getenv("ADMIN_USER_IDS", "").strip()

AI_ENABLED = os.getenv("AI_ENABLED", "false").strip().lower() == "true"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

# =========================================================
# SHEETS
# =========================================================
SETTINGS_SHEET = "Settings"
BAZAR_SHEET = "Bazar_Entry"
PAYMENT_SHEET = "Payment_Entry"
TELEGRAM_SETUP_SHEET = "Telegram_Setup"
PENDING_BAZAR_SHEET = "Pending_Bazar"
NEED_LIST_SHEET = "Need_List"
USER_PERSONALITY_SHEET = "User_Personality"
BOT_CHAT_LOG_SHEET = "Bot_Chat_Log"

_cache: Dict[str, Any] = {
    "loaded_at": None,
    "spreadsheet_title": "",
    "sheet_titles": [],
    "settings_rows": [],
    "bazar_rows": [],
    "payment_rows": [],
    "telegram_rows": [],
    "selected_month": "",
    "low_threshold": 500.0,
    "member_map": {},
    "admin_group_id": "",
    "stats": None,
}

cache_lock = asyncio.Lock()
send_lock = asyncio.Lock()
low_alert_sent_cache = set()
processed_bazar_rows = set()
processed_payment_rows = set()
repair_mode = False

user_pending_bazar: Dict[str, Dict[str, Any]] = {}
user_pending_memory: Dict[str, Dict[str, Any]] = {}

BN_ITEM_MAP = {
    "chal": "চাল", "cal": "চাল", "rice": "চাল",
    "dal": "ডাল", "dhal": "ডাল",
    "tel": "তেল", "oil": "তেল",
    "alu": "আলু", "aloo": "আলু", "potato": "আলু",
    "mach": "মাছ", "mas": "মাছ", "fish": "মাছ",
    "murgi": "মুরগি", "chicken": "মুরগি",
    "dim": "ডিম", "egg": "ডিম",
    "peyaj": "পেঁয়াজ", "onion": "পেঁয়াজ",
    "rosun": "রসুন", "garlic": "রসুন",
    "ada": "আদা", "ginger": "আদা",
    "lobon": "লবণ", "salt": "লবণ",
    "chini": "চিনি", "sugar": "চিনি",
    "jal": "পানি", "water": "পানি",
    "sobji": "সবজি", "sabji": "সবজি", "torkari": "তরকারি",
}

NAME_MAP = {
    "alpha": "ALPHA", "আলফা": "ALPHA", "alfa": "ALPHA",
    "surjo": "SURJO", "surjo": "SURJO", "সূর্য": "SURJO", "surjo": "SURJO",
    "mony": "MONY", "money": "MONY", "মনি": "MONY",
    "alon": "ALON", "আলন": "ALON", "alon": "ALON",
}

# =========================================================
# BASIC HELPERS
# =========================================================
def require_env() -> None:
    missing = []
    if not BOT_TOKEN: missing.append("BOT_TOKEN")
    if not SPREADSHEET_ID: missing.append("SPREADSHEET_ID")
    if not GOOGLE_SERVICE_ACCOUNT_JSON: missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
    if missing:
        raise RuntimeError("Missing Railway variables: " + ", ".join(missing))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def parse_amount(value: Any) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    cleaned = "".join(ch for ch in s if ch.isdigit() or ch in ".-")
    try:
        return float(cleaned) if cleaned else 0.0
    except ValueError:
        return 0.0


def format_lkr(value: Any) -> str:
    amount = parse_amount(value)
    if amount == int(amount):
        return f"{int(amount):,}"
    return f"{amount:,.2f}"


def normalize_name(value: Any) -> str:
    return str(value or "").strip().upper()


def row_value(row: List[Any], index: int) -> str:
    return str(row[index]).strip() if index < len(row) else ""


def month_from_date(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m")
        except ValueError:
            pass
    if len(s) >= 7 and s[4] in "-/":
        return s[:7].replace("/", "-")
    return ""


def get_service_account_email() -> str:
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        return info.get("client_email", "")
    except Exception:
        return ""


def get_admin_ids() -> List[str]:
    return [x.strip() for x in ADMIN_USER_IDS.split(",") if x.strip()]


def is_admin(user_id: Any) -> bool:
    return str(user_id).strip() in get_admin_ids()

# =========================================================
# GOOGLE SHEET
# =========================================================
def get_gspread_client():
    require_env()
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet():
    return get_gspread_client().open_by_key(SPREADSHEET_ID)


def safe_update_cell(sheet_name: str, row: int, col: int, value: str) -> None:
    sh = get_spreadsheet().worksheet(sheet_name)
    sh.update_cell(row, col, value)


def append_row(sheet_name: str, values: List[Any]) -> None:
    sh = get_spreadsheet().worksheet(sheet_name)
    sh.append_row(values, value_input_option="USER_ENTERED")


def get_sheet_rows(sheet_name: str) -> List[List[str]]:
    try:
        return get_spreadsheet().worksheet(sheet_name).get_all_values()
    except Exception:
        return []


def ensure_extra_sheets() -> None:
    ss = get_spreadsheet()
    existing = {w.title for w in ss.worksheets()}
    headers = {
        PENDING_BAZAR_SHEET: ["ID", "DateTime", "User ID", "Member", "Raw Message", "Items", "Total", "Status", "Admin Status", "Final Row", "Note"],
        NEED_LIST_SHEET: ["ID", "DateTime", "User ID", "Member", "Item", "Status", "Bought By", "Bought Date", "Source Message", "Note"],
        USER_PERSONALITY_SHEET: ["User ID", "Name", "Nickname", "Relation", "Tags", "Inside Jokes", "Bot Style", "Notes", "Last Updated"],
        BOT_CHAT_LOG_SHEET: ["DateTime", "User ID", "Member", "Message", "Bot Reply", "Type", "Status"],
    }
    for title, header in headers.items():
        if title not in existing:
            ws = ss.add_worksheet(title=title, rows=200, cols=max(len(header), 10))
            ws.append_row(header, value_input_option="USER_ENTERED")
        else:
            ws = ss.worksheet(title)
            first = ws.row_values(1)
            if not first:
                ws.append_row(header, value_input_option="USER_ENTERED")


def load_all_data_from_google() -> Dict[str, Any]:
    ss = get_spreadsheet()
    worksheets = ss.worksheets()
    title_map = {w.title: w for w in worksheets}

    def get_rows(sheet_name: str) -> List[List[str]]:
        if sheet_name not in title_map:
            return []
        return title_map[sheet_name].get_all_values()

    settings_rows = get_rows(SETTINGS_SHEET)
    bazar_rows = get_rows(BAZAR_SHEET)
    payment_rows = get_rows(PAYMENT_SHEET)
    telegram_rows = get_rows(TELEGRAM_SETUP_SHEET)

    selected_month = ""
    low_threshold = 500.0
    member_map: Dict[str, str] = {}
    admin_group_id = ""

    if len(settings_rows) >= 5:
        b3 = row_value(settings_rows[2], 1)
        b2 = row_value(settings_rows[1], 1) if len(settings_rows) >= 2 else ""
        selected_month = month_from_date(b3 or b2) or (b3 or b2)
        low_threshold = parse_amount(row_value(settings_rows[4], 1)) or 500.0

    for r in range(8, min(12, len(settings_rows))):
        row = settings_rows[r]
        name = normalize_name(row_value(row, 1))
        user_id = row_value(row, 2)
        active = normalize_name(row_value(row, 3))
        if name and user_id and active == "YES":
            member_map[name] = str(user_id).strip()

    if len(telegram_rows) >= 4:
        admin_group_id = row_value(telegram_rows[3], 1)

    data = {
        "loaded_at": now_utc(),
        "spreadsheet_title": ss.title,
        "sheet_titles": [w.title for w in worksheets],
        "settings_rows": settings_rows,
        "bazar_rows": bazar_rows,
        "payment_rows": payment_rows,
        "telegram_rows": telegram_rows,
        "selected_month": selected_month,
        "low_threshold": low_threshold,
        "member_map": member_map,
        "admin_group_id": admin_group_id,
    }
    data["stats"] = build_stats_from_rows(data)
    return data


async def get_cached_data(force: bool = False) -> Dict[str, Any]:
    async with cache_lock:
        loaded_at = _cache.get("loaded_at")
        expired = not loaded_at or (now_utc() - loaded_at).total_seconds() > CACHE_TTL_SECONDS
        if force or expired or not _cache.get("stats"):
            fresh = await asyncio.to_thread(load_all_data_from_google)
            _cache.update(fresh)
        return dict(_cache)


async def refresh_cache() -> Dict[str, Any]:
    return await get_cached_data(force=True)

# =========================================================
# HISAB
# =========================================================
def get_wallet_status(wallet: float, threshold: float) -> str:
    if wallet < 0:
        return "NEGATIVE"
    if wallet < threshold:
        return "LOW"
    return "OK"


def build_stats_from_rows(data: Dict[str, Any]) -> Dict[str, Any]:
    month_key = data["selected_month"]
    threshold = data["low_threshold"]
    members = data["member_map"]
    stats = {"month": month_key, "threshold": threshold, "total_topup": 0.0, "total_expense": 0.0, "share_per_head": 0.0, "total_wallet_left": 0.0, "members": {}}

    for name in members:
        stats["members"][name] = {"topup": 0.0, "own_expense": 0.0, "share_deduction": 0.0, "wallet": 0.0, "status": "OK"}

    for row in data["payment_rows"][3:]:
        row_month = month_from_date(row_value(row, 0))
        member = normalize_name(row_value(row, 1))
        amount = parse_amount(row_value(row, 2))
        if row_month == month_key and member in stats["members"] and amount:
            stats["members"][member]["topup"] += amount
            stats["total_topup"] += amount

    for row in data["bazar_rows"][3:]:
        row_month = month_from_date(row_value(row, 0))
        buyer = normalize_name(row_value(row, 1))
        total = parse_amount(row_value(row, 3))
        if row_month == month_key and total:
            stats["total_expense"] += total
            if buyer in stats["members"]:
                stats["members"][buyer]["own_expense"] += total

    count = max(len(stats["members"]), 1)
    stats["share_per_head"] = stats["total_expense"] / count
    for name, m in stats["members"].items():
        m["share_deduction"] = stats["share_per_head"]
        m["wallet"] = m["topup"] - m["share_deduction"]
        m["status"] = get_wallet_status(m["wallet"], threshold)
        stats["total_wallet_left"] += m["wallet"]
    return stats


def get_member_name_by_user_id(data: Dict[str, Any], user_id: Any) -> Optional[str]:
    user_id = str(user_id).strip()
    for name, uid in data["member_map"].items():
        if str(uid).strip() == user_id:
            return name
    return None

# =========================================================
# MESSAGE BUILDERS
# =========================================================
def build_help_message() -> str:
    return (
        "👋 Market Hisab Bot\n\n"
        "📌 Member Commands:\n"
        "/wallet - My wallet details\n"
        "/summary - Full month summary\n"
        "/low - Low wallet list\n"
        "/bazarlist - দরকারি বাজারের লিস্ট\n"
        "/cancel - Pending কাজ cancel\n"
        "/id - My Telegram ID\n\n"
        "🛠 Admin Commands:\n"
        "/debug - Check sheet connection\n"
        "/refresh - Refresh sheet cache\n"
        "/repair_on - Repair mode ON\n"
        "/repair_off - Repair mode OFF\n"
        "/status - Bot status\n"
        "/approve ID - Pending bazar approve\n"
        "/reject ID - Pending bazar reject\n\n"
        "💬 Examples:\n"
        "চাল শেষ ডাল শেষ তেল শেষ\n"
        "আলু 128 পানি 24 চাল 450 ডাল 127 মাছ 1200\n"
        "Alon amar mama, se chitar\n\n"
        f"🔧 Repair Mode: {'ON 🔧' if repair_mode else 'OFF ✅'}\n"
        f"🤖 AI Chat: {'ON 🤖' if AI_ENABLED and GEMINI_API_KEY else 'OFF'}"
    )


def build_wallet_message(member_name: str, stats: Dict[str, Any]) -> str:
    m = stats["members"].get(member_name)
    if not m:
        return "❌ Wallet data পাওয়া যায়নি।"
    return (
        "💼 MY WALLET STATUS\n\n"
        f"👤 Member: {member_name}\n📅 Month: {stats['month']}\n"
        f"➕ Top-up: {format_lkr(m['topup'])} LKR\n"
        f"🛒 Own Bazar: {format_lkr(m['own_expense'])} LKR\n"
        f"👥 Monthly Share: {format_lkr(m['share_deduction'])} LKR\n"
        f"💰 Current Wallet: {format_lkr(m['wallet'])} LKR\n"
        f"📌 Status: {m['status']}\n\n"
        f"📊 Month Total Expense: {format_lkr(stats['total_expense'])} LKR\n"
        f"💼 Total Wallet Left: {format_lkr(stats['total_wallet_left'])} LKR"
    )


def build_summary_message(stats: Dict[str, Any]) -> str:
    msg = (
        "📊 CURRENT MONTH SUMMARY\n\n"
        f"📅 Month: {stats['month']}\n"
        f"➕ Total Top-up: {format_lkr(stats['total_topup'])} LKR\n"
        f"🛒 Total Bazar: {format_lkr(stats['total_expense'])} LKR\n"
        f"👥 Per Person Share: {format_lkr(stats['share_per_head'])} LKR\n"
        f"💼 Total Wallet Left: {format_lkr(stats['total_wallet_left'])} LKR\n\n"
    )
    for name, m in stats["members"].items():
        msg += f"👤 {name}\n➕ Top-up: {format_lkr(m['topup'])} LKR\n🛒 Own Bazar: {format_lkr(m['own_expense'])} LKR\n👥 Share: {format_lkr(m['share_deduction'])} LKR\n💰 Wallet: {format_lkr(m['wallet'])} LKR\n📌 Status: {m['status']}\n\n"
    return msg


def build_low_wallet_list(stats: Dict[str, Any]) -> str:
    threshold = stats["threshold"]
    msg = f"⚠️ LOW WALLET MEMBERS\n\n📅 Month: {stats['month']}\n📉 Threshold: {format_lkr(threshold)} LKR\n\n"
    found = False
    for name, m in stats["members"].items():
        if m["wallet"] < threshold:
            found = True
            suggested = threshold - m["wallet"]
            msg += f"👤 {name}\n💸 Wallet: {format_lkr(m['wallet'])} LKR\n💳 Suggested Top-up: {format_lkr(suggested)} LKR\n📌 Status: {m['status']}\n\n"
    if not found:
        msg += "✅ এখন কোনো low wallet member নেই।"
    return msg


def build_bazar_message(entry: Dict[str, Any], stats: Dict[str, Any], member_name: str) -> str:
    m = stats["members"].get(member_name, {"wallet": 0, "status": "OK"})
    return (
        "🛒 BAZAR UPDATE\n\n"
        f"👤 Buyer: {entry['buyer']}\n📅 Date: {entry['date']}\n🧾 Type: {entry['type']}\n"
        f"💰 Total Expense: {format_lkr(entry['total'])} LKR\n👥 Per Person Share: {format_lkr(entry['share'])} LKR\n\n"
        f"👷 Your Name: {member_name}\n💼 Your Wallet Now: {format_lkr(m['wallet'])} LKR\n📌 Status: {m['status']}\n\n"
        f"📊 Month Total Expense: {format_lkr(stats['total_expense'])} LKR\n💼 Total Wallet Left: {format_lkr(stats['total_wallet_left'])} LKR"
    )


def build_payment_message(entry: Dict[str, Any], wallet_now: float, threshold: float) -> str:
    return (
        "💳 WALLET TOP-UP / ADJUSTMENT\n\n"
        f"👤 Member: {entry['member']}\n📅 Date: {entry['date']}\n➕ Amount: {format_lkr(entry['amount'])} LKR\n"
        f"📦 Type: {entry['type']}\n📝 Note: {entry['note']}\n\n"
        f"💼 Your Wallet Now: {format_lkr(wallet_now)} LKR\n📌 Status: {get_wallet_status(wallet_now, threshold)}"
    )


def build_low_wallet_personal(member: str, wallet: float, status: str, threshold: float) -> str:
    suggested = threshold - wallet if wallet < threshold else 0
    return f"⚠️ LOW WALLET ALERT\n\n👤 Member: {member}\n💸 Current Wallet: {format_lkr(wallet)} LKR\n💳 Suggested Top-up: {format_lkr(suggested)} LKR\n📌 Status: {status}"

# =========================================================
# PARSERS
# =========================================================
def bangla_item(word: str) -> str:
    return BN_ITEM_MAP.get(word.strip().lower(), word.strip())


def normalize_items_text(text: str) -> str:
    parts = re.split(r"[,\s]+", text.strip())
    cleaned = []
    for p in parts:
        p = p.strip()
        if p:
            cleaned.append(bangla_item(p))
    return ", ".join(dict.fromkeys(cleaned))


def parse_bazar_text(text: str) -> Optional[Dict[str, Any]]:
    raw = text.strip()
    low = raw.lower()
    numbers = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", low)]
    if not numbers:
        return None

    total_match = re.search(r"(total|মোট|mot)\s*[:=]?\s*(\d+(?:\.\d+)?)", low)
    if total_match:
        total = float(total_match.group(2))
        item_part = re.sub(r"(total|মোট|mot)\s*[:=]?\s*\d+(?:\.\d+)?", "", raw, flags=re.I)
        item_part = re.sub(r"\d+(?:\.\d+)?", "", item_part)
        items = normalize_items_text(item_part)
        if items:
            return {"items": items, "total": total, "note": raw, "type": "বাজার"}

    pairs = re.findall(r"([A-Za-z\u0980-\u09FF]+)\s*[:=]?\s*(\d+(?:\.\d+)?)", raw)
    if len(pairs) >= 2:
        total = sum(float(amount) for _, amount in pairs)
        items = ", ".join(dict.fromkeys([bangla_item(name) for name, _ in pairs]))
        return {"items": items, "total": total, "note": raw, "type": "বাজার"}
    return None


def parse_need_list_text(text: str) -> List[str]:
    raw = text.strip()
    low = raw.lower()
    need_words = ["শেষ", "ses", "shesh", "নেই", "nai", "ফুরিয়ে", "ফুরাইছে"]
    if not any(w in low for w in need_words):
        return []
    cleaned = low
    for w in need_words:
        cleaned = cleaned.replace(w, " ")
    cleaned = re.sub(r"[^\w\u0980-\u09FF\s,]", " ", cleaned)
    parts = re.split(r"[,\s]+", cleaned)
    items = []
    ignore = {"ajke", "aaj", "আজকে", "আজ", "amader", "আমাদের", "ar", "আর", "o", "ও", "ta", "টা"}
    for p in parts:
        p = p.strip()
        if not p or p in ignore:
            continue
        item = bangla_item(p)
        if item and item not in items:
            items.append(item)
    return items


def generate_id(prefix: str = "BZ") -> str:
    return f"{prefix}{datetime.now().strftime('%Y%m%d%H%M%S')}"

# =========================================================
# AUTO MEMORY + PERSONALITY
# =========================================================
def get_personality_rows() -> List[List[str]]:
    return get_sheet_rows(USER_PERSONALITY_SHEET)


def get_personality_notes() -> str:
    rows = get_personality_rows()
    notes = []
    for row in rows[1:]:
        name = row_value(row, 1)
        nickname = row_value(row, 2)
        relation = row_value(row, 3)
        tags = row_value(row, 4)
        jokes = row_value(row, 5)
        style = row_value(row, 6)
        note = row_value(row, 7)
        if name:
            notes.append(f"{name}: ডাকনাম={nickname}, সম্পর্ক={relation}, ট্যাগ={tags}, ভিতরের মজা={jokes}, স্টাইল={style}, নোট={note}")
    return "\n".join(notes[:30])


def save_chat_log(user_id: str, member: str, message: str, reply: str, typ: str, status: str = "OK") -> None:
    try:
        append_row(BOT_CHAT_LOG_SHEET, [now_str(), user_id, member, message, reply, typ, status])
    except Exception as exc:
        print("Chat log save failed:", exc)


def find_target_name(text: str) -> str:
    low = text.lower()
    for key, val in NAME_MAP.items():
        if key in low:
            return val
    return ""


def detect_memory_text(text: str, speaker_member: str) -> Optional[Dict[str, str]]:
    raw = text.strip()
    low = raw.lower()
    target = find_target_name(raw)
    if not target:
        return None

    memory_keywords = [
        "amar", "amr", "আমার", "mama", "মামা", "vai", "ভাই", "friend", "bondhu", "বন্ধু",
        "chitar", "cheater", "চিটার", "valo", "ভালো", "kharap", "খারাপ", "smart", "lazy", "লেজি", "boka", "বোকা",
        "hutas", "hutase", "হুটাস", "হুটাসে", "nator", "drama", "নাটক", "boss", "admin"
    ]
    if not any(k in low for k in memory_keywords):
        return None

    relation = ""
    nickname = ""
    tags = []
    inside_jokes = []
    bot_style = "funny roast"

    if "mama" in low or "মামা" in low:
        relation = f"{speaker_member}-এর মামা"
        tags.append("mama")
    if "vai" in low or "ভাই" in low:
        relation = relation or f"{speaker_member}-এর ভাই/বন্ধু"
    if "friend" in low or "bondhu" in low or "বন্ধু" in low:
        relation = relation or "বন্ধু"

    if "chitar" in low or "cheater" in low or "চিটার" in low:
        nickname = "চিটার মামা" if ("mama" in low or "মামা" in low) else "চিটার"
        inside_jokes.append("চিটার")
        tags.append("roast")
    if "hutase" in low or "hutas" in low or "হুটাস" in low or "হুটাসে" in low:
        inside_jokes.append("হুটাসে চলে")
        tags.append("inside-joke")
    if "smart" in low:
        tags.append("smart")
    if "lazy" in low or "লেজি" in low:
        tags.append("lazy")
    if "drama" in low or "নাটক" in low:
        tags.append("drama")
        inside_jokes.append("নাটক করে")
    if "boss" in low or "admin" in low:
        tags.append("boss")
        bot_style = "respect+fun"
    if "valo" in low or "ভালো" in low:
        tags.append("good")
    if "kharap" in low or "খারাপ" in low:
        tags.append("roast")

    return {
        "name": target,
        "nickname": nickname,
        "relation": relation,
        "tags": ",".join(dict.fromkeys(tags)),
        "inside_jokes": ", ".join(dict.fromkeys(inside_jokes)),
        "bot_style": bot_style,
        "notes": raw,
    }


def merge_old(old: str, new: str) -> str:
    old = str(old or "").strip()
    new = str(new or "").strip()
    if not new:
        return old
    if not old:
        return new
    if new.lower() in old.lower():
        return old
    return old + ", " + new


def upsert_user_personality(memory: Dict[str, str]) -> None:
    ss = get_spreadsheet()
    sh = ss.worksheet(USER_PERSONALITY_SHEET)
    rows = sh.get_all_values()
    name = memory["name"].strip().upper()
    now_date = datetime.now().strftime("%Y-%m-%d")

    target_row = None
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 1).upper() == name:
            target_row = idx
            break

    if target_row:
        old = rows[target_row - 1]
        sh.update_cell(target_row, 3, merge_old(row_value(old, 2), memory.get("nickname", "")))
        sh.update_cell(target_row, 4, merge_old(row_value(old, 3), memory.get("relation", "")))
        sh.update_cell(target_row, 5, merge_old(row_value(old, 4), memory.get("tags", "")))
        sh.update_cell(target_row, 6, merge_old(row_value(old, 5), memory.get("inside_jokes", "")))
        sh.update_cell(target_row, 7, merge_old(row_value(old, 6), memory.get("bot_style", "")))
        sh.update_cell(target_row, 8, merge_old(row_value(old, 7), memory.get("notes", "")))
        sh.update_cell(target_row, 9, now_date)
    else:
        sh.append_row([
            "", name, memory.get("nickname", ""), memory.get("relation", ""), memory.get("tags", ""),
            memory.get("inside_jokes", ""), memory.get("bot_style", ""), memory.get("notes", ""), now_date
        ], value_input_option="USER_ENTERED")


def safe_fun_reply(text: str, member: str) -> str:
    t = text.lower()
    if "alon" in t or "আলন" in t:
        return "আলন মামার কথা বলছো? ওইটা তো আমাদের গ্রুপের VIP চিটার 😂 তবে মানুষটা মন্দ না, শুধু চালাকি একটু বেশি 😏"
    if "surjo" in t or "সূর্য" in t:
        return "সূর্য ভাই মানে আলো আছে, কিন্তু বাজারের সময় মাঝে মাঝে মেঘ ঢেকে যায় 😂"
    if "mony" in t or "মনি" in t:
        return "মনি ভাই শান্ত টাইপ, কিন্তু হিসাবের সময় চুপচাপ সব দেখে রাখে 😄"
    if "alpha" in t or "আলফা" in t:
        return "AlphA ভাই তো এই সিস্টেমের boss 😎 বাজার, wallet, bot—সব জায়গায় control!"
    return f"হাহাহা {member or 'ভাই'}, কথা শুনে মনে হচ্ছে আজকে গ্রুপে আবার মজা শুরু হবে 😂"


def gemini_reply(user_text: str, member: str, personality_notes: str) -> Optional[str]:
    if not (AI_ENABLED and GEMINI_API_KEY):
        return None
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={GEMINI_API_KEY}"
    prompt = f"""
তুমি একটি Telegram market hisab bot, কিন্তু তুমি খাঁটি বাংলা ভাষায় বন্ধুর মতো কথা বলো।

Style:
- সব reply বাংলা ভাষায়।
- funny, sweet, বন্ধুসুলভ, light roasting allowed।
- ৪ জন close friend: ALPHA, SURJO, MONY, ALON।
- তাদের saved memory/inside joke ব্যবহার করবে।
- religion, race, health, body, family নিয়ে toxic insult করবে না।
- reply ছোট রাখবে।
- command বা হিসাবের fake data বানাবে না।
- জরুরি কথা মনে হলে বলবে: "এটা একটু গুরুত্বপূর্ণ মনে হচ্ছে, চাইলে admin কে জানাতে পারি।"

Current user: {member}

Known friend notes:
{personality_notes}

User message:
{user_text}

এখন natural বাংলা reply দাও।
"""
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.9, "maxOutputTokens": 180},
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            print("Gemini error:", r.status_code, r.text[:500])
            return None
        data = r.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as exc:
        print("Gemini request failed:", exc)
        return None

# =========================================================
# COMMANDS
# =========================================================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(build_help_message())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(build_help_message())

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 Your Telegram ID: {update.effective_user.id}")

async def debug_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = await get_cached_data(force=True)
        await update.message.reply_text(
            "✅ Google Sheet connected!\n\n"
            f"Service account:\n{get_service_account_email()}\n\n"
            f"Spreadsheet ID:\n{SPREADSHEET_ID}\n\n"
            f"Cache TTL: {CACHE_TTL_SECONDS}s\nAuto scan: {SCAN_INTERVAL_SECONDS}s\n"
            f"Repair Mode: {'ON' if repair_mode else 'OFF'}\nAI Enabled: {AI_ENABLED}\n\n"
            "Sheets:\n- " + "\n- ".join(data["sheet_titles"])
        )
    except Exception as exc:
        await update.message.reply_text(f"❌ Google Sheet connection failed.\n\nError:\n{type(exc).__name__}: {exc}")
        print(traceback.format_exc())

async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    try:
        await asyncio.to_thread(ensure_extra_sheets)
        await refresh_cache()
        await update.message.reply_text("✅ Sheet cache refreshed + extra sheets checked.")
    except Exception as exc:
        await update.message.reply_text(f"❌ Refresh failed:\n{type(exc).__name__}: {exc}")
        print(traceback.format_exc())

async def wallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = await get_cached_data()
        member = get_member_name_by_user_id(data, update.effective_user.id)
        if not member:
            await update.message.reply_text(f"❌ তোমার Telegram User ID member list-এ পাওয়া যায়নি।\n\nতোমার ID: {update.effective_user.id}")
            return
        await update.message.reply_text(build_wallet_message(member, data["stats"]))
    except Exception as exc:
        await update.message.reply_text(f"❌ /wallet error:\n{type(exc).__name__}: {exc}")
        print(traceback.format_exc())

async def summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = await get_cached_data()
        await update.message.reply_text(build_summary_message(data["stats"]))
    except Exception as exc:
        await update.message.reply_text(f"❌ /summary error:\n{type(exc).__name__}: {exc}")
        print(traceback.format_exc())

async def low_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = await get_cached_data()
        await update.message.reply_text(build_low_wallet_list(data["stats"]))
    except Exception as exc:
        await update.message.reply_text(f"❌ /low error:\n{type(exc).__name__}: {exc}")
        print(traceback.format_exc())

async def repair_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global repair_mode
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    repair_mode = True
    await update.message.reply_text("🔧 Repair Mode ON\n\nAuto member notifications are now paused.")

async def repair_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global repair_mode
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    repair_mode = False
    await update.message.reply_text("✅ Repair Mode OFF\n\nAuto member notifications are active again.")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = await get_cached_data()
    await update.message.reply_text(
        "🤖 BOT STATUS\n\n"
        f"Repair Mode: {'ON 🔧' if repair_mode else 'OFF ✅'}\n"
        f"AI Chat: {'ON 🤖' if AI_ENABLED and GEMINI_API_KEY else 'OFF'}\n"
        f"Month: {data['selected_month']}\nMembers: {len(data['member_map'])}\n"
        f"Cache TTL: {CACHE_TTL_SECONDS}s\nAuto Scan: {SCAN_INTERVAL_SECONDS}s\n"
        f"Admin IDs: {ADMIN_USER_IDS or 'Not set'}"
    )

async def bazarlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rows = await asyncio.to_thread(get_sheet_rows, NEED_LIST_SHEET)
        pending = []
        for row in rows[1:]:
            item = row_value(row, 4)
            status = row_value(row, 5).upper()
            member = row_value(row, 3)
            if item and status != "BOUGHT":
                pending.append((item, member))
        if not pending:
            await update.message.reply_text("✅ বাজার লিস্ট এখন ফাঁকা। সবাই এত responsible কবে হলো রে ভাই? 😄")
            return
        msg = "🛒 দরকারি বাজার লিস্ট:\n\n"
        for i, (item, member) in enumerate(pending, start=1):
            msg += f"{i}. {item} — added by {member}\n"
        msg += "\nযে বাজারে যাবে, এগুলো দেখে যেও। না হলে বাসায় বিচার বসবে 😄"
        await update.message.reply_text(msg)
    except Exception as exc:
        await update.message.reply_text(f"❌ /bazarlist error:\n{type(exc).__name__}: {exc}")

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    cancelled = False
    if uid in user_pending_bazar:
        user_pending_bazar.pop(uid, None)
        cancelled = True
    if uid in user_pending_memory:
        user_pending_memory.pop(uid, None)
        cancelled = True
    await update.message.reply_text("✅ Pending কাজ cancel করা হয়েছে।" if cancelled else "কোনো pending কাজ নেই ভাই 😄")

async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /approve BZ20260424123456")
        return
    pending_id = context.args[0].strip()
    rows = await asyncio.to_thread(get_sheet_rows, PENDING_BAZAR_SHEET)
    found_row = None
    found_index = None
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 0) == pending_id:
            found_row, found_index = row, idx
            break
    if not found_row:
        await update.message.reply_text("❌ Pending ID পাওয়া যায়নি।")
        return
    if row_value(found_row, 8).upper() == "APPROVED":
        await update.message.reply_text("এই entry already approved.")
        return
    member = row_value(found_row, 3)
    items = row_value(found_row, 5)
    total = row_value(found_row, 6)
    await asyncio.to_thread(append_row, BAZAR_SHEET, [today_str(), member, "বাজার", total, "", items, ""])
    await asyncio.to_thread(safe_update_cell, PENDING_BAZAR_SHEET, found_index, 9, "APPROVED")
    await asyncio.to_thread(safe_update_cell, PENDING_BAZAR_SHEET, found_index, 10, "Added to Bazar_Entry")
    await refresh_cache()
    await update.message.reply_text(f"✅ Approved!\n\n👤 Buyer: {member}\n🧾 Items: {items}\n💰 Total: {format_lkr(total)} LKR\n\nএখন auto scanner আগের নিয়মে সবাইকে message পাঠাবে।")

async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /reject BZ20260424123456")
        return
    pending_id = context.args[0].strip()
    rows = await asyncio.to_thread(get_sheet_rows, PENDING_BAZAR_SHEET)
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 0) == pending_id:
            await asyncio.to_thread(safe_update_cell, PENDING_BAZAR_SHEET, idx, 9, "REJECTED")
            await update.message.reply_text(f"❌ Rejected: {pending_id}")
            return
    await update.message.reply_text("❌ Pending ID পাওয়া যায়নি।")

# =========================================================
# SCANNER
# =========================================================
def is_sent_status(value: str) -> bool:
    return "SENT" in str(value or "").upper()

def complete_bazar_row(row: List[str]) -> bool:
    return bool(row_value(row, 0) and row_value(row, 1) and row_value(row, 2) and parse_amount(row_value(row, 3)) > 0)

def complete_payment_row(row: List[str]) -> bool:
    return bool(row_value(row, 0) and row_value(row, 1) and parse_amount(row_value(row, 2)) > 0 and row_value(row, 3))

async def send_admin(bot, text: str, data: Dict[str, Any]) -> None:
    if not SEND_ADMIN_DETAILS:
        return
    group_id = data.get("admin_group_id", "")
    if group_id:
        await bot.send_message(chat_id=group_id, text=text)

async def scan_bazar(bot, data: Dict[str, Any]) -> bool:
    rows, stats, member_map = data["bazar_rows"], data["stats"], data["member_map"]
    if len(rows) < 4:
        return False
    changed = False
    for idx, row in enumerate(rows[3:], start=4):
        status = row_value(row, 6)
        row_key = f"bazar:{idx}:{row_value(row, 0)}:{row_value(row, 1)}:{row_value(row, 3)}"
        if row_key in processed_bazar_rows or not complete_bazar_row(row) or is_sent_status(status):
            continue
        date, buyer, typ = row_value(row, 0), normalize_name(row_value(row, 1)), row_value(row, 2)
        total = parse_amount(row_value(row, 3))
        share = parse_amount(row_value(row, 4)) or stats["share_per_head"]
        note = row_value(row, 5)
        entry = {"date": date, "buyer": buyer, "type": typ, "total": total, "share": share}
        await send_admin(bot, f"📋 BAZAR DETAILS\n\n👤 Buyer: {buyer}\n📅 Date: {date}\n🧾 Category: {typ}\n💰 Total: {format_lkr(total)} LKR\n👥 Share: {format_lkr(share)} LKR\n📝 Note: {note}\n\n🔧 Repair Mode: {'ON' if repair_mode else 'OFF'}", data)
        success = 0
        if not repair_mode:
            for member_name, user_id in member_map.items():
                try:
                    async with send_lock:
                        await bot.send_message(chat_id=user_id, text=build_bazar_message(entry, stats, member_name))
                        await asyncio.sleep(0.25)
                    success += 1
                except Exception as exc:
                    print("Bazar send error:", member_name, exc)
        await asyncio.to_thread(safe_update_cell, BAZAR_SHEET, idx, 7, "SENT ADMIN ONLY" if repair_mode else f"SENT: {success}")
        processed_bazar_rows.add(row_key)
        changed = True
    return changed

async def scan_payment(bot, data: Dict[str, Any]) -> bool:
    rows, stats, member_map = data["payment_rows"], data["stats"], data["member_map"]
    if len(rows) < 4:
        return False
    changed = False
    for idx, row in enumerate(rows[3:], start=4):
        status = row_value(row, 5)
        row_key = f"payment:{idx}:{row_value(row, 0)}:{row_value(row, 1)}:{row_value(row, 2)}"
        if row_key in processed_payment_rows or not complete_payment_row(row) or is_sent_status(status):
            continue
        date, member = row_value(row, 0), normalize_name(row_value(row, 1))
        amount, typ, note = parse_amount(row_value(row, 2)), row_value(row, 3), row_value(row, 4)
        user_id = member_map.get(member)
        wallet_now = stats["members"].get(member, {}).get("wallet", 0)
        entry = {"date": date, "member": member, "amount": amount, "type": typ, "note": note}
        await send_admin(bot, f"📋 PAYMENT DETAILS\n\n👤 Member: {member}\n📅 Date: {date}\n💰 Amount: {format_lkr(amount)} LKR\n📦 Type: {typ}\n📝 Note: {note}\n💼 Wallet Now: {format_lkr(wallet_now)} LKR\n\n🔧 Repair Mode: {'ON' if repair_mode else 'OFF'}", data)
        if not user_id:
            await asyncio.to_thread(safe_update_cell, PAYMENT_SHEET, idx, 6, "Member user id missing")
            continue
        if repair_mode:
            await asyncio.to_thread(safe_update_cell, PAYMENT_SHEET, idx, 6, "SENT ADMIN ONLY")
            processed_payment_rows.add(row_key)
            changed = True
            continue
        try:
            async with send_lock:
                await bot.send_message(chat_id=user_id, text=build_payment_message(entry, wallet_now, stats["threshold"]))
                await asyncio.sleep(0.25)
            await asyncio.to_thread(safe_update_cell, PAYMENT_SHEET, idx, 6, f"SENT TO {member}")
            processed_payment_rows.add(row_key)
            changed = True
        except Exception as exc:
            print("Payment send error:", member, exc)
            await asyncio.to_thread(safe_update_cell, PAYMENT_SHEET, idx, 6, "SEND FAILED")
    return changed

async def scan_low_wallet(bot, data: Dict[str, Any]) -> None:
    stats, threshold, member_map = data["stats"], data["stats"]["threshold"], data["member_map"]
    group_id = data.get("admin_group_id", "")
    for member, m in stats["members"].items():
        wallet, status = m["wallet"], m["status"]
        key = f"{stats['month']}:{member}"
        if wallet < threshold:
            if key in low_alert_sent_cache:
                continue
            group_msg = f"⚠️ LOW WALLET AUTO ALERT\n\n👤 Member: {member}\n💸 Wallet: {format_lkr(wallet)} LKR\n💳 Suggested Top-up: {format_lkr(threshold - wallet)} LKR\n📌 Status: {status}\n📉 Threshold: {format_lkr(threshold)} LKR\n\n🔧 Repair Mode: {'ON' if repair_mode else 'OFF'}"
            if group_id:
                await bot.send_message(chat_id=group_id, text=group_msg)
            if SEND_LOW_PERSONAL and not repair_mode and member_map.get(member):
                await bot.send_message(chat_id=member_map[member], text=build_low_wallet_personal(member, wallet, status, threshold))
            low_alert_sent_cache.add(key)
        else:
            low_alert_sent_cache.discard(key)

async def auto_scan_loop(bot):
    await asyncio.sleep(10)
    while True:
        try:
            data = await refresh_cache()
            bazar_changed = await scan_bazar(bot, data)
            payment_changed = await scan_payment(bot, data)
            await scan_low_wallet(bot, data)
            if bazar_changed or payment_changed:
                await refresh_cache()
        except Exception as exc:
            print("Auto scanner error:", repr(exc))
            print(traceback.format_exc())
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# =========================================================
# NORMAL MESSAGE HANDLER
# =========================================================
async def normal_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    uid = str(update.effective_user.id)
    data = await get_cached_data()
    member = get_member_name_by_user_id(data, uid) or update.effective_user.first_name or "UNKNOWN"
    member = normalize_name(member)
    low = text.lower().strip()

    if low in ["ok", "okay", "ওকে", "ঠিক আছে", "হ্যাঁ", "ha", "yes"]:
        pending_memory = user_pending_memory.get(uid)
        if pending_memory:
            await asyncio.to_thread(upsert_user_personality, pending_memory)
            user_pending_memory.pop(uid, None)
            reply = (
                "✅ Memory save করে রাখলাম 😄\n\n"
                f"👤 {pending_memory['name']}\n"
                f"🏷 Nickname: {pending_memory.get('nickname') or '-'}\n"
                f"🤝 Relation: {pending_memory.get('relation') or '-'}\n"
                f"😂 Inside Joke: {pending_memory.get('inside_jokes') or '-'}\n\n"
                "এখন থেকে এই তথ্য ধরে মজা করবো 😏"
            )
            await update.message.reply_text(reply)
            await asyncio.to_thread(save_chat_log, uid, member, text, reply, "MEMORY_SAVE")
            return

        pending = user_pending_bazar.get(uid)
        if pending:
            pending_id = pending["id"]
            await asyncio.to_thread(append_row, PENDING_BAZAR_SHEET, [pending_id, now_str(), uid, member, pending["raw"], pending["items"], pending["total"], "USER_OK", "PENDING", "", pending.get("note", "")])
            user_pending_bazar.pop(uid, None)
            admin_msg = f"🆕 নতুন বাজার approval দরকার\n\nID: {pending_id}\n👤 Buyer: {member}\n🧾 Items: {pending['items']}\n💰 Total: {format_lkr(pending['total'])} LKR\n\nApprove করতে:\n/approve {pending_id}\n\nReject করতে:\n/reject {pending_id}"
            await send_admin(context.bot, admin_msg, data)
            reply = "✅ ঠিক আছে ভাই, বাজারটা admin approval-এ পাঠিয়ে দিলাম।\nAdmin approve দিলেই সবার wallet update হয়ে যাবে 😄"
            await update.message.reply_text(reply)
            await asyncio.to_thread(save_chat_log, uid, member, text, reply, "BAZAR_OK")
            return

    bazar = parse_bazar_text(text)
    if bazar:
        pending_id = generate_id("BZ")
        user_pending_bazar[uid] = {"id": pending_id, "raw": text, "items": bazar["items"], "total": bazar["total"], "note": bazar["note"]}
        reply = f"🛒 বাজারটা আমি এভাবে বুঝেছি:\n\n👤 Buyer: {member}\n🧾 Items: {bazar['items']}\n💰 Total: {format_lkr(bazar['total'])} LKR\n\nসব ঠিক থাকলে শুধু OK লিখো ✅\nভুল হলে /cancel দিয়ে আবার লিখো।\n\nবাহ ভাই, আজকে তো বাজার mission চালু হয়ে গেছে 😄"
        await update.message.reply_text(reply)
        await asyncio.to_thread(save_chat_log, uid, member, text, reply, "BAZAR_DRAFT")
        return

    need_items = parse_need_list_text(text)
    if need_items:
        for item in need_items:
            await asyncio.to_thread(append_row, NEED_LIST_SHEET, [generate_id("ND"), now_str(), uid, member, item, "PENDING", "", "", text, ""])
        reply = "✅ বাজার লিস্টে add করে রাখলাম:\n\n" + "\n".join([f"• {i}" for i in need_items]) + "\n\nযে বাজারে যাবে, তাকে এখন আর অজুহাত দিতে দিবো না 😄"
        await update.message.reply_text(reply)
        await send_admin(context.bot, f"📝 NEED LIST UPDATE\n\n👤 Added by: {member}\n" + "\n".join([f"• {i}" for i in need_items]), data)
        await asyncio.to_thread(save_chat_log, uid, member, text, reply, "NEED_LIST")
        return

    memory = detect_memory_text(text, member)
    if memory:
        user_pending_memory[uid] = memory
        reply = (
            "🧠 এটা আমি memory হিসেবে save করতে পারি:\n\n"
            f"👤 Name: {memory['name']}\n"
            f"🏷 Nickname: {memory.get('nickname') or '-'}\n"
            f"🤝 Relation: {memory.get('relation') or '-'}\n"
            f"🏷 Tags: {memory.get('tags') or '-'}\n"
            f"😂 Inside Joke: {memory.get('inside_jokes') or '-'}\n"
            f"📝 Note: {memory.get('notes') or '-'}\n\n"
            "সব ঠিক থাকলে OK লিখো ✅\nভুল হলে /cancel দাও।"
        )
        await update.message.reply_text(reply)
        await asyncio.to_thread(save_chat_log, uid, member, text, reply, "MEMORY_DRAFT")
        return

    personality_notes = await asyncio.to_thread(get_personality_notes)
    ai_text = await asyncio.to_thread(gemini_reply, text, member, personality_notes)
    reply = ai_text or safe_fun_reply(text, member)
    await update.message.reply_text(reply)
    await asyncio.to_thread(save_chat_log, uid, member, text, reply, "CHAT")

# =========================================================
# APP START
# =========================================================
async def post_init(application: Application):
    commands = [
        BotCommand("start", "Start bot and show help"),
        BotCommand("help", "Show all commands"),
        BotCommand("wallet", "My wallet details"),
        BotCommand("summary", "Full month summary"),
        BotCommand("low", "Low wallet list"),
        BotCommand("bazarlist", "Need list"),
        BotCommand("cancel", "Cancel pending work"),
        BotCommand("id", "My Telegram ID"),
        BotCommand("debug", "Check Google Sheet connection"),
        BotCommand("refresh", "Admin only refresh cache"),
        BotCommand("repair_on", "Admin only repair mode ON"),
        BotCommand("repair_off", "Admin only repair mode OFF"),
        BotCommand("status", "Bot status"),
        BotCommand("approve", "Admin approve pending bazar"),
        BotCommand("reject", "Admin reject pending bazar"),
    ]
    await application.bot.set_my_commands(commands)
    try:
        await asyncio.to_thread(ensure_extra_sheets)
    except Exception as exc:
        print("ensure_extra_sheets failed:", exc)
    asyncio.create_task(auto_scan_loop(application.bot))


def main():
    require_env()
    print("Market Hisab Bot is running...")
    print("Spreadsheet ID:", SPREADSHEET_ID)
    print("Service account:", get_service_account_email())
    print("Cache TTL:", CACHE_TTL_SECONDS)
    print("Scan interval:", SCAN_INTERVAL_SECONDS)
    print("Admin IDs:", ADMIN_USER_IDS)
    print("AI Enabled:", AI_ENABLED)

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("debug", debug_cmd))
    app.add_handler(CommandHandler("refresh", refresh_cmd))
    app.add_handler(CommandHandler(["wallet", "balance", "me"], wallet_cmd))
    app.add_handler(CommandHandler("summary", summary_cmd))
    app.add_handler(CommandHandler("low", low_cmd))
    app.add_handler(CommandHandler("bazarlist", bazarlist_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("repair_on", repair_on_cmd))
    app.add_handler(CommandHandler("repair_off", repair_off_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("reject", reject_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, normal_message_handler))
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
