# Market Hisab Bot - V3 Final Clean
# Existing features kept:
# - Wallet / summary / low wallet scanner
# - Payment scanner
# - Bazar submit -> user OK -> admin approve -> Bazar_Entry -> auto messages to all members
# - Need list submit -> user OK -> admin approve -> Need_List
# New/fixed:
# - High roast AI chat with no repeated fixed message
# - Savage fallback if Gemini fails, no "AI reply asheni" message
# - Auto memory learning into User_Personality, with rule fallback
# - Need list ID display + remove/done/clear commands

import os
import json
import re
import asyncio
import traceback
import requests
import random
from zoneinfo import ZoneInfo
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
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash-latest").strip()

# OpenAI chat brain (Luna-style). Keep Gemini as optional fallback.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TEXT_MODEL = os.getenv("TEXT_MODEL", "gpt-4o-mini").strip()
AI_PROVIDER = os.getenv("AI_PROVIDER", "openai").strip().lower()

ROAST_LEVEL = os.getenv("ROAST_LEVEL", "savage").strip().lower()
OFFICE_GROUP_ID = os.getenv("OFFICE_GROUP_ID", "").strip()
SRI_LANKA_TZ = os.getenv("SRI_LANKA_TZ", "Asia/Colombo").strip()
DAY_SHIFT_START = os.getenv("DAY_SHIFT_START", "05:00").strip()
DAY_SHIFT_END = os.getenv("DAY_SHIFT_END", "17:20").strip()

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
OFFICE_USERS_SHEET = "Office_Users"

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

# pending confirmations: uid -> data
user_pending: Dict[str, Dict[str, Any]] = {}
# for memory like "tar nickname hobe ..."
user_last_subject: Dict[str, str] = {}

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
    "mangsho": "মাংস", "beef": "গরুর মাংস", "gosht": "গোশত",
}

# =========================================================
# BASIC HELPERS
# =========================================================
def require_env() -> None:
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
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
    cleaned = "".join(ch for ch in str(value).strip() if ch.isdigit() or ch in ".-")
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
    s = str(value or "").strip()
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
        return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON).get("client_email", "")
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
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_spreadsheet():
    return get_gspread_client().open_by_key(SPREADSHEET_ID)


def safe_update_cell(sheet_name: str, row: int, col: int, value: str) -> None:
    get_spreadsheet().worksheet(sheet_name).update_cell(row, col, value)


def append_row(sheet_name: str, values: List[Any]) -> None:
    get_spreadsheet().worksheet(sheet_name).append_row(values, value_input_option="USER_ENTERED")


def get_sheet_rows(sheet_name: str) -> List[List[str]]:
    try:
        return get_spreadsheet().worksheet(sheet_name).get_all_values()
    except Exception:
        return []


def delete_sheet_row(sheet_name: str, row_index: int) -> None:
    get_spreadsheet().worksheet(sheet_name).delete_rows(row_index)


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

    # Settings rows 9-12 contain members based on your current sheet.
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
# STATS / MESSAGES
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
    stats = {
        "month": month_key,
        "threshold": threshold,
        "total_topup": 0.0,
        "total_expense": 0.0,
        "share_per_head": 0.0,
        "total_wallet_left": 0.0,
        "members": {},
    }

    for name in members:
        stats["members"][name] = {
            "topup": 0.0,
            "own_expense": 0.0,
            "share_deduction": 0.0,
            "wallet": 0.0,
            "status": "OK",
        }

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
    uid = str(user_id).strip()
    for name, saved_uid in data["member_map"].items():
        if str(saved_uid).strip() == uid:
            return name
    return None


def get_member_names(data: Dict[str, Any]) -> List[str]:
    names = list(data.get("member_map", {}).keys())
    return names if names else ["ALPHA", "SURJO", "MONY", "ALON"]


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
        "/reject ID - Pending bazar reject\n"
        "/needapprove ID - Need item approve\n"
        "/needreject ID - Need item reject\n"
        "/needremove ID - Need item remove\n"
        "/needdone ID - Need item bought/done\n"
        "/needclear - Clear all pending need items\n\n"
        "💬 Examples:\n"
        "চাল শেষ ডাল শেষ তেল শেষ\n"
        "আলু 128 পানি 24 চাল 450 ডাল 127 মাছ 1200\n"
        "Alon amar mama, se chitar\n\n"
        f"🔧 Repair Mode: {'ON 🔧' if repair_mode else 'OFF ✅'}\n"
        f"🤖 AI Chat: {'ON 🤖' if AI_ENABLED and (OPENAI_API_KEY or GEMINI_API_KEY) else 'OFF'}\n"
        f"🔥 Roast Level: {ROAST_LEVEL.upper()}"
    )


def build_wallet_message(member_name: str, stats: Dict[str, Any]) -> str:
    m = stats["members"].get(member_name)
    if not m:
        return "❌ Wallet data পাওয়া যায়নি।"
    return (
        "💼 MY WALLET STATUS\n\n"
        f"👤 Member: {member_name}\n"
        f"📅 Month: {stats['month']}\n"
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
        msg += (
            f"👤 {name}\n"
            f"➕ Top-up: {format_lkr(m['topup'])} LKR\n"
            f"🛒 Own Bazar: {format_lkr(m['own_expense'])} LKR\n"
            f"💰 Wallet: {format_lkr(m['wallet'])} LKR\n"
            f"📌 Status: {m['status']}\n\n"
        )
    return msg


def build_low_wallet_list(stats: Dict[str, Any]) -> str:
    threshold = stats["threshold"]
    msg = f"⚠️ LOW WALLET MEMBERS\n\n📅 Month: {stats['month']}\n📉 Threshold: {format_lkr(threshold)} LKR\n\n"
    found = False
    for name, m in stats["members"].items():
        if m["wallet"] < threshold:
            found = True
            msg += (
                f"👤 {name}\n"
                f"💸 Wallet: {format_lkr(m['wallet'])} LKR\n"
                f"💳 Suggested Top-up: {format_lkr(threshold - m['wallet'])} LKR\n"
                f"📌 Status: {m['status']}\n\n"
            )
    if not found:
        msg += "✅ এখন কোনো low wallet member নেই।"
    return msg


def build_bazar_message(entry: Dict[str, Any], stats: Dict[str, Any], member_name: str) -> str:
    m = stats["members"].get(member_name, {"wallet": 0, "status": "OK"})
    return (
        "🛒 BAZAR UPDATE\n\n"
        f"👤 Buyer: {entry['buyer']}\n"
        f"📅 Date: {entry['date']}\n"
        f"🧾 Type: {entry['type']}\n"
        f"💰 Total Expense: {format_lkr(entry['total'])} LKR\n"
        f"👥 Per Person Share: {format_lkr(entry['share'])} LKR\n\n"
        f"👷 Your Name: {member_name}\n"
        f"💼 Your Wallet Now: {format_lkr(m['wallet'])} LKR\n"
        f"📌 Status: {m['status']}\n\n"
        f"📊 Month Total Expense: {format_lkr(stats['total_expense'])} LKR\n"
        f"💼 Total Wallet Left: {format_lkr(stats['total_wallet_left'])} LKR"
    )


def build_payment_message(entry: Dict[str, Any], wallet_now: float, threshold: float) -> str:
    return (
        "💳 WALLET TOP-UP / ADJUSTMENT\n\n"
        f"👤 Member: {entry['member']}\n"
        f"📅 Date: {entry['date']}\n"
        f"➕ Amount: {format_lkr(entry['amount'])} LKR\n"
        f"📦 Type: {entry['type']}\n"
        f"📝 Note: {entry['note']}\n\n"
        f"💼 Your Wallet Now: {format_lkr(wallet_now)} LKR\n"
        f"📌 Status: {get_wallet_status(wallet_now, threshold)}"
    )


def build_low_wallet_personal(member: str, wallet: float, status: str, threshold: float) -> str:
    return (
        "⚠️ LOW WALLET ALERT\n\n"
        f"👤 Member: {member}\n"
        f"💸 Current Wallet: {format_lkr(wallet)} LKR\n"
        f"💳 Suggested Top-up: {format_lkr(threshold - wallet)} LKR\n"
        f"📌 Status: {status}"
    )

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
        if p and not re.fullmatch(r"\d+(?:\.\d+)?", p):
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
    ignore = {
        "ajke", "aaj", "আজকে", "আজ", "amader", "আমাদের", "ar", "আর",
        "o", "ও", "ta", "টা", "ki", "কি", "hobe", "হবে", "kinte", "কিনতে",
        "to", "তো", "e", "এই", "oi", "ওই", "amar", "amr", "আমার", "tomar", "তোমার",
    }
    items = []
    for p in parts:
        p = p.strip()
        if not p or p in ignore:
            continue
        item = bangla_item(p)
        if item and item not in items:
            items.append(item)
    return items


def generate_id(prefix: str) -> str:
    return f"{prefix}{datetime.now().strftime('%Y%m%d%H%M%S')}"

# =========================================================
# PERSONALITY / AI / ROAST
# =========================================================
def get_personality_rows() -> List[List[str]]:
    return get_sheet_rows(USER_PERSONALITY_SHEET)


def get_personality_notes() -> str:
    """Return compact memory context for AI. No label-style dump in prompt."""
    rows = get_personality_rows()
    notes = []
    for row in rows[1:]:
        name = row_value(row, 1)
        if not name:
            continue
        pieces = []
        for idx in [2, 3, 4, 5, 7]:  # nickname, relation, tags, jokes, notes
            val = row_value(row, idx)
            if val:
                # remove old report words if they accidentally got saved earlier
                val = re.sub(r"\b(nickname|relation|inside jokes?|notes?|tags|bot_style)\s*[:=]", "", val, flags=re.I)
                val = re.sub(r"\s+", " ", val).strip()
                if val:
                    pieces.append(val[:90])
        if pieces:
            notes.append(f"{normalize_name(name)} => " + " | ".join(pieces[:5]))
    return "\n".join(notes[:40])


def get_member_memory(name: str) -> str:
    """Return plain memory only, never label words like nickname/relation/notes."""
    target = normalize_name(name)
    rows = get_personality_rows()
    for row in rows[1:]:
        if normalize_name(row_value(row, 1)) == target:
            parts = []
            for idx in [2, 3, 5, 7]:
                val = row_value(row, idx)
                if val:
                    val = re.sub(r"\b(nickname|relation|inside jokes?|notes?|tags|bot_style)\s*[:=]", "", val, flags=re.I)
                    val = re.sub(r"\s+", " ", val).strip()
                    if val:
                        parts.append(val[:80])
            return " | ".join(parts[:3])
    return ""


def save_chat_log(user_id: str, member: str, message: str, reply: str, typ: str, status: str = "OK") -> None:
    try:
        append_row(BOT_CHAT_LOG_SHEET, [now_str(), user_id, member, message, reply, typ, status])
    except Exception as exc:
        print("Chat log save failed:", exc)


def upsert_user_personality(memory: Dict[str, str], data: Dict[str, Any]) -> None:
    if not memory or not memory.get("name"):
        return
    sh = get_spreadsheet().worksheet(USER_PERSONALITY_SHEET)
    rows = sh.get_all_values()
    member_map = data.get("member_map", {})
    name = normalize_name(memory.get("name"))
    user_id = member_map.get(name, "")
    now_date = today_str()

    def clean_mem(v: Any, limit: int = 120) -> str:
        val = str(v or "").strip()
        val = re.sub(r"\b(nickname|relation|inside jokes?|notes?|tags|bot_style)\s*[:=]", "", val, flags=re.I)
        val = re.sub(r"\s+", " ", val).strip()
        return val[:limit]

    def merge_old(old: str, new: str) -> str:
        old = clean_mem(old, 180)
        new = clean_mem(new, 120)
        if not new:
            return old
        if not old:
            return new
        if new.lower() in old.lower():
            return old
        return (old + ", " + new)[:240]

    target_row = None
    for idx, row in enumerate(rows[1:], start=2):
        if normalize_name(row_value(row, 1)) == name:
            target_row = idx
            break

    if target_row:
        old = rows[target_row - 1]
        sh.update_cell(target_row, 1, row_value(old, 0) or user_id)
        sh.update_cell(target_row, 3, merge_old(row_value(old, 2), memory.get("nickname", "")))
        sh.update_cell(target_row, 4, merge_old(row_value(old, 3), memory.get("relation", "")))
        sh.update_cell(target_row, 5, merge_old(row_value(old, 4), memory.get("tags", "")))
        sh.update_cell(target_row, 6, merge_old(row_value(old, 5), memory.get("inside_jokes", "")))
        sh.update_cell(target_row, 7, merge_old(row_value(old, 6), memory.get("bot_style", "")))
        sh.update_cell(target_row, 8, merge_old(row_value(old, 7), memory.get("notes", "")))
        sh.update_cell(target_row, 9, now_date)
    else:
        sh.append_row([
            user_id,
            name,
            clean_mem(memory.get("nickname", "")),
            clean_mem(memory.get("relation", "")),
            clean_mem(memory.get("tags", "")),
            clean_mem(memory.get("inside_jokes", "")),
            clean_mem(memory.get("bot_style", "savage roast")),
            clean_mem(memory.get("notes", "")),
            now_date,
        ], value_input_option="USER_ENTERED")


def ai_model_call(prompt: str, max_tokens: int = 220, temperature: float = 0.9) -> Optional[str]:
    """OpenAI first. Gemini kept only as emergency fallback so old setup does not break."""
    if not AI_ENABLED:
        return None

    if OPENAI_API_KEY and AI_PROVIDER in ["openai", "", "auto"]:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": TEXT_MODEL or "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "You are a natural Telegram friend-group bot. Follow the user's language and tone. Never output debug reports."},
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=20)
            if r.status_code != 200:
                print("OpenAI error:", r.status_code, r.text[:500])
            else:
                data = r.json()
                return data["choices"][0]["message"]["content"].strip()
        except Exception as exc:
            print("OpenAI request failed:", exc)

    if GEMINI_API_KEY:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
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

    return None


# Backward-compatible name; old parts calling gemini_call will still work.
def gemini_call(prompt: str, max_tokens: int = 220, temperature: float = 0.9) -> Optional[str]:
    return ai_model_call(prompt, max_tokens=max_tokens, temperature=temperature)


def extract_json_object(text: str) -> Optional[dict]:
    if not text:
        return None
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None


def detect_member_in_text(text: str, data: Dict[str, Any]) -> Optional[str]:
    low = text.lower()
    for name in get_member_names(data):
        if name.lower() in low:
            return normalize_name(name)
    aliases = {
        "আলফা": "ALPHA", "alpha": "ALPHA",
        "সুরজ": "SURJO", "সূর্য": "SURJO", "surjo": "SURJO",
        "মনি": "MONY", "mony": "MONY", "money": "MONY",
        "আলন": "ALON", "alon": "ALON",
    }
    for k, v in aliases.items():
        if k in low and v in get_member_names(data):
            return v
    return None


def rule_extract_memory(user_text: str, speaker: str, data: Dict[str, Any], uid: str) -> Optional[Dict[str, str]]:
    text = user_text.strip()
    low = text.lower()
    target = detect_member_in_text(text, data)

    if not target and any(x in low for x in ["tar nickname", "tar nick", "তার nickname", "তার ডাকনাম", "nickname hobe", "nickname holo"]):
        target = user_last_subject.get(uid)

    if not target:
        return None

    user_last_subject[uid] = target
    nickname = ""
    relation = ""
    tags = []
    inside = []
    notes = []

    nick_match = re.search(r"(?:nickname|nick|ডাকনাম)\s*(?:hobe|holo|হবে|হলো|=|:)\s*([A-Za-z\u0980-\u09FF\s]+)", text, re.I)
    if nick_match:
        nickname = nick_match.group(1).strip(" .,!।")[:50]

    if any(x in low for x in ["mama", "মামা"]):
        relation = f"{speaker}-এর মামা"
        tags.append("mama")

    roast_words = {
        "chitar": "চিটার", "cheater": "চিটার", "চিটার": "চিটার",
        "hutase": "হুটাসে চলে", "hutashe": "হুটাসে চলে", "হুটাসে": "হুটাসে চলে",
        "lazy": "lazy", "লেজি": "লেজি", "boka": "বোকা", "বোকা": "বোকা",
        "drama": "drama", "নাটক": "নাটক", "mal": "মাল", "joss": "জোস",
    }
    for k, v in roast_words.items():
        if k in low:
            inside.append(v)
            tags.append("roast")

    memory_keywords = ["amar", "amr", "আমার", "se", "সে", "onek", "boro", "বড়", "valo", "ভালো", "kharap", "খারাপ", "smart", "friend", "bondhu", "বন্ধু", "posondo", "পছন্দ"]
    if not (nickname or relation or inside) and any(k in low for k in memory_keywords):
        notes.append(text[:100])

    if not (nickname or relation or inside or notes):
        return None

    if inside and not nickname and ("mama" in low or "মামা" in low) and "চিটার" in inside:
        nickname = "চিটার মামা"

    return {
        "name": target,
        "nickname": nickname,
        "relation": relation,
        "tags": ",".join(dict.fromkeys(tags)),
        "inside_jokes": ", ".join(dict.fromkeys(inside)),
        "bot_style": "savage roast",
        "notes": "; ".join(dict.fromkeys(notes))[:100],
    }


def ai_extract_memory(user_text: str, speaker: str, data: Dict[str, Any]) -> Optional[Dict[str, str]]:
    members = ", ".join(get_member_names(data))
    prompt = f"""
Extract memory from Bangla/Banglish Telegram chat.
Known members: {members}
Speaker: {speaker}
Message: {user_text}

Return ONLY valid JSON.
If useful memory about a known member, return:
{{"save":true,"name":"MEMBER_NAME","nickname":"","relation":"","tags":"","inside_jokes":"","bot_style":"savage roast","notes":"very short note"}}
If not useful:
{{"save":false}}

Rules:
- Do not invent.
- name must be one known member.
- notes max 8 words.
- inside_jokes max 5 words.
- Do NOT copy full message.
- No markdown.
"""
    out = ai_model_call(prompt, max_tokens=120, temperature=0.1)
    obj = extract_json_object(out or "")
    if not obj or not obj.get("save"):
        return None
    name = normalize_name(obj.get("name", ""))
    if name not in get_member_names(data):
        return None

    def short(v: Any, limit: int = 60) -> str:
        val = str(v or "").strip()
        val = re.sub(r"\b(nickname|relation|inside jokes?|notes?|tags|bot_style)\s*[:=]", "", val, flags=re.I)
        val = re.sub(r"\s+", " ", val).strip()
        return val[:limit]

    return {
        "name": name,
        "nickname": short(obj.get("nickname", ""), 40),
        "relation": short(obj.get("relation", ""), 40),
        "tags": short(obj.get("tags", ""), 50),
        "inside_jokes": short(obj.get("inside_jokes", ""), 50),
        "bot_style": "savage roast",
        "notes": short(obj.get("notes", ""), 60),
    }


def savage_fallback_reply(user_text: str, member: str, data: Dict[str, Any]) -> str:
    target = detect_member_in_text(user_text, data) or member
    memory = get_member_memory(target)
    if target != member and memory:
        return f"{target} এর কথা বলছো? 😏 {memory.split('|')[0].strip()} — এই লোকটা আলাদা লেভেলের কেস ভাই 😂"[:260]
    if target != member:
        return f"{target} এর কথা বলছো? 😏 ওর নাম শুনলেই মনে হয় গ্রুপে শান্তি থাকবে না 😂"[:260]
    return f"হাহাহা {member}, কথাটা শুনে মনে হচ্ছে আজকে গ্রুপে আবার আগুন লাগবে 😏😂"[:260]


def ai_chat_reply(user_text: str, member: str, data: Dict[str, Any]) -> Optional[str]:
    notes = get_personality_notes()
    members = ", ".join(get_member_names(data))
    prompt = f"""
তুমি close friend Telegram group-এর natural savage roaster bot.

Personality:
- Luna bot-এর মতো natural, warm, human-like reply দিবে।
- কিন্তু এই bot বেশি savage/funny roast mood-এ থাকবে।
- User যেভাবে কথা বলবে, সেই vibe ধরে reply দিবে।
- reply বাংলা/Banglish mixed হতে পারে, natural হতে হবে।
- কোনো report, debug, analysis, list, JSON দিবে না।
- nickname/relation/tags/inside_jokes/notes এই শব্দগুলো reply-তে লেখা যাবে না।
- Stored memory থাকলে data হিসেবে dump না করে joke হিসেবে naturally use করবে।
- ১-৩ লাইনের বেশি না।
- religion/race/health/body/family নিয়ে hard insult করবে না।
- explicit গালি avoid করবে, কিন্তু sharp খোঁচা দিবে।
- কোনো fixed template না; প্রতিবার fresh reply।

Known friends: {members}
Current user: {member}
Context memory, use only as background:
{notes}

User message:
{user_text}

Now reply naturally like a savage close friend:
"""
    return ai_model_call(prompt, max_tokens=180, temperature=1.15)


def clean_ai_reply(ai: str) -> Optional[str]:
    if not ai:
        return None
    text = ai.replace("```", "").strip()
    text = re.sub(r"\{.*?\}", "", text, flags=re.S)
    text = re.sub(r"\s+", " ", text).strip()

    bad_words = [
        "nickname", "relation", "inside joke", "inside_joke", "inside_jokes",
        "notes", "tags", "bot_style", "stored memory", "json", "debug", "analysis",
        "ডাকনাম", "রিলেশন", "নোট", "ট্যাগ"
    ]
    if any(w.lower() in text.lower() for w in bad_words):
        return None
    return text[:350] if text else None


def final_chat_reply(user_text: str, member: str, data: Dict[str, Any]) -> str:
    ai = ai_chat_reply(user_text, member, data)
    cleaned = clean_ai_reply(ai or "")
    if cleaned:
        return cleaned

    # Second chance with zero memory if model tried to dump context.
    prompt = f"""
Reply to this Bangla/Banglish Telegram message as a close friend.
Tone: natural, funny, savage roast, warm.
No report. No labels. No JSON. Max 2 lines.
Message: {user_text}
"""
    ai2 = ai_model_call(prompt, max_tokens=100, temperature=1.2)
    cleaned2 = clean_ai_reply(ai2 or "")
    if cleaned2:
        return cleaned2

    return savage_fallback_reply(user_text, member, data)

# =========================================================
# OFFICE GROUP SAVAGE MODE
# =========================================================
def parse_hhmm(value: str) -> tuple:
    try:
        h, m = value.strip().split(":")
        return int(h), int(m)
    except Exception:
        return 5, 0


def is_day_shift_now() -> bool:
    now = datetime.now(ZoneInfo(SRI_LANKA_TZ))
    start_h, start_m = parse_hhmm(DAY_SHIFT_START)
    end_h, end_m = parse_hhmm(DAY_SHIFT_END)

    start_minutes = start_h * 60 + start_m
    end_minutes = end_h * 60 + end_m
    current_minutes = now.hour * 60 + now.minute

    if start_minutes <= end_minutes:
        return start_minutes <= current_minutes <= end_minutes

    return current_minutes >= start_minutes or current_minutes <= end_minutes


def is_office_group(update: Update) -> bool:
    if not OFFICE_GROUP_ID or not update.effective_chat:
        return False
    return str(update.effective_chat.id).strip() == str(OFFICE_GROUP_ID).strip()


def get_office_user_profile(user_id: str) -> Dict[str, str]:
    rows = get_sheet_rows(OFFICE_USERS_SHEET)

    for row in rows[1:]:
        saved_id = row_value(row, 0)
        if str(saved_id).strip() == str(user_id).strip():
            day_name = row_value(row, 1)
            night_name = row_value(row, 2)
            role = row_value(row, 3).upper()
            roast_level = row_value(row, 4) or "savage"
            notes = row_value(row, 5)

            active_name = day_name if is_day_shift_now() else night_name
            if not active_name:
                active_name = day_name or night_name or "UNKNOWN"

            return {
                "found": "YES",
                "user_id": user_id,
                "name": active_name,
                "day_name": day_name,
                "night_name": night_name,
                "role": role,
                "roast_level": roast_level,
                "notes": notes,
            }

    return {
        "found": "NO",
        "user_id": user_id,
        "name": "UNKNOWN",
        "day_name": "",
        "night_name": "",
        "role": "MEMBER",
        "roast_level": "savage",
        "notes": "",
    }


def office_openai_reply(user_text: str, profile: Dict[str, str]) -> Optional[str]:
    name = profile.get("name", "UNKNOWN")
    role = profile.get("role", "MEMBER")
    notes = profile.get("notes", "")
    shift = "DAY" if is_day_shift_now() else "NIGHT"

    if role in ["ADMIN", "OWNER"] or normalize_name(name) == "ALPHA":
        prompt = f"""
তুমি office Telegram group-এর savage funny bot.

User: {name}
Role: {role}
Shift: {shift}
Notes: {notes}
Message: {user_text}

Rules:
- AlphA/Admin কে insult করবে না।
- Admin হলে respect + light funny reply দিবে।
- Reply বাংলা/Banglish natural হবে।
- কোনো report/list/debug/JSON না।
- ১-২ লাইনের মধ্যে।
- reply যেন মানুষের মতো লাগে।
"""
    else:
        prompt = f"""
তুমি office Telegram group-এর extreme savage roaster bot.

User name to roast: {name}
Shift: {shift}
User notes: {notes}
Message: {user_text}

Rules:
- Reply অবশ্যই {name} নাম ধরে হবে।
- Savage level HIGH: sharp, funny, অপমানজনক friend-group roast vibe.
- কিন্তু religion/race/health/body/family/sexual বিষয় নিয়ে insult করবে না।
- threat/violence/real harassment করবে না।
- fixed template না, প্রতিবার fresh reply।
- কোনো report/list/debug/JSON না।
- ১-৩ লাইনের মধ্যে।
- বাংলা/Banglish mixed natural style.
- reply এমন হবে যেন office group-এর বন্ধুরা হাসে।

Now give only the reply:
"""

    return ai_model_call(prompt, max_tokens=140, temperature=1.25)


def office_savage_fallback(user_text: str, profile: Dict[str, str]) -> str:
    name = profile.get("name", "UNKNOWN")
    role = profile.get("role", "MEMBER")

    if role in ["ADMIN", "OWNER"] or normalize_name(name) == "ALPHA":
        return f"{name} ভাই বলছে মানে group একটু straight হও, নাহলে bot-ও attendance কেটে দিবে 😄"

    words = [w for w in re.split(r"\s+", user_text.strip()) if len(w) > 2]
    keyword = random.choice(words) if words else "এই কথা"

    lines = [
        f"{name}, {keyword} নিয়ে এত confidence দেখাচ্ছো কেন? তোমার নিজের system-ই তো pending update 😂",
        f"{name} আবার কথা বলছে? group-এর শান্তি আজ officially শেষ 😭😂",
        f"{name}, তোমার logic দেখে calculator-ও resign দিতে চাইবে 😏",
        f"{name} ভাই, এই কথা বলার আগে mirror check করা উচিত ছিল 😂",
        f"{name}, তোমার কথা শুনে মনে হচ্ছে brain আজ half-day leave নিয়েছে 😭",
        f"{name} আসছে মানে drama free না, full package সহ এসেছে 😏😂",
    ]
    return random.choice(lines)


def final_office_group_reply(user_text: str, profile: Dict[str, str]) -> str:
    ai = office_openai_reply(user_text, profile)

    if ai:
        text = ai.replace("```", "").strip()
        text = re.sub(r"\{.*?\}", "", text, flags=re.S)
        text = re.sub(r"\s+", " ", text).strip()

        bad = ["json", "debug", "analysis", "report", "রিপোর্ট", "বিশ্লেষণ"]
        if text and not any(x in text.lower() for x in bad):
            return text[:400]

    return office_savage_fallback(user_text, profile)


async def handle_office_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, uid: str) -> bool:
    if not is_office_group(update):
        return False

    profile = await asyncio.to_thread(get_office_user_profile, uid)

    try:
        reply = await asyncio.to_thread(final_office_group_reply, text, profile)
    except Exception as exc:
        print("Office savage reply error:", repr(exc))
        print(traceback.format_exc())
        reply = office_savage_fallback(text, profile)

    await update.message.reply_text(reply)
    await asyncio.to_thread(
        save_chat_log,
        uid,
        profile.get("name", "UNKNOWN"),
        text,
        reply,
        "OFFICE_SAVAGE",
    )
    return True

# =========================================================
# COMMANDS
# =========================================================
async def send_admin(bot, text: str, data: Dict[str, Any]) -> None:
    if not SEND_ADMIN_DETAILS:
        return
    group_id = data.get("admin_group_id", "")
    if group_id:
        await bot.send_message(chat_id=group_id, text=text)


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
            f"Cache TTL: {CACHE_TTL_SECONDS}s\n"
            f"Auto scan: {SCAN_INTERVAL_SECONDS}s\n"
            f"Repair Mode: {'ON' if repair_mode else 'OFF'}\n"
            f"AI Enabled: {AI_ENABLED}\n"
            f"AI Provider: {AI_PROVIDER}\n"
            f"Text Model: {TEXT_MODEL if OPENAI_API_KEY else GEMINI_MODEL}\n\n"
            "Sheets:\n- " + "\n- ".join(data["sheet_titles"])
        )
    except Exception as exc:
        await update.message.reply_text(f"❌ Google Sheet connection failed.\n\n{type(exc).__name__}: {exc}")
        print(traceback.format_exc())


async def refresh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    try:
        await refresh_cache()
        await update.message.reply_text("✅ Sheet cache refreshed.")
    except Exception as exc:
        await update.message.reply_text(f"❌ Refresh failed:\n{type(exc).__name__}: {exc}")


async def wallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = await get_cached_data()
    member = get_member_name_by_user_id(data, update.effective_user.id)
    if not member:
        await update.message.reply_text(f"❌ তোমার Telegram User ID member list-এ নেই।\nতোমার ID: {update.effective_user.id}")
        return
    await update.message.reply_text(build_wallet_message(member, data["stats"]))


async def summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = await get_cached_data()
    await update.message.reply_text(build_summary_message(data["stats"]))


async def low_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = await get_cached_data()
    await update.message.reply_text(build_low_wallet_list(data["stats"]))


async def repair_on_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global repair_mode
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    repair_mode = True
    await update.message.reply_text("🔧 Repair Mode ON")


async def repair_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global repair_mode
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    repair_mode = False
    await update.message.reply_text("✅ Repair Mode OFF")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = await get_cached_data()
    await update.message.reply_text(
        "🤖 BOT STATUS\n\n"
        f"Repair Mode: {'ON 🔧' if repair_mode else 'OFF ✅'}\n"
        f"AI Chat: {'ON 🤖' if AI_ENABLED and (OPENAI_API_KEY or GEMINI_API_KEY) else 'OFF'}\n"
        f"Roast Level: {ROAST_LEVEL.upper()} 🔥\n"
        f"Month: {data['selected_month']}\n"
        f"Members: {len(data['member_map'])}\n"
        f"Cache TTL: {CACHE_TTL_SECONDS}s\n"
        f"Auto Scan: {SCAN_INTERVAL_SECONDS}s\n"
        f"Admin IDs: {ADMIN_USER_IDS or 'Not set'}"
    )


async def bazarlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await asyncio.to_thread(get_sheet_rows, NEED_LIST_SHEET)
    pending = []
    for row in rows[1:]:
        need_id = row_value(row, 0)
        item = row_value(row, 4)
        status = row_value(row, 5).upper()
        member = row_value(row, 3)
        if item and status in ["PENDING", "WAITING_ADMIN"]:
            pending.append((need_id, item, member, status))
    if not pending:
        await update.message.reply_text("✅ বাজার লিস্ট এখন ফাঁকা।")
        return
    msg = "🛒 দরকারি বাজার লিস্ট:\n\n"
    for i, (need_id, item, member, status) in enumerate(pending, start=1):
        msg += f"{i}. {need_id} — {item} — added by {member} — {status}\n"
    msg += "\nRemove: /needremove ID\nDone: /needdone ID"
    await update.message.reply_text(msg)


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if uid in user_pending:
        user_pending.pop(uid, None)
        await update.message.reply_text("✅ Pending কাজ cancel করা হয়েছে।")
    else:
        await update.message.reply_text("কোনো pending কাজ নেই।")


async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /approve BZ20260424123456")
        return
    pending_id = context.args[0].strip()
    rows = await asyncio.to_thread(get_sheet_rows, PENDING_BAZAR_SHEET)
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 0) == pending_id:
            if row_value(row, 8).upper() == "APPROVED":
                await update.message.reply_text("Already approved.")
                return

            # Add to Bazar_Entry exactly like manual entry: scanner will calculate share/message by existing logic.
            await asyncio.to_thread(
                append_row,
                BAZAR_SHEET,
                [today_str(), row_value(row, 3), "বাজার", row_value(row, 6), "", row_value(row, 5), ""]
            )
            await asyncio.to_thread(safe_update_cell, PENDING_BAZAR_SHEET, idx, 9, "APPROVED")
            await asyncio.to_thread(safe_update_cell, PENDING_BAZAR_SHEET, idx, 10, "Added to Bazar_Entry")

            # Immediately run scanner once so everyone gets the normal old-style message without waiting interval.
            fresh = await refresh_cache()
            await scan_bazar(context.bot, fresh)
            await refresh_cache()

            await update.message.reply_text(f"✅ Bazar approved and messages sent by old rule: {pending_id}")
            return
    await update.message.reply_text("❌ Pending ID পাওয়া যায়নি।")


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
            await update.message.reply_text(f"❌ Bazar rejected: {pending_id}")
            return
    await update.message.reply_text("❌ Pending ID পাওয়া যায়নি।")


async def needapprove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /needapprove ND20260424123456")
        return
    need_id = context.args[0].strip()
    rows = await asyncio.to_thread(get_sheet_rows, NEED_LIST_SHEET)
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 0) == need_id:
            await asyncio.to_thread(safe_update_cell, NEED_LIST_SHEET, idx, 6, "PENDING")
            await update.message.reply_text(f"✅ Need item approved: {row_value(row, 4)}")
            return
    await update.message.reply_text("❌ Need ID পাওয়া যায়নি।")


async def needreject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /needreject ND20260424123456")
        return
    need_id = context.args[0].strip()
    rows = await asyncio.to_thread(get_sheet_rows, NEED_LIST_SHEET)
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 0) == need_id:
            await asyncio.to_thread(safe_update_cell, NEED_LIST_SHEET, idx, 6, "REJECTED")
            await update.message.reply_text(f"❌ Need item rejected: {row_value(row, 4)}")
            return
    await update.message.reply_text("❌ Need ID পাওয়া যায়নি।")


async def needremove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /needremove ND20260424123456")
        return
    need_id = context.args[0].strip()
    rows = await asyncio.to_thread(get_sheet_rows, NEED_LIST_SHEET)
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 0) == need_id:
            item = row_value(row, 4)
            await asyncio.to_thread(delete_sheet_row, NEED_LIST_SHEET, idx)
            await update.message.reply_text(f"🗑 Removed from bazar list: {item} ({need_id})")
            return
    await update.message.reply_text("❌ Need ID পাওয়া যায়নি।")


async def needdone_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /needdone ND20260424123456")
        return
    need_id = context.args[0].strip()
    data = await get_cached_data()
    buyer = get_member_name_by_user_id(data, update.effective_user.id) or "ADMIN"
    rows = await asyncio.to_thread(get_sheet_rows, NEED_LIST_SHEET)
    for idx, row in enumerate(rows[1:], start=2):
        if row_value(row, 0) == need_id:
            await asyncio.to_thread(safe_update_cell, NEED_LIST_SHEET, idx, 6, "BOUGHT")
            await asyncio.to_thread(safe_update_cell, NEED_LIST_SHEET, idx, 7, buyer)
            await asyncio.to_thread(safe_update_cell, NEED_LIST_SHEET, idx, 8, today_str())
            await update.message.reply_text(f"✅ Marked bought: {row_value(row, 4)} ({need_id})")
            return
    await update.message.reply_text("❌ Need ID পাওয়া যায়নি।")


async def needclear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    rows = await asyncio.to_thread(get_sheet_rows, NEED_LIST_SHEET)
    removed = 0
    # Delete from bottom to top, keep header.
    for idx in range(len(rows), 1, -1):
        status = row_value(rows[idx - 1], 5).upper()
        if status in ["PENDING", "WAITING_ADMIN"]:
            await asyncio.to_thread(delete_sheet_row, NEED_LIST_SHEET, idx)
            removed += 1
    await update.message.reply_text(f"🧹 Cleared pending need items: {removed}")

# =========================================================
# SCANNERS
# =========================================================
def is_sent_status(value: str) -> bool:
    return "SENT" in str(value or "").upper()


def complete_bazar_row(row: List[str]) -> bool:
    return bool(row_value(row, 0) and row_value(row, 1) and row_value(row, 2) and parse_amount(row_value(row, 3)) > 0)


def complete_payment_row(row: List[str]) -> bool:
    return bool(row_value(row, 0) and row_value(row, 1) and parse_amount(row_value(row, 2)) > 0 and row_value(row, 3))


async def scan_bazar(bot, data: Dict[str, Any]) -> bool:
    rows = data["bazar_rows"]
    stats = data["stats"]
    member_map = data["member_map"]
    if len(rows) < 4:
        return False
    changed = False
    for idx, row in enumerate(rows[3:], start=4):
        status = row_value(row, 6)
        row_key = f"bazar:{idx}:{row_value(row, 0)}:{row_value(row, 1)}:{row_value(row, 3)}"
        if row_key in processed_bazar_rows or not complete_bazar_row(row) or is_sent_status(status):
            continue
        date = row_value(row, 0)
        buyer = normalize_name(row_value(row, 1))
        typ = row_value(row, 2)
        total = parse_amount(row_value(row, 3))
        share = parse_amount(row_value(row, 4)) or stats["share_per_head"]
        note = row_value(row, 5)
        entry = {"date": date, "buyer": buyer, "type": typ, "total": total, "share": share}
        await send_admin(
            bot,
            f"📋 BAZAR DETAILS\n\n👤 Buyer: {buyer}\n📅 Date: {date}\n🧾 Category: {typ}\n💰 Total: {format_lkr(total)} LKR\n👥 Share: {format_lkr(share)} LKR\n📝 Note: {note}",
            data,
        )
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
    rows = data["payment_rows"]
    stats = data["stats"]
    member_map = data["member_map"]
    if len(rows) < 4:
        return False
    changed = False
    for idx, row in enumerate(rows[3:], start=4):
        status = row_value(row, 5)
        row_key = f"payment:{idx}:{row_value(row, 0)}:{row_value(row, 1)}:{row_value(row, 2)}"
        if row_key in processed_payment_rows or not complete_payment_row(row) or is_sent_status(status):
            continue
        date = row_value(row, 0)
        member = normalize_name(row_value(row, 1))
        amount = parse_amount(row_value(row, 2))
        typ = row_value(row, 3)
        note = row_value(row, 4)
        user_id = member_map.get(member)
        wallet_now = stats["members"].get(member, {}).get("wallet", 0)
        entry = {"date": date, "member": member, "amount": amount, "type": typ, "note": note}
        await send_admin(
            bot,
            f"📋 PAYMENT DETAILS\n\n👤 Member: {member}\n📅 Date: {date}\n💰 Amount: {format_lkr(amount)} LKR\n📦 Type: {typ}\n📝 Note: {note}\n💼 Wallet Now: {format_lkr(wallet_now)} LKR",
            data,
        )
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
    stats = data["stats"]
    threshold = stats["threshold"]
    member_map = data["member_map"]
    group_id = data.get("admin_group_id", "")
    for member, m in stats["members"].items():
        wallet = m["wallet"]
        status = m["status"]
        key = f"{stats['month']}:{member}"
        if wallet < threshold:
            if key in low_alert_sent_cache:
                continue
            group_msg = (
                f"⚠️ LOW WALLET AUTO ALERT\n\n"
                f"👤 Member: {member}\n"
                f"💸 Wallet: {format_lkr(wallet)} LKR\n"
                f"💳 Suggested Top-up: {format_lkr(threshold - wallet)} LKR\n"
                f"📌 Status: {status}\n"
                f"📉 Threshold: {format_lkr(threshold)} LKR"
            )
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
    member = normalize_name(get_member_name_by_user_id(data, uid) or update.effective_user.first_name or "UNKNOWN")
    low = text.lower().strip()
        # Office group savage mode. এখানে market/bazar logic চলবে না.
    if await handle_office_group_message(update, context, text, uid):
        return

    # Confirmation flow: no admin/sheet update until user OK.
    if low in ["ok", "okay", "ওকে", "ঠিক আছে", "হ্যাঁ", "ha", "yes"]:
        pending = user_pending.get(uid)
        if pending and pending.get("type") == "BAZAR":
            p = pending
            user_pending.pop(uid, None)
            await asyncio.to_thread(
                append_row,
                PENDING_BAZAR_SHEET,
                [p["id"], now_str(), uid, member, p["raw"], p["items"], p["total"], "USER_OK", "PENDING", "", p.get("note", "")]
            )
            await send_admin(
                context.bot,
                f"🆕 নতুন বাজার approval দরকার\n\nID: {p['id']}\n👤 Buyer: {member}\n🧾 Items: {p['items']}\n💰 Total: {format_lkr(p['total'])} LKR\n\nApprove: /approve {p['id']}\nReject: /reject {p['id']}",
                data,
            )
            reply = "✅ বাজারটা admin approval-এ পাঠানো হয়েছে। Admin approve দিলেই আগের নিয়মে sheet update + সবার message যাবে।"
            await update.message.reply_text(reply)
            await asyncio.to_thread(save_chat_log, uid, member, text, reply, "BAZAR_OK")
            return

        if pending and pending.get("type") == "NEED":
            p = pending
            user_pending.pop(uid, None)
            saved_ids = []
            for item in p["items"]:
                need_id = generate_id("ND")
                saved_ids.append((need_id, item))
                await asyncio.to_thread(
                    append_row,
                    NEED_LIST_SHEET,
                    [need_id, now_str(), uid, member, item, "WAITING_ADMIN", "", "", p["raw"], ""]
                )
            admin_msg = "📝 NEED LIST APPROVAL দরকার\n\n" + f"👤 Added by: {member}\n"
            for need_id, item in saved_ids:
                admin_msg += f"\n• {item}\nApprove: /needapprove {need_id}\nReject: /needreject {need_id}\n"
            await send_admin(context.bot, admin_msg, data)
            reply = "✅ লিস্টটা admin approval-এ পাঠানো হয়েছে।"
            await update.message.reply_text(reply)
            await asyncio.to_thread(save_chat_log, uid, member, text, reply, "NEED_OK")
            return
        # If no pending, continue to AI chat.

    if low in ["cancel", "/cancel"]:
        user_pending.pop(uid, None)
        await update.message.reply_text("✅ Cancel করা হয়েছে।")
        return

    # Bazar draft -> ask user confirmation.
    bazar = parse_bazar_text(text)
    if bazar:
        pending_id = generate_id("BZ")
        user_pending[uid] = {"type": "BAZAR", "id": pending_id, "raw": text, "items": bazar["items"], "total": bazar["total"], "note": bazar["note"]}
        reply = (
            f"🛒 বাজারটা আমি এভাবে বুঝেছি:\n\n"
            f"👤 Buyer: {member}\n"
            f"🧾 Items: {bazar['items']}\n"
            f"💰 Total: {format_lkr(bazar['total'])} LKR\n\n"
            "ঠিক থাকলে OK লিখো ✅\nভুল হলে /cancel"
        )
        await update.message.reply_text(reply)
        await asyncio.to_thread(save_chat_log, uid, member, text, reply, "BAZAR_DRAFT")
        return

    # Need draft -> ask user confirmation.
    need_items = parse_need_list_text(text)
    if need_items:
        user_pending[uid] = {"type": "NEED", "raw": text, "items": need_items}
        reply = "📝 বাজার লিস্টে add করার আগে confirm করো:\n\n" + "\n".join([f"• {i}" for i in need_items]) + "\n\nঠিক থাকলে OK লিখো ✅\nভুল হলে /cancel"
        await update.message.reply_text(reply)
        await asyncio.to_thread(save_chat_log, uid, member, text, reply, "NEED_DRAFT")
        return

    # Memory learning: rule first, AI second. Silent save; error হলেও reply বন্ধ হবে না.
    try:
        memory = await asyncio.to_thread(rule_extract_memory, text, member, data, uid)
        if not memory:
            memory = await asyncio.to_thread(ai_extract_memory, text, member, data)
        if memory:
            await asyncio.to_thread(upsert_user_personality, memory, data)
            data = await refresh_cache()
    except Exception as exc:
        print("Memory save error:", repr(exc))
        print(traceback.format_exc())

    try:
        reply = await asyncio.to_thread(final_chat_reply, text, member, data)
    except Exception as exc:
        print("Final chat reply error:", repr(exc))
        print(traceback.format_exc())
        reply = savage_fallback_reply(text, member, data)

    if not reply:
        reply = savage_fallback_reply(text, member, data)

    await update.message.reply_text(reply)
    await asyncio.to_thread(save_chat_log, uid, member, text, reply, "CHAT")

# =========================================================
# BOT SETUP
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
        BotCommand("refresh", "Admin refresh cache"),
        BotCommand("repair_on", "Repair mode ON"),
        BotCommand("repair_off", "Repair mode OFF"),
        BotCommand("status", "Bot status"),
        BotCommand("approve", "Approve pending bazar"),
        BotCommand("reject", "Reject pending bazar"),
        BotCommand("needapprove", "Approve need item"),
        BotCommand("needreject", "Reject need item"),
        BotCommand("needremove", "Remove need item"),
        BotCommand("needdone", "Mark need item bought"),
        BotCommand("needclear", "Clear pending need list"),
    ]
    await application.bot.set_my_commands(commands)
    asyncio.create_task(auto_scan_loop(application.bot))


def main():
    require_env()
    print("Market Hisab Bot V3 Final Clean running...")
    print("Spreadsheet ID:", SPREADSHEET_ID)
    print("Service account:", get_service_account_email())
    print("AI Enabled:", AI_ENABLED)
    print("AI Provider:", AI_PROVIDER)
    print("Text Model:", TEXT_MODEL)
    print("Roast Level:", ROAST_LEVEL)

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
    app.add_handler(CommandHandler("needapprove", needapprove_cmd))
    app.add_handler(CommandHandler("needreject", needreject_cmd))
    app.add_handler(CommandHandler("needremove", needremove_cmd))
    app.add_handler(CommandHandler("needdone", needdone_cmd))
    app.add_handler(CommandHandler("needclear", needclear_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, normal_message_handler))
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
