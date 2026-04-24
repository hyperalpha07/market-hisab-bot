import os
import json
import asyncio
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes

# =========================================================
# MARKET HISAB BOT - FINAL ULTRA STABLE VERSION
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "180"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "60"))

SEND_LOW_PERSONAL = os.getenv("SEND_LOW_PERSONAL", "true").strip().lower() == "true"
SEND_ADMIN_DETAILS = os.getenv("SEND_ADMIN_DETAILS", "true").strip().lower() == "true"
ADMIN_USER_IDS = os.getenv("ADMIN_USER_IDS", "").strip()

SETTINGS_SHEET = "Settings"
BAZAR_SHEET = "Bazar_Entry"
PAYMENT_SHEET = "Payment_Entry"
TELEGRAM_SETUP_SHEET = "Telegram_Setup"

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
    sh = get_spreadsheet().worksheet(sheet_name)
    sh.update_cell(row, col, value)


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
    user_id = str(user_id).strip()
    for name, uid in data["member_map"].items():
        if str(uid).strip() == user_id:
            return name
    return None


def build_help_message() -> str:
    mode = "ON 🔧" if repair_mode else "OFF ✅"
    return (
        "👋 Market Hisab Bot\n\n"
        "📌 Member Commands:\n"
        "/wallet - My wallet details\n"
        "/summary - Full month summary\n"
        "/low - Low wallet list\n"
        "/id - My Telegram ID\n\n"
        "🛠 Admin Commands:\n"
        "/debug - Check sheet connection\n"
        "/refresh - Refresh sheet cache\n"
        "/repair_on - Admin only repair mode ON\n"
        "/repair_off - Admin only repair mode OFF\n"
        "/status - Bot status\n\n"
        f"🔧 Repair Mode: {mode}"
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
            f"👥 Share: {format_lkr(m['share_deduction'])} LKR\n"
            f"💰 Wallet: {format_lkr(m['wallet'])} LKR\n"
            f"📌 Status: {m['status']}\n\n"
        )
    return msg


def build_low_wallet_list(stats: Dict[str, Any]) -> str:
    threshold = stats["threshold"]
    msg = (
        "⚠️ LOW WALLET MEMBERS\n\n"
        f"📅 Month: {stats['month']}\n"
        f"📉 Threshold: {format_lkr(threshold)} LKR\n\n"
    )
    found = False
    for name, m in stats["members"].items():
        if m["wallet"] < threshold:
            found = True
            suggested = threshold - m["wallet"]
            msg += (
                f"👤 {name}\n"
                f"💸 Wallet: {format_lkr(m['wallet'])} LKR\n"
                f"💳 Suggested Top-up: {format_lkr(suggested)} LKR\n"
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
    suggested = threshold - wallet if wallet < threshold else 0
    return (
        "⚠️ LOW WALLET ALERT\n\n"
        f"👤 Member: {member}\n"
        f"💸 Current Wallet: {format_lkr(wallet)} LKR\n"
        f"💳 Suggested Top-up: {format_lkr(suggested)} LKR\n"
        f"📌 Status: {status}"
    )


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
            f"Repair Mode: {'ON' if repair_mode else 'OFF'}\n\n"
            "Sheets:\n- " + "\n- ".join(data["sheet_titles"])
        )
    except Exception as exc:
        await update.message.reply_text(
            "❌ Google Sheet connection failed.\n\n"
            f"Error:\n{type(exc).__name__}: {exc}\n\n"
            f"Service account:\n{get_service_account_email()}\n\n"
            f"Spreadsheet ID:\n{SPREADSHEET_ID}"
        )
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
        print(traceback.format_exc())


async def wallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = await get_cached_data()
        member = get_member_name_by_user_id(data, update.effective_user.id)
        if not member:
            await update.message.reply_text(
                "❌ তোমার Telegram User ID member list-এ পাওয়া যায়নি।\n\n"
                f"তোমার ID: {update.effective_user.id}\n\n"
                "Settings sheet-এর Telegram User ID column-এ এই ID add করতে হবে।"
            )
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
    await update.message.reply_text(
        "🔧 Repair Mode ON\n\n"
        "Auto member notifications are now paused.\n"
        "Only admin/admin group will receive update notices."
    )


async def repair_off_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global repair_mode
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ You are not admin.")
        return
    repair_mode = False
    await update.message.reply_text(
        "✅ Repair Mode OFF\n\n"
        "Auto member notifications are active again."
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = await get_cached_data()
    await update.message.reply_text(
        "🤖 BOT STATUS\n\n"
        f"Repair Mode: {'ON 🔧' if repair_mode else 'OFF ✅'}\n"
        f"Month: {data['selected_month']}\n"
        f"Members: {len(data['member_map'])}\n"
        f"Cache TTL: {CACHE_TTL_SECONDS}s\n"
        f"Auto Scan: {SCAN_INTERVAL_SECONDS}s\n"
        f"Admin IDs: {ADMIN_USER_IDS or 'Not set'}"
    )


def is_sent_status(value: str) -> bool:
    return "SENT" in str(value or "").upper()


def complete_bazar_row(row: List[str]) -> bool:
    return bool(
        row_value(row, 0)
        and row_value(row, 1)
        and row_value(row, 2)
        and parse_amount(row_value(row, 3)) > 0
    )


def complete_payment_row(row: List[str]) -> bool:
    return bool(
        row_value(row, 0)
        and row_value(row, 1)
        and parse_amount(row_value(row, 2)) > 0
        and row_value(row, 3)
    )


async def send_admin(bot, text: str, data: Dict[str, Any]) -> None:
    if not SEND_ADMIN_DETAILS:
        return
    group_id = data.get("admin_group_id", "")
    if group_id:
        await bot.send_message(chat_id=group_id, text=text)


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

        if row_key in processed_bazar_rows:
            continue
        if not complete_bazar_row(row) or is_sent_status(status):
            continue

        date = row_value(row, 0)
        buyer = normalize_name(row_value(row, 1))
        typ = row_value(row, 2)
        total = parse_amount(row_value(row, 3))
        share = parse_amount(row_value(row, 4)) or stats["share_per_head"]
        note = row_value(row, 5)

        entry = {"date": date, "buyer": buyer, "type": typ, "total": total, "share": share}

        admin_msg = (
            "📋 BAZAR DETAILS\n\n"
            f"👤 Buyer: {buyer}\n"
            f"📅 Date: {date}\n"
            f"🧾 Category: {typ}\n"
            f"💰 Total: {format_lkr(total)} LKR\n"
            f"👥 Share: {format_lkr(share)} LKR\n"
            f"📝 Note: {note}\n\n"
            f"🔧 Repair Mode: {'ON' if repair_mode else 'OFF'}"
        )
        await send_admin(bot, admin_msg, data)

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

        status_text = "SENT ADMIN ONLY" if repair_mode else f"SENT: {success}"
        await asyncio.to_thread(safe_update_cell, BAZAR_SHEET, idx, 7, status_text)

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

        if row_key in processed_payment_rows:
            continue
        if not complete_payment_row(row) or is_sent_status(status):
            continue

        date = row_value(row, 0)
        member = normalize_name(row_value(row, 1))
        amount = parse_amount(row_value(row, 2))
        typ = row_value(row, 3)
        note = row_value(row, 4)
        user_id = member_map.get(member)
        wallet_now = stats["members"].get(member, {}).get("wallet", 0)

        entry = {"date": date, "member": member, "amount": amount, "type": typ, "note": note}

        admin_msg = (
            "📋 PAYMENT DETAILS\n\n"
            f"👤 Member: {member}\n"
            f"📅 Date: {date}\n"
            f"💰 Amount: {format_lkr(amount)} LKR\n"
            f"📦 Type: {typ}\n"
            f"📝 Note: {note}\n"
            f"💼 Wallet Now: {format_lkr(wallet_now)} LKR\n\n"
            f"🔧 Repair Mode: {'ON' if repair_mode else 'OFF'}"
        )
        await send_admin(bot, admin_msg, data)

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
                "⚠️ LOW WALLET AUTO ALERT\n\n"
                f"👤 Member: {member}\n"
                f"💸 Wallet: {format_lkr(wallet)} LKR\n"
                f"💳 Suggested Top-up: {format_lkr(threshold - wallet)} LKR\n"
                f"📌 Status: {status}\n"
                f"📉 Threshold: {format_lkr(threshold)} LKR\n\n"
                f"🔧 Repair Mode: {'ON' if repair_mode else 'OFF'}"
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


async def post_init(application: Application):
    commands = [
        BotCommand("start", "Start bot and show help"),
        BotCommand("help", "Show all commands"),
        BotCommand("wallet", "My wallet details"),
        BotCommand("summary", "Full month summary"),
        BotCommand("low", "Low wallet list"),
        BotCommand("id", "My Telegram ID"),
        BotCommand("debug", "Check Google Sheet connection"),
        BotCommand("refresh", "Admin only refresh cache"),
        BotCommand("repair_on", "Admin only repair mode ON"),
        BotCommand("repair_off", "Admin only repair mode OFF"),
        BotCommand("status", "Bot status"),
    ]
    await application.bot.set_my_commands(commands)
    asyncio.create_task(auto_scan_loop(application.bot))


def main():
    require_env()

    print("Market Hisab Bot is running...")
    print("Spreadsheet ID:", SPREADSHEET_ID)
    print("Service account:", get_service_account_email())
    print("Cache TTL:", CACHE_TTL_SECONDS)
    print("Scan interval:", SCAN_INTERVAL_SECONDS)
    print("Admin IDs:", ADMIN_USER_IDS)

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("debug", debug_cmd))
    app.add_handler(CommandHandler("refresh", refresh_cmd))
    app.add_handler(CommandHandler(["wallet", "balance", "me"], wallet_cmd))
    app.add_handler(CommandHandler("summary", summary_cmd))
    app.add_handler(CommandHandler("low", low_cmd))
    app.add_handler(CommandHandler("repair_on", repair_on_cmd))
    app.add_handler(CommandHandler("repair_off", repair_off_cmd))
    app.add_handler(CommandHandler("status", status_cmd))

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
