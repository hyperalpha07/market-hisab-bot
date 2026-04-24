import os
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "60"))
SEND_LOW_PERSONAL = os.getenv("SEND_LOW_PERSONAL", "true").strip().lower() == "true"
SEND_ADMIN_DETAILS = os.getenv("SEND_ADMIN_DETAILS", "true").strip().lower() == "true"

SETTINGS_SHEET = "Settings"
BAZAR_SHEET = "Bazar_Entry"
PAYMENT_SHEET = "Payment_Entry"
TELEGRAM_SETUP_SHEET = "Telegram_Setup"

low_alert_sent_cache = set()

def require_env():
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
    if missing:
        raise RuntimeError("Missing environment variables: " + ", ".join(missing))

def parse_amount(value):
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

def format_lkr(value):
    amount = parse_amount(value)
    if amount == int(amount):
        return f"{int(amount):,}"
    return f"{amount:,.2f}"

def normalize_name(value):
    return str(value or "").strip().upper()

def row_value(row, index):
    return str(row[index]).strip() if index < len(row) else ""

def month_from_date(value):
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m")
        except ValueError:
            pass
    if len(s) >= 7 and s[4] in "-/":
        return s[:7].replace("/", "-")
    return ""

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

def ws(name):
    return get_spreadsheet().worksheet(name)

def get_selected_month():
    sheet = ws(SETTINGS_SHEET)
    value = sheet.acell("B3").value or sheet.acell("B2").value or ""
    return month_from_date(value) or str(value).strip()

def get_low_threshold():
    sheet = ws(SETTINGS_SHEET)
    return parse_amount(sheet.acell("B5").value or 500) or 500

def get_admin_group_id():
    sheet = ws(TELEGRAM_SETUP_SHEET)
    return str(sheet.acell("B4").value or "").strip()

def get_member_map():
    sheet = ws(SETTINGS_SHEET)
    rows = sheet.get("B9:D12")
    result = {}
    for row in rows:
        name = normalize_name(row_value(row, 0))
        user_id = row_value(row, 1)
        active = normalize_name(row_value(row, 2))
        if name and user_id and active == "YES":
            result[name] = str(user_id).strip()
    return result

def get_member_name_by_user_id(user_id):
    user_id = str(user_id).strip()
    for name, uid in get_member_map().items():
        if str(uid).strip() == user_id:
            return name
    return None

def get_wallet_status(wallet, threshold):
    if wallet < 0:
        return "NEGATIVE"
    if wallet < threshold:
        return "LOW"
    return "OK"

def get_month_stats(month_key=None):
    month_key = month_key or get_selected_month()
    threshold = get_low_threshold()
    members = get_member_map()

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

    try:
        payment_rows = ws(PAYMENT_SHEET).get_all_values()[3:]
        for row in payment_rows:
            row_month = month_from_date(row_value(row, 0))
            member = normalize_name(row_value(row, 1))
            amount = parse_amount(row_value(row, 2))
            if row_month == month_key and member in stats["members"] and amount:
                stats["members"][member]["topup"] += amount
                stats["total_topup"] += amount
    except Exception as exc:
        print("Payment stats error:", exc)

    try:
        bazar_rows = ws(BAZAR_SHEET).get_all_values()[3:]
        for row in bazar_rows:
            row_month = month_from_date(row_value(row, 0))
            buyer = normalize_name(row_value(row, 1))
            total = parse_amount(row_value(row, 3))
            if row_month == month_key and total:
                stats["total_expense"] += total
                if buyer in stats["members"]:
                    stats["members"][buyer]["own_expense"] += total
    except Exception as exc:
        print("Bazar stats error:", exc)

    count = max(len(stats["members"]), 1)
    stats["share_per_head"] = stats["total_expense"] / count

    for name, m in stats["members"].items():
        m["share_deduction"] = stats["share_per_head"]
        m["wallet"] = m["topup"] - m["share_deduction"]
        m["status"] = get_wallet_status(m["wallet"], threshold)
        stats["total_wallet_left"] += m["wallet"]

    return stats

def build_help_message():
    return (
        "👋 Market Hisab Bot\n\n"
        "Available commands:\n\n"
        "/wallet - Show my wallet status\n"
        "/balance - Show my current balance\n"
        "/me - Show my personal wallet details\n"
        "/summary - Show current month summary\n"
        "/low - Show low wallet members\n"
        "/id - Show my Telegram ID\n"
        "/help - Show all commands"
    )

def build_wallet_message(member_name, stats):
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
        f"📌 Status: {m['status']}"
    )

def build_summary_message(stats):
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

def build_low_wallet_list(stats):
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
            msg += f"👤 {name}\n💸 Wallet: {format_lkr(m['wallet'])} LKR\n📌 Status: {m['status']}\n\n"
    if not found:
        msg += "✅ এখন কোনো low wallet member নেই।"
    return msg

def build_bazar_message(entry, stats, member_name):
    m = stats["members"].get(member_name, {"wallet": 0, "status": "OK"})
    return (
        "🛒 BAZAR UPDATE\n\n"
        f"👤 Buyer: {entry['buyer']}\n"
        f"📅 Date: {entry['date']}\n"
        f"🧾 Type: {entry['type']}\n"
        f"💰 Total Expense: {format_lkr(entry['total'])} LKR\n"
        f"👥 Per Person Share: {format_lkr(entry['share'])} LKR\n\n"
        f"🙍 Your Name: {member_name}\n"
        f"💼 Your Wallet Now: {format_lkr(m['wallet'])} LKR\n"
        f"📌 Status: {m['status']}\n\n"
        f"📊 Month Total Expense: {format_lkr(stats['total_expense'])} LKR\n"
        f"💼 Total Wallet Left: {format_lkr(stats['total_wallet_left'])} LKR"
    )

def build_payment_message(entry, wallet_now, threshold):
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

def build_low_wallet_personal(member, wallet, status, threshold):
    suggested = threshold - wallet if wallet < threshold else 0
    return (
        "⚠️ LOW WALLET ALERT\n\n"
        f"👤 Member: {member}\n"
        f"💸 Current Wallet: {format_lkr(wallet)} LKR\n"
        f"📌 Status: {status}\n"
        f"💳 Suggested Top-up: {format_lkr(suggested)} LKR"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(build_help_message())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(build_help_message())

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 Your Telegram ID: {update.effective_user.id}")

async def wallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    member = await asyncio.to_thread(get_member_name_by_user_id, user_id)
    if not member:
        await update.message.reply_text(
            "❌ তোমার Telegram User ID member list-এ পাওয়া যায়নি।\n\n"
            f"তোমার ID: {user_id}\n\n"
            "Settings sheet-এর Telegram User ID column-এ এই ID add করতে হবে।"
        )
        return
    stats = await asyncio.to_thread(get_month_stats)
    await update.message.reply_text(build_wallet_message(member, stats))

async def summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await asyncio.to_thread(get_month_stats)
    await update.message.reply_text(build_summary_message(stats))

async def low_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await asyncio.to_thread(get_month_stats)
    await update.message.reply_text(build_low_wallet_list(stats))

def is_sent_status(value):
    return "SENT" in str(value or "").upper()

def complete_bazar_row(row):
    return bool(row_value(row, 0) and row_value(row, 1) and row_value(row, 2) and parse_amount(row_value(row, 3)) > 0)

def complete_payment_row(row):
    return bool(row_value(row, 0) and row_value(row, 1) and parse_amount(row_value(row, 2)) > 0 and row_value(row, 3))

async def send_admin(bot, text):
    if not SEND_ADMIN_DETAILS:
        return
    group_id = await asyncio.to_thread(get_admin_group_id)
    if group_id:
        await bot.send_message(chat_id=group_id, text=text)

async def scan_bazar(bot):
    sheet = ws(BAZAR_SHEET)
    rows = await asyncio.to_thread(sheet.get_all_values)
    if len(rows) < 4:
        return
    stats = await asyncio.to_thread(get_month_stats)
    member_map = await asyncio.to_thread(get_member_map)
    for idx, row in enumerate(rows[3:], start=4):
        status = row_value(row, 6)
        if not complete_bazar_row(row) or is_sent_status(status):
            continue
        date = row_value(row, 0)
        buyer = normalize_name(row_value(row, 1))
        typ = row_value(row, 2)
        total = parse_amount(row_value(row, 3))
        share = parse_amount(row_value(row, 4)) or stats["share_per_head"]
        note = row_value(row, 5)
        entry = {"date": date, "buyer": buyer, "type": typ, "total": total, "share": share}
        await send_admin(bot, f"📋 BAZAR DETAILS\n\n👤 Buyer: {buyer}\n📅 Date: {date}\n🧾 Category: {typ}\n💰 Total: {format_lkr(total)} LKR\n👥 Share: {format_lkr(share)} LKR\n📝 Note: {note}")
        success = 0
        for member_name, user_id in member_map.items():
            try:
                await bot.send_message(chat_id=user_id, text=build_bazar_message(entry, stats, member_name))
                success += 1
            except Exception as exc:
                print("Bazar send error:", member_name, exc)
        await asyncio.to_thread(sheet.update_cell, idx, 7, f"SENT: {success}")

async def scan_payment(bot):
    sheet = ws(PAYMENT_SHEET)
    rows = await asyncio.to_thread(sheet.get_all_values)
    if len(rows) < 4:
        return
    stats = await asyncio.to_thread(get_month_stats)
    member_map = await asyncio.to_thread(get_member_map)
    for idx, row in enumerate(rows[3:], start=4):
        status = row_value(row, 5)
        if not complete_payment_row(row) or is_sent_status(status):
            continue
        date = row_value(row, 0)
        member = normalize_name(row_value(row, 1))
        amount = parse_amount(row_value(row, 2))
        typ = row_value(row, 3)
        note = row_value(row, 4)
        user_id = member_map.get(member)
        if not user_id:
            await asyncio.to_thread(sheet.update_cell, idx, 6, "Member user id missing")
            continue
        wallet_now = stats["members"].get(member, {}).get("wallet", 0)
        entry = {"date": date, "member": member, "amount": amount, "type": typ, "note": note}
        await send_admin(bot, f"📋 PAYMENT DETAILS\n\n👤 Member: {member}\n📅 Date: {date}\n💰 Amount: {format_lkr(amount)} LKR\n📦 Type: {typ}\n📝 Note: {note}\n💼 Wallet Now: {format_lkr(wallet_now)} LKR")
        try:
            await bot.send_message(chat_id=user_id, text=build_payment_message(entry, wallet_now, stats["threshold"]))
            await asyncio.to_thread(sheet.update_cell, idx, 6, f"SENT TO {member}")
        except Exception as exc:
            print("Payment send error:", member, exc)
            await asyncio.to_thread(sheet.update_cell, idx, 6, "SEND FAILED")

async def scan_low_wallet(bot):
    stats = await asyncio.to_thread(get_month_stats)
    threshold = stats["threshold"]
    member_map = await asyncio.to_thread(get_member_map)
    group_id = await asyncio.to_thread(get_admin_group_id)
    for member, m in stats["members"].items():
        wallet = m["wallet"]
        status = m["status"]
        key = f"{stats['month']}:{member}"
        if wallet < threshold:
            if key in low_alert_sent_cache:
                continue
            group_msg = f"⚠️ LOW WALLET AUTO ALERT\n\n👤 Member: {member}\n💸 Wallet: {format_lkr(wallet)} LKR\n📌 Status: {status}\n📉 Threshold: {format_lkr(threshold)} LKR"
            if group_id:
                await bot.send_message(chat_id=group_id, text=group_msg)
            if SEND_LOW_PERSONAL and member_map.get(member):
                await bot.send_message(chat_id=member_map[member], text=build_low_wallet_personal(member, wallet, status, threshold))
            low_alert_sent_cache.add(key)
        else:
            low_alert_sent_cache.discard(key)

async def auto_scan_loop(bot):
    await asyncio.sleep(5)
    while True:
        try:
            await scan_bazar(bot)
            await scan_payment(bot)
            await scan_low_wallet(bot)
        except Exception as exc:
            print("Auto scanner error:", exc)
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def post_init(application: Application):
    commands = [
        BotCommand("start", "Start bot and show help"),
        BotCommand("help", "Show all commands"),
        BotCommand("wallet", "Show my wallet status"),
        BotCommand("balance", "Show my current balance"),
        BotCommand("me", "Show my personal wallet details"),
        BotCommand("summary", "Show current month summary"),
        BotCommand("low", "Show low wallet members"),
        BotCommand("id", "Show my Telegram ID"),
    ]
    await application.bot.set_my_commands(commands)
    asyncio.create_task(auto_scan_loop(application.bot))

def main():
    require_env()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler(["wallet", "balance", "me"], wallet_cmd))
    app.add_handler(CommandHandler("summary", summary_cmd))
    app.add_handler(CommandHandler("low", low_cmd))
    print("Market Hisab Bot is running...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
