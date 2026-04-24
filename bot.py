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

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "180"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "60"))

SEND_LOW_PERSONAL = os.getenv("SEND_LOW_PERSONAL", "true").strip().lower() == "true"
SEND_ADMIN_DETAILS = os.getenv("SEND_ADMIN_DETAILS", "true").strip().lower() == "true"

SETTINGS_SHEET = "Settings"
BAZAR_SHEET = "Bazar_Entry"
PAYMENT_SHEET = "Payment_Entry"
TELEGRAM_SETUP_SHEET = "Telegram_Setup"

repair_mode = False
low_alert_sent_cache = set()
processed_bazar_rows = set()
processed_payment_rows = set()

cache_lock = asyncio.Lock()
send_lock = asyncio.Lock()

CACHE = {
    "loaded_at": None,
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


def require_env():
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON")
    if missing:
        raise RuntimeError("Missing ENV: " + ", ".join(missing))


def now_utc():
    return datetime.now(timezone.utc)


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
            return datetime.strptime(s, fmt).strftime("%Y-%m")
        except ValueError:
            pass

    if len(s) >= 7 and s[4] in "-/":
        return s[:7].replace("/", "-")

    return ""


def get_service_account_email():
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        return info.get("client_email", "")
    except Exception:
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


def safe_update_cell(sheet_name, row, col, value):
    sh = get_spreadsheet().worksheet(sheet_name)
    sh.update_cell(row, col, value)


def get_wallet_status(wallet, threshold):
    if wallet < 0:
        return "NEGATIVE"
    if wallet < threshold:
        return "LOW"
    return "OK"


def is_sent_status(value):
    return "SENT" in str(value or "").upper()


def complete_bazar_row(row):
    return bool(
        row_value(row, 0)
        and row_value(row, 1)
        and row_value(row, 2)
        and parse_amount(row_value(row, 3)) > 0
    )


def complete_payment_row(row):
    return bool(
        row_value(row, 0)
        and row_value(row, 1)
        and parse_amount(row_value(row, 2)) > 0
        and row_value(row, 3)
    )


def load_all_data_from_google():
    ss = get_spreadsheet()
    worksheets = ss.worksheets()
    title_map = {w.title: w for w in worksheets}

    def get_rows(sheet_name):
        if sheet_name not in title_map:
            return []
        return title_map[sheet_name].get_all_values()

    settings_rows = get_rows(SETTINGS_SHEET)
    bazar_rows = get_rows(BAZAR_SHEET)
    payment_rows = get_rows(PAYMENT_SHEET)
    telegram_rows = get_rows(TELEGRAM_SETUP_SHEET)

    selected_month = ""
    low_threshold = 500.0
    member_map = {}
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


def build_stats_from_rows(data):
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
            "share_deduction
