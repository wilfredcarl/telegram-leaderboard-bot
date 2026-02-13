import os
import sqlite3
import time as time_mod
import asyncio
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    CommandHandler,
    CallbackQueryHandler,
    filters,
)

# ----------------------------
# Config
# ----------------------------
TOKEN = os.getenv("TOKEN")
TZ = ZoneInfo("America/Montreal")

# Only the tiny group notice should auto-delete
DM_NOTICE_SECONDS = 5

DB_PATH = "leaderboard.db"

def parse_int_env(name: str, default: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"⚠️ Invalid {name} env value: {raw!r}. Using {default}.")
        return default

# ✅ Recommended: set this in Railway Variables to your single group
# DEFAULT_GROUP_CHAT_ID = -1001234567890
DEFAULT_GROUP_CHAT_ID = parse_int_env("DEFAULT_GROUP_CHAT_ID", 0)


# ----------------------------
# Database
# ----------------------------
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
db_lock = asyncio.Lock()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,

    -- monthly (reset each month)
    text_count INTEGER DEFAULT 0,
    media_count INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,

    -- all-time (never reset)
    all_text_count INTEGER DEFAULT 0,
    all_media_count INTEGER DEFAULT 0,
    all_total_count INTEGER DEFAULT 0,

    PRIMARY KEY (chat_id, user_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS hall_of_fame (
    chat_id INTEGER NOT NULL,
    month TEXT NOT NULL,
    rank INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,
    total_count INTEGER NOT NULL,
    text_count INTEGER NOT NULL,
    media_count INTEGER NOT NULL,
    PRIMARY KEY (chat_id, month, rank)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS pending_messages (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,
    kind TEXT NOT NULL,              -- 'text' or 'media'
    created_at_utc INTEGER NOT NULL, -- unix timestamp
    PRIMARY KEY (chat_id, message_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_context (
    user_id INTEGER PRIMARY KEY,
    last_chat_id INTEGER NOT NULL
)
""")

conn.commit()

def ensure_all_time_columns():
    cursor.execute("PRAGMA table_info(users)")
    cols = {row[1] for row in cursor.fetchall()}
    needed = {
        "all_text_count": "ALTER TABLE users ADD COLUMN all_text_count INTEGER DEFAULT 0",
        "all_media_count": "ALTER TABLE users ADD COLUMN all_media_count INTEGER DEFAULT 0",
        "all_total_count": "ALTER TABLE users ADD COLUMN all_total_count INTEGER DEFAULT 0",
    }
    for col, stmt in needed.items():
        if col not in cols:
            cursor.execute(stmt)
    conn.commit()

ensure_all_time_columns()


# ----------------------------
# Meta helpers
# ----------------------------
def _get_meta_sync(key: str) -> str | None:
    cursor.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else None

def _set_meta_sync(key: str, value: str):
    cursor.execute("""
        INSERT INTO meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (key, value))

async def get_meta(key: str) -> str | None:
    async with db_lock:
        return _get_meta_sync(key)

async def set_meta(key: str, value: str):
    async with db_lock:
        _set_meta_sync(key, value)
        conn.commit()


# ----------------------------
# Telegram helpers
# ----------------------------
async def safe_delete_message(message):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass

def schedule_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay_seconds: int):
    async def _delete_cb(ctx: ContextTypes.DEFAULT_TYPE):
        try:
            await ctx.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
    try:
        context.job_queue.run_once(_delete_cb, when=delay_seconds)
    except Exception:
        pass


# ----------------------------
# Context helpers
# ----------------------------
async def save_user_context(user_id: int, chat_id: int):
    async with db_lock:
        cursor.execute("""
            INSERT INTO user_context (user_id, last_chat_id)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET last_chat_id = excluded.last_chat_id
        """, (user_id, chat_id))
        conn.commit()

async def get_user_last_chat(user_id: int) -> int | None:
    async with db_lock:
        cursor.execute("SELECT last_chat_id FROM user_context WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        return row[0] if row else None

async def resolve_group_chat_id(update: Update | None) -> int | None:
    # single-group mode
    if DEFAULT_GROUP_CHAT_ID:
        return DEFAULT_GROUP_CHAT_ID

    # fallback: whatever group user last used a command in
    if update and update.effective_chat and update.effective_user:
        if update.effective_chat.type in ("group", "supergroup"):
            return update.effective_chat.id
        return await get_user_last_chat(update.effective_user.id)

    return None


# ----------------------------
# DM-only sending (group notice 5s)
# ----------------------------
async def send_dm_only(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, *,
                       parse_mode: str | None = None, reply_markup=None):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return

    # If already in private chat → just send DM (no auto-delete)
    if chat.type == "private":
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        return

    # Save last group for this user (helpful if DEFAULT_GROUP_CHAT_ID isn't set during testing)
    try:
        await save_user_context(user.id, chat.id)
    except Exception as e:
        print("save_user_context failed:", repr(e))

    # Try DM
    dm_ok = False
    try:
        await context.bot.send_message(
            chat_id=user.id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        dm_ok = True
    except Exception as e:
        print("DM failed:", repr(e))

    # Always try to notify the group (5 seconds only)
    try:
        if dm_ok:
            notice = await context.bot.send_message(chat_id=chat.id, text="📩 Sent you a DM.")
        else:
            notice = await context.bot.send_message(
                chat_id=chat.id,
                text="❗ I couldn't DM you. Please open the bot in private and press Start, then try again.",
            )
        schedule_delete(context, chat.id, notice.message_id, DM_NOTICE_SECONDS)
    except Exception as e:
        print("Group notice failed:", repr(e))


# ----------------------------
# UI Keyboards
# ----------------------------
def home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏆 Monthly", callback_data="lb:month"),
            InlineKeyboardButton("🕰 All-Time", callback_data="lb:all"),
        ],
        [
            InlineKeyboardButton("📊 My Stats", callback_data="nav:stats"),
            InlineKeyboardButton("🏅 Hall of Fame", callback_data="nav:hof"),
        ],
        [InlineKeyboardButton("ℹ️ Help", callback_data="nav:help")],
    ])

def leaderboard_keyboard(view: str) -> InlineKeyboardMarkup:
    switch = InlineKeyboardButton("🏆 View Monthly", callback_data="lb:month") if view == "all" \
        else InlineKeyboardButton("🕰 View All-Time", callback_data="lb:all")
    return InlineKeyboardMarkup([
        [switch],
        [
            InlineKeyboardButton("📊 My Stats", callback_data="nav:stats"),
            InlineKeyboardButton("🏅 Hall of Fame", callback_data="nav:hof"),
        ],
        [InlineKeyboardButton("🏠 Home", callback_data="nav:home")],
    ])

def secondary_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏆 Monthly", callback_data="lb:month"),
            InlineKeyboardButton("🕰 All-Time", callback_data="lb:all"),
        ],
        [InlineKeyboardButton("🏠 Home", callback_data="nav:home")],
    ])

def help_text() -> str:
    return (
        "📌 *Leaderboard Bot — Command Center*\n\n"
        "*Leaderboards*\n"
        "• `/leaderboard` — Monthly Top 10\n"
        "• `/leaderboard all` — All-time Top 10\n\n"
        "*Stats*\n"
        "• `/stats` — Your totals\n\n"
        "*History*\n"
        "• `/hof` — Hall of Fame (Top 3 each month)\n\n"
        "*Group Board*\n"
        "• `/board` — Create/refresh the permanent group leaderboard (admin)\n\n"
        "*Admin*\n"
        "• `/forceaward` — Award pending messages\n\n"
        "⏳ Messages count *24 hours after posting*."
    )

def rank_badge(i: int) -> str:
    if i == 1:
        return "🥇"
    if i == 2:
        return "🥈"
    if i == 3:
        return "🥉"
    keycaps = {4: "4️⃣", 5: "5️⃣", 6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣", 10: "🔟"}
    return keycaps.get(i, f"{i}.")


# ----------------------------
# Awarding + month rollover
# ----------------------------
def _ensure_user_row(chat_id: int, user_id: int, username: str):
    cursor.execute("SELECT 1 FROM users WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO users (
                chat_id, user_id, username,
                text_count, media_count, total_count,
                all_text_count, all_media_count, all_total_count
            )
            VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0)
        """, (chat_id, user_id, username))

def _award_count(chat_id: int, user_id: int, username: str, kind: str, n: int):
    _ensure_user_row(chat_id, user_id, username)
    if kind == "media":
        cursor.execute("""
            UPDATE users
            SET media_count = media_count + ?,
                total_count = total_count + ?,
                all_media_count = all_media_count + ?,
                all_total_count = all_total_count + ?,
                username = ?
            WHERE chat_id = ? AND user_id = ?
        """, (n, n, n, n, username, chat_id, user_id))
    else:
        cursor.execute("""
            UPDATE users
            SET text_count = text_count + ?,
                total_count = total_count + ?,
                all_text_count = all_text_count + ?,
                all_total_count = all_total_count + ?,
                username = ?
            WHERE chat_id = ? AND user_id = ?
        """, (n, n, n, n, username, chat_id, user_id))

def _current_month_key() -> str:
    return datetime.now(TZ).strftime("%Y-%m")

def _previous_month_key() -> str:
    now = datetime.now(TZ)
    prev_month_day = now.replace(day=1) - timedelta(days=1)
    return prev_month_day.strftime("%Y-%m")

async def ensure_month_is_current():
    current_month = _current_month_key()
    async with db_lock:
        last_reset_month = _get_meta_sync("last_reset_month")
        if last_reset_month is None:
            _set_meta_sync("last_reset_month", current_month)
            conn.commit()
            return
        if last_reset_month == current_month:
            return
    await monthly_reset_internal()

async def monthly_reset_internal():
    honor_month = _previous_month_key()
    new_month = _current_month_key()

    async with db_lock:
        cursor.execute("SELECT DISTINCT chat_id FROM users")
        chat_ids = [row[0] for row in cursor.fetchall()]

        for chat_id in chat_ids:
            cursor.execute("""
                SELECT user_id, username, text_count, media_count, total_count
                FROM users
                WHERE chat_id = ?
                ORDER BY total_count DESC
                LIMIT 3
            """, (chat_id,))
            top3 = cursor.fetchall()

            for idx, (user_id, username, text_c, media_c, total_c) in enumerate(top3, start=1):
                cursor.execute("""
                    INSERT OR REPLACE INTO hall_of_fame
                    (chat_id, month, rank, user_id, username, total_count, text_count, media_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (chat_id, honor_month, idx, user_id, username, total_c, text_c, media_c))

            cursor.execute("""
                UPDATE users
                SET text_count = 0, media_count = 0, total_count = 0
                WHERE chat_id = ?
            """, (chat_id,))

        _set_meta_sync("last_reset_month", new_month)
        conn.commit()


# ----------------------------
# Permanent group leaderboard (single message that updates)
# ----------------------------
def group_board_key(chat_id: int) -> str:
    return f"group_lb_msg_id:{chat_id}"

async def render_leaderboard(chat_id: int, show_all_time: bool) -> str:
    async with db_lock:
        if show_all_time:
            cursor.execute("""
                SELECT username, all_text_count, all_media_count, all_total_count
                FROM users
                WHERE chat_id = ?
                ORDER BY all_total_count DESC
                LIMIT 10
            """, (chat_id,))
            rows = cursor.fetchall()
        else:
            cursor.execute("""
                SELECT username, text_count, media_count, total_count,
                       all_text_count, all_media_count, all_total_count
                FROM users
                WHERE chat_id = ?
                ORDER BY total_count DESC
                LIMIT 10
            """, (chat_id,))
            rows = cursor.fetchall()

    if not rows:
        return "No data yet! (Messages count after 24h.)"

    if show_all_time:
        out = "🏆 Leaderboard — All-Time (Top 10)\n\n"
        for i, (username, t, m, total) in enumerate(rows, start=1):
            out += f"{rank_badge(i)} {username}\n   📝 {t}  •  🖼 {m}  •  📊 {total}\n\n"
        return out

    out = "🏆 Leaderboard — This Month (Top 10)\n\n"
    for i, (username, t, m, total, at, am, a_total) in enumerate(rows, start=1):
        out += (
            f"{rank_badge(i)} {username}\n"
            f"   📅 Month: 📝 {t}  •  🖼 {m}  •  📊 {total}\n"
            f"   🕰 All-time: 📝 {at}  •  🖼 {am}  •  📊 {a_total}\n\n"
        )
    return out

async def update_group_leaderboard(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """
    Creates one permanent leaderboard message (and pins it if allowed),
    then keeps editing that same message on updates.
    """
    await ensure_month_is_current()
    text = await render_leaderboard(chat_id, show_all_time=False)

    key = group_board_key(chat_id)
    async with db_lock:
        stored = _get_meta_sync(key)

    # Try edit existing
    if stored:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=int(stored), text=text)
            return
        except Exception as e:
            print("edit_group_leaderboard failed (will recreate):", repr(e))

    # Create new message
    try:
        msg = await context.bot.send_message(chat_id=chat_id, text=text)
    except Exception as e:
        print("send_group_leaderboard failed:", repr(e))
        return

    # Store message_id
    async with db_lock:
        _set_meta_sync(key, str(msg.message_id))
        conn.commit()

    # Try pin (optional)
    try:
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        pass


# ----------------------------
# Tracking messages (non-command only)
# ----------------------------
async def track_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.from_user:
        return

    msg = update.message

    # Ignore stickers entirely
    if msg.sticker:
        return

    chat_id = update.effective_chat.id
    user = msg.from_user
    username = user.username or user.first_name or "Unknown"

    is_media = bool(msg.photo or msg.video or msg.document or msg.animation or msg.voice)
    kind = "media" if is_media else "text"
    created_at_utc = int(time_mod.time())

    async with db_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO pending_messages
            (chat_id, message_id, user_id, username, kind, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (chat_id, msg.message_id, user.id, username, kind, created_at_utc))
        conn.commit()


# ----------------------------
# Jobs
# ----------------------------
async def award_matured_messages(context: ContextTypes.DEFAULT_TYPE):
    await ensure_month_is_current()

    now = int(time_mod.time())
    cutoff = now - 24 * 60 * 60

    async with db_lock:
        cursor.execute("""
            SELECT chat_id, user_id, MAX(username) as username, kind, COUNT(*)
            FROM pending_messages
            WHERE created_at_utc <= ?
            GROUP BY chat_id, user_id, kind
        """, (cutoff,))
        rows = cursor.fetchall()

        if not rows:
            return

        for chat_id, user_id, username, kind, n in rows:
            _award_count(chat_id, user_id, username, kind, n)

        cursor.execute("DELETE FROM pending_messages WHERE created_at_utc <= ?", (cutoff,))
        conn.commit()

    # ✅ Update the permanent group board (single-group)
    group_id = DEFAULT_GROUP_CHAT_ID
    if group_id:
        await update_group_leaderboard(context, group_id)

async def monthly_reset_job(context: ContextTypes.DEFAULT_TYPE):
    await monthly_reset_internal()
    group_id = DEFAULT_GROUP_CHAT_ID
    if group_id:
        await update_group_leaderboard(context, group_id)


# ----------------------------
# Commands (DM-only)
# ----------------------------
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await ensure_month_is_current()

    group_chat_id = await resolve_group_chat_id(update)
    if not group_chat_id:
        await send_dm_only(update, context, "❗ Set DEFAULT_GROUP_CHAT_ID in Railway variables.")
        return

    args = [a.lower() for a in (context.args or [])]
    show_all_time = len(args) >= 1 and args[0] in ("all", "alltime", "lifetime")
    view = "all" if show_all_time else "month"

    text = await render_leaderboard(group_chat_id, show_all_time)
    await send_dm_only(update, context, text, reply_markup=leaderboard_keyboard(view))

async def render_stats(chat_id: int, user_id: int) -> str:
    async with db_lock:
        cursor.execute("""
            SELECT text_count, media_count, total_count,
                   all_text_count, all_media_count, all_total_count
            FROM users
            WHERE chat_id = ? AND user_id = ?
        """, (chat_id, user_id))
        row = cursor.fetchone()

    if not row:
        return "You have no stats yet! (Messages count after 24h.)"

    t, m, total, at, am, a_total = row
    return (
        "📊 Your Stats\n\n"
        "📅 This Month\n"
        f"• 📝 Text: {t}\n"
        f"• 🖼 Media: {m}\n"
        f"• 📊 Total: {total}\n\n"
        "🕰 All-Time\n"
        f"• 📝 Text: {at}\n"
        f"• 🖼 Media: {am}\n"
        f"• 📊 Total: {a_total}\n\n"
        "⏳ Messages are awarded after 24 hours."
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await ensure_month_is_current()

    group_chat_id = await resolve_group_chat_id(update)
    if not group_chat_id:
        await send_dm_only(update, context, "❗ Set DEFAULT_GROUP_CHAT_ID in Railway variables.")
        return

    text = await render_stats(group_chat_id, update.effective_user.id)
    await send_dm_only(update, context, text, reply_markup=secondary_keyboard())

async def render_hof(chat_id: int) -> str:
    async with db_lock:
        cursor.execute("""
            SELECT month, rank, username, total_count
            FROM hall_of_fame
            WHERE chat_id = ?
            ORDER BY month DESC, rank ASC
            LIMIT 36
        """, (chat_id,))
        rows = cursor.fetchall()

    if not rows:
        return "🏅 No Hall of Fame data yet (first snapshot happens on the 1st)."

    out = "🏅 Hall of Fame — Top 3 per Month\n\n"
    current = None
    for month, rank, username, total in rows:
        if month != current:
            current = month
            out += f"📅 {month}\n"
        out += f"  {rank_badge(rank)} {username} — {total}\n"
        if rank == 3:
            out += "\n"
    return out

async def hof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    group_chat_id = await resolve_group_chat_id(update)
    if not group_chat_id:
        await send_dm_only(update, context, "❗ Set DEFAULT_GROUP_CHAT_ID in Railway variables.")
        return

    text = await render_hof(group_chat_id)
    await send_dm_only(update, context, text, reply_markup=secondary_keyboard())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await send_dm_only(update, context, help_text(), parse_mode="Markdown", reply_markup=home_keyboard())


# ----------------------------
# Group-only admin commands
# ----------------------------
async def forceaward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    chat = update.effective_chat
    user = update.effective_user

    member = await chat.get_member(user.id)
    if member.status not in ("administrator", "creator"):
        try:
            msg = await context.bot.send_message(chat.id, "❌ Admins only.")
            schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    await ensure_month_is_current()

    async with db_lock:
        cursor.execute("""
            SELECT user_id, MAX(username) as username, kind, COUNT(*)
            FROM pending_messages
            WHERE chat_id = ?
            GROUP BY user_id, kind
        """, (chat.id,))
        rows = cursor.fetchall()

        if not rows:
            pass
        else:
            for user_id, username, kind, n in rows:
                _award_count(chat.id, user_id, username, kind, n)

            cursor.execute("DELETE FROM pending_messages WHERE chat_id = ?", (chat.id,))
            conn.commit()

    # Update group board for this chat (and for DEFAULT_GROUP_CHAT_ID)
    await update_group_leaderboard(context, chat.id)

    try:
        msg = await context.bot.send_message(chat.id, "✅ Awarded pending messages and refreshed the board.")
        schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
    except Exception:
        pass

async def board(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to create/refresh the permanent leaderboard in the group."""
    await safe_delete_message(update.message)

    chat = update.effective_chat
    user = update.effective_user

    if not chat or chat.type not in ("group", "supergroup"):
        await send_dm_only(update, context, "Use /board inside the group.")
        return

    member = await chat.get_member(user.id)
    if member.status not in ("administrator", "creator"):
        try:
            msg = await context.bot.send_message(chat.id, "❌ Admins only.")
            schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    await update_group_leaderboard(context, chat.id)

    try:
        msg = await context.bot.send_message(chat.id, "📌 Leaderboard board created/updated.")
        schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
    except Exception:
        pass


# ----------------------------
# Buttons (DM)
# ----------------------------
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.message:
        return

    await query.answer()
    await ensure_month_is_current()

    # Buttons are meant for DM
    if query.message.chat.type != "private":
        try:
            msg = await context.bot.send_message(chat_id=query.message.chat_id, text="📩 Please use the DM I sent you.")
            schedule_delete(context, query.message.chat_id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    # Single-group
    group_chat_id = DEFAULT_GROUP_CHAT_ID
    if not group_chat_id:
        # fallback: last group user used
        group_chat_id = await get_user_last_chat(query.from_user.id)

    if not group_chat_id:
        await query.edit_message_text(
            "❗ Set DEFAULT_GROUP_CHAT_ID in Railway variables (recommended).",
            reply_markup=home_keyboard(),
        )
        return

    data = query.data or ""

    if data in ("nav:home", "nav:help"):
        await query.edit_message_text(help_text(), reply_markup=home_keyboard(), parse_mode="Markdown")
        return

    if data == "lb:month":
        text = await render_leaderboard(group_chat_id, show_all_time=False)
        await query.edit_message_text(text, reply_markup=leaderboard_keyboard("month"))
        return

    if data == "lb:all":
        text = await render_leaderboard(group_chat_id, show_all_time=True)
        await query.edit_message_text(text, reply_markup=leaderboard_keyboard("all"))
        return

    if data == "nav:stats":
        text = await render_stats(group_chat_id, query.from_user.id)
        await query.edit_message_text(text, reply_markup=secondary_keyboard())
        return

    if data == "nav:hof":
        text = await render_hof(group_chat_id)
        await query.edit_message_text(text, reply_markup=secondary_keyboard())
        return


# ----------------------------
# App lifecycle
# ----------------------------
async def post_init(app):
    await ensure_month_is_current()
    try:
        await app.bot.set_my_commands([
            BotCommand("leaderboard", "DM: monthly leaderboard (add 'all' for all-time)"),
            BotCommand("stats", "DM: your stats"),
            BotCommand("hof", "DM: hall of fame"),
            BotCommand("help", "DM: show commands"),
            BotCommand("board", "Admin (group): create/refresh the permanent leaderboard board"),
            BotCommand("forceaward", "Admin (group): award pending messages"),
        ])
    except Exception:
        pass

    # If you set DEFAULT_GROUP_CHAT_ID, create/update the permanent board on startup
    if DEFAULT_GROUP_CHAT_ID:
        try:
            await update_group_leaderboard(app.bot, DEFAULT_GROUP_CHAT_ID)  # (won't work: needs ContextTypes)
        except Exception:
            # We'll do it via a short one-off job below in main()
            pass


# ----------------------------
# Main
# ----------------------------
def main():
    if not TOKEN:
        raise RuntimeError("TOKEN env var is missing. Set TOKEN in Railway Variables.")

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )

    # Track ONLY non-command messages (so commands are not counted)
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, track_messages), group=1)

    # DM-only commands
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("hof", hof))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("commands", help_command))

    # Group admin commands
    app.add_handler(CommandHandler("board", board))
    app.add_handler(CommandHandler("forceaward", forceaward))

    # Buttons (DM)
    app.add_handler(CallbackQueryHandler(on_button))

    # Jobs
    app.job_queue.run_repeating(award_matured_messages, interval=60 * 60, first=30)

    app.job_queue.run_monthly(
        monthly_reset_job,
        when=time(hour=0, minute=5, tzinfo=TZ),
        day=1,
    )

    # Create/refresh group board shortly after startup (Context is available here)
    if DEFAULT_GROUP_CHAT_ID:
        async def _startup_board(ctx: ContextTypes.DEFAULT_TYPE):
            await update_group_leaderboard(ctx, DEFAULT_GROUP_CHAT_ID)

        app.job_queue.run_once(_startup_board, when=5)

    app.run_polling()


if __name__ == "__main__":
    main()
