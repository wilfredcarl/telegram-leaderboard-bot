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

TOKEN = os.getenv("TOKEN")
TZ = ZoneInfo("America/Montreal")
AUTO_DELETE_SECONDS = 30

DB_PATH = "leaderboard.db"

# --- Database Setup ---
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

# Per-chat per-user monthly + all-time counters
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

# Hall of fame snapshots (Top 3 per month per chat)
cursor.execute("""
CREATE TABLE IF NOT EXISTS hall_of_fame (
    chat_id INTEGER NOT NULL,
    month TEXT NOT NULL,       -- e.g. "2026-02" (the month being honored / the month that just ended)
    rank INTEGER NOT NULL,     -- 1,2,3
    user_id INTEGER NOT NULL,
    username TEXT,
    total_count INTEGER NOT NULL,
    text_count INTEGER NOT NULL,
    media_count INTEGER NOT NULL,
    PRIMARY KEY (chat_id, month, rank)
)
""")

# Pending messages to be awarded after 24h
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

# Meta table for resiliency (handles downtime across month boundary)
cursor.execute("""
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")

conn.commit()

# Global DB lock (PTB runs handlers/jobs concurrently)
db_lock = asyncio.Lock()


def ensure_all_time_columns():
    """
    For existing databases: add all-time columns if missing.
    Safe to call on every startup.
    """
    cursor.execute("PRAGMA table_info(users)")
    columns = [row[1] for row in cursor.fetchall()]

    needed = {
        "all_text_count": "ALTER TABLE users ADD COLUMN all_text_count INTEGER DEFAULT 0",
        "all_media_count": "ALTER TABLE users ADD COLUMN all_media_count INTEGER DEFAULT 0",
        "all_total_count": "ALTER TABLE users ADD COLUMN all_total_count INTEGER DEFAULT 0",
    }

    for col, stmt in needed.items():
        if col not in columns:
            cursor.execute(stmt)

    conn.commit()


# Ensure schema is up-to-date (important if DB already existed)
ensure_all_time_columns()


# --- Helpers (Telegram) ---
async def safe_delete_message(message):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


def schedule_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay_seconds: int):
    """Schedule deletion using PTB JobQueue."""
    async def _delete_cb(ctx: ContextTypes.DEFAULT_TYPE):
        try:
            await ctx.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass

    try:
        context.job_queue.run_once(_delete_cb, when=delay_seconds)
    except Exception:
        pass


# --- Inline Keyboards ---
def leaderboard_keyboard(view: str) -> InlineKeyboardMarkup:
    # view: "month" or "all"
    if view == "all":
        buttons = [
            [InlineKeyboardButton("📅 This Month", callback_data="lb:month")],
            [InlineKeyboardButton("📊 My Stats", callback_data="nav:stats")],
            [InlineKeyboardButton("📖 Help", callback_data="nav:help")],
        ]
    else:
        buttons = [
            [InlineKeyboardButton("🕰 All-Time", callback_data="lb:all")],
            [InlineKeyboardButton("📊 My Stats", callback_data="nav:stats")],
            [InlineKeyboardButton("📖 Help", callback_data="nav:help")],
        ]
    return InlineKeyboardMarkup(buttons)


def help_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏆 Leaderboard (Month)", callback_data="lb:month")],
        [InlineKeyboardButton("🕰 Leaderboard (All-Time)", callback_data="lb:all")],
        [InlineKeyboardButton("📊 My Stats", callback_data="nav:stats")],
        [InlineKeyboardButton("🏅 Hall of Fame", callback_data="nav:hof")],
    ])


# --- Helpers (DB) ---
def _ensure_user_row(chat_id: int, user_id: int, username: str):
    cursor.execute(
        "SELECT 1 FROM users WHERE chat_id = ? AND user_id = ?",
        (chat_id, user_id)
    )
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


def _get_meta(key: str) -> str | None:
    cursor.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else None


def _set_meta(key: str, value: str):
    cursor.execute("""
        INSERT INTO meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (key, value))


def _current_month_key() -> str:
    return datetime.now(TZ).strftime("%Y-%m")


def _previous_month_key() -> str:
    now = datetime.now(TZ)
    prev_month_day = now.replace(day=1) - timedelta(days=1)
    return prev_month_day.strftime("%Y-%m")


async def ensure_month_is_current():
    """
    If the bot was down during the 1st 00:05 schedule, we still want:
    - snapshot top 3 for the month that ended
    - reset monthly counters
    This runs on startup + periodically (e.g., hourly award job).
    """
    current_month = _current_month_key()

    async with db_lock:
        last_reset_month = _get_meta("last_reset_month")

        # First run: initialize meta to current month (no retro snapshot)
        if last_reset_month is None:
            _set_meta("last_reset_month", current_month)
            conn.commit()
            return

        if last_reset_month == current_month:
            return

    # Month rolled over; perform reset once
    await monthly_reset_internal()


async def monthly_reset_internal():
    """DB-only monthly reset (safe to call from ensure_month_is_current)."""
    honor_month = _previous_month_key()
    new_month = _current_month_key()

    async with db_lock:
        # Snapshot top 3 per chat
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

            # Reset monthly counters only (all-time stays)
            cursor.execute("""
                UPDATE users
                SET text_count = 0, media_count = 0, total_count = 0
                WHERE chat_id = ?
            """, (chat_id,))

        # Record that we are now in the new month
        _set_meta("last_reset_month", new_month)

        conn.commit()


# --- Rendering helpers (reuse for buttons + commands) ---
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
                SELECT username,
                       text_count, media_count, total_count,
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
        message = "🏆 Leaderboard (All-Time — Top 10)\n\n"
        for i, (username, all_text_c, all_media_c, all_total_c) in enumerate(rows, start=1):
            message += f"{i}. {username}\n   📝 {all_text_c} | 🖼 {all_media_c} | 📊 {all_total_c}\n\n"
        return message

    message = "🏆 Leaderboard (This month — Top 10)\n\n"
    for i, (username, text_c, media_c, total_c, all_text_c, all_media_c, all_total_c) in enumerate(rows, start=1):
        message += (
            f"{i}. {username}\n"
            f"   📅 Month → 📝 {text_c} | 🖼 {media_c} | 📊 {total_c}\n"
            f"   🕰 All-Time → 📝 {all_text_c} | 🖼 {all_media_c} | 📊 {all_total_c}\n\n"
        )
    return message


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

    text_c, media_c, total_c, all_text_c, all_media_c, all_total_c = row
    return (
        "📊 Your Stats\n\n"
        "📅 This Month:\n"
        f"📝 Text: {text_c}\n"
        f"🖼 Media: {media_c}\n"
        f"📊 Total: {total_c}\n\n"
        "🕰 All-Time:\n"
        f"📝 Text: {all_text_c}\n"
        f"🖼 Media: {all_media_c}\n"
        f"📊 Total: {all_total_c}\n\n"
        "⏳ Note: messages are awarded after 24 hours."
    )


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

    out = "🏅 Hall of Fame (Top 3 each month)\n\n"
    current_month = None
    for month, rank, username, total in rows:
        if month != current_month:
            current_month = month
            out += f"📅 {month}\n"
        out += f"  {rank}. {username} — {total}\n"
        if rank == 3:
            out += "\n"
    return out


def help_text() -> str:
    return (
        "🤖 Available Commands\n\n"
        "🏆 /leaderboard\n"
        "   Show monthly leaderboard + all-time stats\n\n"
        "🏆 /leaderboard all\n"
        "   Show all-time leaderboard\n\n"
        "📊 /stats\n"
        "   View your monthly + all-time stats\n\n"
        "🏅 /hof\n"
        "   View Hall of Fame (Top 3 each month)\n\n"
        "🛠 /forceaward\n"
        "   (Admins only) Force award pending messages\n\n"
        "📖 /help\n"
        "   Show this command list\n"
    )


# --- Message Handler ---
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

    is_media = bool(
        msg.photo
        or msg.video
        or msg.document
        or msg.animation
        or msg.voice
    )

    kind = "media" if is_media else "text"
    created_at_utc = int(time_mod.time())

    async with db_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO pending_messages
            (chat_id, message_id, user_id, username, kind, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (chat_id, msg.message_id, user.id, username, kind, created_at_utc))
        conn.commit()


# --- Job: award matured pending messages (>= 24h old) ---
async def award_matured_messages(context: ContextTypes.DEFAULT_TYPE):
    await ensure_month_is_current()

    now = int(time_mod.time())
    cutoff = now - 24 * 60 * 60  # 24 hours

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


# --- Commands ---
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await ensure_month_is_current()

    chat_id = update.effective_chat.id

    args = [a.lower() for a in (context.args or [])]
    show_all_time = len(args) >= 1 and args[0] in ("all", "alltime", "lifetime")
    view = "all" if show_all_time else "month"

    text = await render_leaderboard(chat_id, show_all_time)
    msg = await context.bot.send_message(chat_id, text, reply_markup=leaderboard_keyboard(view))
    schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await ensure_month_is_current()

    chat_id = update.effective_chat.id
    user = update.effective_user

    text = await render_stats(chat_id, user.id)
    msg = await context.bot.send_message(chat_id, text, reply_markup=help_keyboard())
    schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)


async def hof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    chat_id = update.effective_chat.id
    text = await render_hof(chat_id)

    msg = await context.bot.send_message(chat_id, text, reply_markup=help_keyboard())
    schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)


async def forceaward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    chat = update.effective_chat
    user = update.effective_user

    member = await chat.get_member(user.id)
    if member.status not in ("administrator", "creator"):
        msg = await context.bot.send_message(chat.id, "❌ Admins only.")
        schedule_delete(context, chat.id, msg.message_id, AUTO_DELETE_SECONDS)
        return

    await ensure_month_is_current()

    async with db_lock:
        cursor.execute("""
            SELECT chat_id, user_id, MAX(username) as username, kind, COUNT(*)
            FROM pending_messages
            WHERE chat_id = ?
            GROUP BY chat_id, user_id, kind
        """, (chat.id,))
        rows = cursor.fetchall()

    if not rows:
        msg = await context.bot.send_message(chat.id, "No pending messages to award.")
        schedule_delete(context, chat.id, msg.message_id, AUTO_DELETE_SECONDS)
        return

    total_awarded = 0
    async with db_lock:
        for chat_id, user_id, username, kind, n in rows:
            _award_count(chat_id, user_id, username, kind, n)
            total_awarded += n

        cursor.execute("DELETE FROM pending_messages WHERE chat_id = ?", (chat.id,))
        conn.commit()

    msg = await context.bot.send_message(chat.id, f"✅ Awarded {total_awarded} pending messages.")
    schedule_delete(context, chat.id, msg.message_id, AUTO_DELETE_SECONDS)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    chat_id = update.effective_chat.id
    msg = await context.bot.send_message(chat_id, help_text(), reply_markup=help_keyboard())
    schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)


# --- Button Callback Handler ---
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.message:
        return

    await query.answer()
    await ensure_month_is_current()

    chat_id = query.message.chat_id
    data = query.data or ""

    try:
        if data == "lb:month":
            text = await render_leaderboard(chat_id, show_all_time=False)
            await query.edit_message_text(text, reply_markup=leaderboard_keyboard("month"))
            schedule_delete(context, chat_id, query.message.message_id, AUTO_DELETE_SECONDS)
            return

        if data == "lb:all":
            text = await render_leaderboard(chat_id, show_all_time=True)
            await query.edit_message_text(text, reply_markup=leaderboard_keyboard("all"))
            schedule_delete(context, chat_id, query.message.message_id, AUTO_DELETE_SECONDS)
            return

        if data == "nav:stats":
            user_id = query.from_user.id
            text = await render_stats(chat_id, user_id)
            await query.edit_message_text(text, reply_markup=help_keyboard())
            schedule_delete(context, chat_id, query.message.message_id, AUTO_DELETE_SECONDS)
            return

        if data == "nav:hof":
            text = await render_hof(chat_id)
            await query.edit_message_text(text, reply_markup=help_keyboard())
            schedule_delete(context, chat_id, query.message.message_id, AUTO_DELETE_SECONDS)
            return

        if data == "nav:help":
            await query.edit_message_text(help_text(), reply_markup=help_keyboard())
            schedule_delete(context, chat_id, query.message.message_id, AUTO_DELETE_SECONDS)
            return
    except Exception:
        # If edit fails (message too old, not modified, etc.), ignore.
        pass


# --- Monthly Reset Job (scheduled) ---
async def monthly_reset(context: ContextTypes.DEFAULT_TYPE):
    """
    Scheduled: 1st at 00:05 Montreal time.
    Snapshot previous month, reset counters, update meta.
    """
    await monthly_reset_internal()


# --- App lifecycle ---
async def post_init(app):
    await ensure_month_is_current()

    # Optional: set the Telegram command menu (shows in the UI)
    try:
        await app.bot.set_my_commands([
            BotCommand("leaderboard", "Show monthly leaderboard (add 'all' for all-time)"),
            BotCommand("stats", "View your stats"),
            BotCommand("hof", "Hall of Fame"),
            BotCommand("forceaward", "Admin: award pending messages"),
            BotCommand("help", "Show all commands"),
        ])
    except Exception:
        pass


# --- Main ---
def main():
    if not TOKEN:
        raise RuntimeError("TOKEN env var is missing. Set TOKEN in Railway Variables (or your env).")

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, track_messages))

    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("hof", hof))
    app.add_handler(CommandHandler("forceaward", forceaward))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("commands", help_command))  # alias

    # NEW: button callbacks
    app.add_handler(CallbackQueryHandler(on_button))

    # Award matured messages every hour (counts messages once they are 24h old)
    app.job_queue.run_repeating(award_matured_messages, interval=60 * 60, first=30)

    # Monthly reset: 1st at 00:05 Montreal time
    app.job_queue.run_monthly(
        monthly_reset,
        when=time(hour=0, minute=5, tzinfo=TZ),
        day=1,
    )

    app.run_polling()


if __name__ == "__main__":
    main()
