import os
import sqlite3
import time as time_mod
import asyncio
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    CommandHandler,
    filters,
)

TOKEN = os.getenv("TOKEN")
TZ = ZoneInfo("America/Montreal")
AUTO_DELETE_SECONDS = 30

DB_PATH = "leaderboard.db"

# --- Database Setup ---
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

# Per-chat per-user monthly counters
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,
    text_count INTEGER DEFAULT 0,
    media_count INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,
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


# --- Helpers (DB) ---
def _ensure_user_row(chat_id: int, user_id: int, username: str):
    cursor.execute(
        "SELECT 1 FROM users WHERE chat_id = ? AND user_id = ?",
        (chat_id, user_id)
    )
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO users (chat_id, user_id, username, text_count, media_count, total_count)
            VALUES (?, ?, ?, 0, 0, 0)
        """, (chat_id, user_id, username))


def _award_count(chat_id: int, user_id: int, username: str, kind: str, n: int):
    _ensure_user_row(chat_id, user_id, username)

    if kind == "media":
        cursor.execute("""
            UPDATE users
            SET media_count = media_count + ?,
                total_count = total_count + ?,
                username = ?
            WHERE chat_id = ? AND user_id = ?
        """, (n, n, username, chat_id, user_id))
    else:
        cursor.execute("""
            UPDATE users
            SET text_count = text_count + ?,
                total_count = total_count + ?,
                username = ?
            WHERE chat_id = ? AND user_id = ?
        """, (n, n, username, chat_id, user_id))


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

    # Month rolled over; perform reset once (outside lock -> call the reset which locks)
    # Using the normal monthly_reset() ensures snapshot month is correct.
    # We pass a dummy context via None by using the internal DB work below.
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

            # Reset counters for the new month
            cursor.execute("""
                UPDATE users
                SET text_count = 0, media_count = 0, total_count = 0
                WHERE chat_id = ?
            """, (chat_id,))

        # Record that we are now in the new month
        _set_meta("last_reset_month", new_month)

        conn.commit()


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
    # Ensure month rollover is handled even if scheduled reset was missed
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

    async with db_lock:
        cursor.execute("""
            SELECT username, text_count, media_count, total_count
            FROM users
            WHERE chat_id = ?
            ORDER BY total_count DESC
            LIMIT 10
        """, (chat_id,))
        rows = cursor.fetchall()

    if not rows:
        msg = await context.bot.send_message(chat_id, "No data yet! (Messages count after 24h.)")
        schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)
        return

    message = "🏆 Leaderboard (This month — Top 10)\n\n"
    for i, (username, text_c, media_c, total_c) in enumerate(rows, start=1):
        message += f"{i}. {username}\n   📝 {text_c} | 🖼 {media_c} | 📊 {total_c}\n\n"

    msg = await context.bot.send_message(chat_id, message)
    schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await ensure_month_is_current()

    chat_id = update.effective_chat.id
    user = update.effective_user

    async with db_lock:
        cursor.execute("""
            SELECT text_count, media_count, total_count
            FROM users
            WHERE chat_id = ? AND user_id = ?
        """, (chat_id, user.id))
        row = cursor.fetchone()

    if not row:
        msg = await context.bot.send_message(chat_id, "You have no stats yet! (Messages count after 24h.)")
        schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)
        return

    text_c, media_c, total_c = row
    msg = await context.bot.send_message(
        chat_id,
        f"📊 Your Stats (This month)\n\n"
        f"📝 Text: {text_c}\n"
        f"🖼 Media: {media_c}\n"
        f"📊 Total: {total_c}\n\n"
        f"⏳ Note: messages are awarded after 24 hours."
    )
    schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)


async def hof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    chat_id = update.effective_chat.id

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
        msg = await context.bot.send_message(chat_id, "🏅 No Hall of Fame data yet (first snapshot happens on the 1st).")
        schedule_delete(context, chat_id, msg.message_id, AUTO_DELETE_SECONDS)
        return

    out = "🏅 Hall of Fame (Top 3 each month)\n\n"
    current_month = None
    for month, rank, username, total in rows:
        if month != current_month:
            current_month = month
            out += f"📅 {month}\n"
        out += f"  {rank}. {username} — {total}\n"
        if rank == 3:
            out += "\n"

    msg = await context.bot.send_message(chat_id, out)
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
            # outside lock for send is fine, but keep it simple:
            pass

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


# --- Monthly Reset Job (scheduled) ---
async def monthly_reset(context: ContextTypes.DEFAULT_TYPE):
    """
    Scheduled: 1st at 00:05 Montreal time.
    Snapshot previous month, reset counters, update meta.
    """
    await monthly_reset_internal()


# --- App lifecycle ---
async def post_init(app):
    # Catch up if the bot starts after a month boundary
    await ensure_month_is_current()


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