import os
import sqlite3
import time as time_mod
from datetime import datetime, time
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

# --- Database Setup ---
conn = sqlite3.connect("leaderboard.db", check_same_thread=False)
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
    month TEXT NOT NULL,       -- e.g. "2026-02"
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

conn.commit()


# --- Helpers ---
async def safe_delete_message(message):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


async def safe_delete_message_later(message, delay_seconds: int):
    """Delete message after delay. Uses JobQueue so it works reliably."""
    if not message:
        return

    async def _delete_cb(ctx: ContextTypes.DEFAULT_TYPE):
        try:
            await ctx.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
        except Exception:
            pass

    try:
        context_job_queue = message.get_bot()._application.job_queue  # fallback path (rare)
        # Prefer scheduling on the application's job queue via context in handlers below
    except Exception:
        context_job_queue = None

    # We will schedule deletion from handlers using context.job_queue (preferred).
    # This function exists for compatibility; we won't call it directly without context.


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
        # If JobQueue is unavailable or scheduling fails, just ignore.
        pass


def ensure_user_row(chat_id: int, user_id: int, username: str):
    cursor.execute(
        "SELECT 1 FROM users WHERE chat_id = ? AND user_id = ?",
        (chat_id, user_id)
    )
    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO users (chat_id, user_id, username, text_count, media_count, total_count)
            VALUES (?, ?, ?, 0, 0, 0)
        """, (chat_id, user_id, username))


def award_count(chat_id: int, user_id: int, username: str, kind: str, n: int):
    ensure_user_row(chat_id, user_id, username)

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


# --- Message Handler ---
async def track_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.from_user:
        return

    msg = update.message

    # ✅ Ignore stickers entirely
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

    # Store as pending and award after 24 hours
    cursor.execute("""
        INSERT OR IGNORE INTO pending_messages
        (chat_id, message_id, user_id, username, kind, created_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (chat_id, msg.message_id, user.id, username, kind, created_at_utc))
    conn.commit()


# --- Job: award matured pending messages (>= 24h old) ---
async def award_matured_messages(context: ContextTypes.DEFAULT_TYPE):
    now = int(time_mod.time())
    cutoff = now - 24 * 60 * 60  # 24 hours

    cursor.execute("""
        SELECT chat_id, user_id, username, kind, COUNT(*)
        FROM pending_messages
        WHERE created_at_utc <= ?
        GROUP BY chat_id, user_id, username, kind
    """, (cutoff,))
    rows = cursor.fetchall()

    if not rows:
        return

    for chat_id, user_id, username, kind, n in rows:
        award_count(chat_id, user_id, username, kind, n)

    cursor.execute("DELETE FROM pending_messages WHERE created_at_utc <= ?", (cutoff,))
    conn.commit()


# --- Commands ---
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Delete the command message
    await safe_delete_message(update.message)

    chat_id = update.effective_chat.id

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

    chat_id = update.effective_chat.id
    user = update.effective_user

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

    cursor.execute("""
        SELECT chat_id, user_id, username, kind, COUNT(*)
        FROM pending_messages
        WHERE chat_id = ?
        GROUP BY chat_id, user_id, username, kind
    """, (chat.id,))
    rows = cursor.fetchall()

    if not rows:
        msg = await context.bot.send_message(chat.id, "No pending messages to award.")
        schedule_delete(context, chat.id, msg.message_id, AUTO_DELETE_SECONDS)
        return

    total_awarded = 0
    for chat_id, user_id, username, kind, n in rows:
        award_count(chat_id, user_id, username, kind, n)
        total_awarded += n

    cursor.execute("DELETE FROM pending_messages WHERE chat_id = ?", (chat.id,))
    conn.commit()

    msg = await context.bot.send_message(chat.id, f"✅ Awarded {total_awarded} pending messages.")
    schedule_delete(context, chat.id, msg.message_id, AUTO_DELETE_SECONDS)


# --- Monthly Reset Job ---
async def monthly_reset(context: ContextTypes.DEFAULT_TYPE):
    """On the 1st, snapshot Top 3 for each chat, then reset monthly counters."""
    now = datetime.now(TZ)
    month_key = now.strftime("%Y-%m")

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
            """, (chat_id, month_key, idx, user_id, username, total_c, text_c, media_c))

        cursor.execute("""
            UPDATE users
            SET text_count = 0, media_count = 0, total_count = 0
            WHERE chat_id = ?
        """, (chat_id,))

    conn.commit()


# --- Main ---
def main():
    app = ApplicationBuilder().token(TOKEN).build()

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
