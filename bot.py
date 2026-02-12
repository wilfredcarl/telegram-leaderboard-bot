import os
import sqlite3
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

# ✅ Recommended: set TOKEN as an environment variable in Railway
# Railway: Variables -> add TOKEN = your_token
TOKEN = os.getenv("TOKEN", "PASTE_YOUR_TOKEN_HERE")

TZ = ZoneInfo("America/Montreal")

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

conn.commit()


# --- Helper Functions ---
def update_user(chat_id: int, user, is_media: bool):
    username = user.username or user.first_name or "Unknown"

    cursor.execute(
        "SELECT 1 FROM users WHERE chat_id = ? AND user_id = ?",
        (chat_id, user.id)
    )
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("""
            INSERT INTO users (chat_id, user_id, username, text_count, media_count, total_count)
            VALUES (?, ?, ?, 0, 0, 0)
        """, (chat_id, user.id, username))
        conn.commit()

    if is_media:
        cursor.execute("""
            UPDATE users
            SET media_count = media_count + 1,
                total_count = total_count + 1,
                username = ?
            WHERE chat_id = ? AND user_id = ?
        """, (username, chat_id, user.id))
    else:
        cursor.execute("""
            UPDATE users
            SET text_count = text_count + 1,
                total_count = total_count + 1,
                username = ?
            WHERE chat_id = ? AND user_id = ?
        """, (username, chat_id, user.id))

    conn.commit()


# --- Message Handler ---
async def track_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.from_user:
        return

    chat_id = update.effective_chat.id
    user = update.message.from_user

    is_media = bool(
        update.message.photo
        or update.message.video
        or update.message.document
        or update.message.animation
        or update.message.voice
        or update.message.sticker
    )

    update_user(chat_id, user, is_media)


# --- Commands ---
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text("No data yet!")
        return

    message = "🏆 Leaderboard (This month — Top 10)\n\n"
    for i, (username, text_c, media_c, total_c) in enumerate(rows, start=1):
        message += (
            f"{i}. {username}\n"
            f"   📝 {text_c} | 🖼 {media_c} | 📊 {total_c}\n\n"
        )

    await update.message.reply_text(message)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.message.from_user

    cursor.execute("""
        SELECT text_count, media_count, total_count
        FROM users
        WHERE chat_id = ? AND user_id = ?
    """, (chat_id, user.id))
    row = cursor.fetchone()

    if not row:
        await update.message.reply_text("You have no stats yet!")
        return

    text_c, media_c, total_c = row
    await update.message.reply_text(
        f"📊 Your Stats (This month)\n\n"
        f"📝 Text: {text_c}\n"
        f"🖼 Media: {media_c}\n"
        f"📊 Total: {total_c}"
    )


async def hof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows saved Top 3 for recent months for this chat."""
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
        await update.message.reply_text("🏅 No Hall of Fame data yet (first snapshot happens on the 1st).")
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

    await update.message.reply_text(out)


# --- Monthly Reset Job ---
async def monthly_reset(context: ContextTypes.DEFAULT_TYPE):
    """On the 1st, snapshot Top 3 for each chat, then reset monthly counters."""
    now = datetime.now(TZ)
    month_key = now.strftime("%Y-%m")

    # Find all chats we have records for
    cursor.execute("SELECT DISTINCT chat_id FROM users")
    chat_ids = [row[0] for row in cursor.fetchall()]

    for chat_id in chat_ids:
        # Top 3 for this chat
        cursor.execute("""
            SELECT user_id, username, text_count, media_count, total_count
            FROM users
            WHERE chat_id = ?
            ORDER BY total_count DESC
            LIMIT 3
        """, (chat_id,))
        top3 = cursor.fetchall()

        # Save snapshot
        for idx, (user_id, username, text_c, media_c, total_c) in enumerate(top3, start=1):
            cursor.execute("""
                INSERT OR REPLACE INTO hall_of_fame
                (chat_id, month, rank, user_id, username, total_count, text_count, media_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (chat_id, month_key, idx, user_id, username, total_c, text_c, media_c))

        # Reset counters
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

    # Run on the 1st at 00:05 Montreal time
    app.job_queue.run_monthly(
        monthly_reset,
        when=time(hour=0, minute=5, tzinfo=TZ),
        day=1,
    )

    app.run_polling()


if __name__ == "__main__":
    main()
