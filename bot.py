import asyncio
from datetime import datetime, time, timedelta

db_lock = asyncio.Lock()

async def track_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.from_user:
        return

    msg = update.message

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


async def award_matured_messages(context: ContextTypes.DEFAULT_TYPE):
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
            award_count(chat_id, user_id, username, kind, n)

        cursor.execute("DELETE FROM pending_messages WHERE created_at_utc <= ?", (cutoff,))
        conn.commit()


async def monthly_reset(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(TZ)
    prev_month_day = now.replace(day=1) - timedelta(days=1)
    month_key = prev_month_day.strftime("%Y-%m")

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
                """, (chat_id, month_key, idx, user_id, username, total_c, text_c, media_c))

            cursor.execute("""
                UPDATE users
                SET text_count = 0, media_count = 0, total_count = 0
                WHERE chat_id = ?
            """, (chat_id,))

        conn.commit()