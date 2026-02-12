import sqlite3

from telegram import Update

from telegram.ext import (

    ApplicationBuilder,

    ContextTypes,

    MessageHandler,

    CommandHandler,

    filters,

)

  

TOKEN = "8590522532:AAES85F2lQfkJgO243HYRV56vEiNQMC0X8w”

  

# Database setup

conn = sqlite3.connect("leaderboard.db", check_same_thread=False)

cursor = conn.cursor()

  

cursor.execute("""

CREATE TABLE IF NOT EXISTS users (

    user_id INTEGER PRIMARY KEY,

    username TEXT,

    text_count INTEGER DEFAULT 0,

    media_count INTEGER DEFAULT 0,

    total_count INTEGER DEFAULT 0

)

""")

conn.commit()

  

  

def update_user(user, is_media):

    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user.id,))

    result = cursor.fetchone()

  

    if result is None:

        cursor.execute("""

        INSERT INTO users (user_id, username)

        VALUES (?, ?)

        """, (user.id, user.username or user.first_name))

        conn.commit()

  

    if is_media:

        cursor.execute("""

        UPDATE users

        SET media_count = media_count + 1,

            total_count = total_count + 1

        WHERE user_id = ?

        """, (user.id,))

    else:

        cursor.execute("""

        UPDATE users

        SET text_count = text_count + 1,

            total_count = total_count + 1

        WHERE user_id = ?

        """, (user.id,))

  

    conn.commit()

  

  

async def track_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.message or not update.message.from_user:

        return

  

    user = update.message.from_user

  

    is_media = any([

        update.message.photo,

        update.message.video,

        update.message.document,

        update.message.animation,

        update.message.voice,

        update.message.sticker

    ])

  

    update_user(user, is_media)

  

  

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):

    cursor.execute("""

        SELECT username, text_count, media_count, total_count

        FROM users

        ORDER BY total_count DESC

        LIMIT 10

    """)

    rows = cursor.fetchall()

  

    if not rows:

        await update.message.reply_text("No data yet!")

        return

  

    message = "🏆 Leaderboard\n\n"

    for i, row in enumerate(rows, start=1):

        message += (

            f"{i}. {row[0]}\n"

            f"   📝 {row[1]} | 🖼 {row[2]} | 📊 {row[3]}\n\n"

        )

  

    await update.message.reply_text(message)

  

  

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user = update.message.from_user

    cursor.execute("""

        SELECT text_count, media_count, total_count

        FROM users WHERE user_id = ?

    """, (user.id,))

    row = cursor.fetchone()

  

    if not row:

        await update.message.reply_text("No stats yet!")

        return

  

    await update.message.reply_text(

        f"📊 Your Stats\n\n"

        f"📝 Text: {row[0]}\n"

        f"🖼 Media: {row[1]}\n"

        f"📊 Total: {row[2]}"

    )

  

  

def main():

    app = ApplicationBuilder().token(TOKEN).build()

  

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, track_messages))

    app.add_handler(CommandHandler("leaderboard", leaderboard))

    app.add_handler(CommandHandler("stats", stats))

  

    app.run_polling()

  

  

if __name__ == "__main__":

    main()
