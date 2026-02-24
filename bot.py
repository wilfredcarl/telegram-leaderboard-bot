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
    MessageReactionHandler,
    filters,
)

# ----------------------------
# Config
# ----------------------------
TOKEN = os.getenv("TOKEN")
TZ = ZoneInfo("America/Montreal")

DM_NOTICE_SECONDS = 5

# ✅ Railway volume-safe default (mount your Volume at /data)
DB_PATH = os.getenv("DB_PATH", "/data/leaderboard.db")

# ✅ Leaderboard size (Top 10)
LEADERBOARD_LIMIT = 10


def parse_int_env(name: str, default: int = 0) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"⚠️ Invalid {name} env value: {raw!r}. Using {default}.")
        return default


# Optional: fixed single-group mode via env (overrides /setgroup)
DEFAULT_GROUP_CHAT_ID_ENV = parse_int_env("DEFAULT_GROUP_CHAT_ID", 0)
META_DEFAULT_GROUP_KEY = "default_group_chat_id"


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

# ✅ permanent message store so reactions can be credited to the author
cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    author_username TEXT,
    is_video INTEGER DEFAULT 0,
    created_at_utc INTEGER NOT NULL,
    PRIMARY KEY (chat_id, message_id)
)
""")

# ✅ track who reacted (so adds/removes don’t double count)
cursor.execute("""
CREATE TABLE IF NOT EXISTS reactions (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    reactor_id INTEGER NOT NULL,
    emoji TEXT NOT NULL,
    created_at_utc INTEGER NOT NULL,
    PRIMARY KEY (chat_id, message_id, reactor_id, emoji)
)
""")

# ✅ per-message reaction totals (for top reacted video)
cursor.execute("""
CREATE TABLE IF NOT EXISTS message_reaction_totals (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    total_reactions INTEGER DEFAULT 0,
    PRIMARY KEY (chat_id, message_id)
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


def ensure_reaction_columns():
    cursor.execute("PRAGMA table_info(users)")
    cols = {row[1] for row in cursor.fetchall()}
    needed = {
        "react_count": "ALTER TABLE users ADD COLUMN react_count INTEGER DEFAULT 0",
        "all_react_count": "ALTER TABLE users ADD COLUMN all_react_count INTEGER DEFAULT 0",
    }
    for col, stmt in needed.items():
        if col not in cols:
            cursor.execute(stmt)
    conn.commit()


ensure_reaction_columns()


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


def _del_meta_sync(key: str):
    cursor.execute("DELETE FROM meta WHERE key = ?", (key,))


async def set_meta(key: str, value: str):
    async with db_lock:
        _set_meta_sync(key, value)
        conn.commit()


async def del_meta(key: str):
    async with db_lock:
        _del_meta_sync(key)
        conn.commit()


async def get_default_group_chat_id() -> int | None:
    """
    Priority:
    1) DEFAULT_GROUP_CHAT_ID env var (fixed)
    2) meta default_group_chat_id (set by /setgroup in the group)
    3) None
    """
    if DEFAULT_GROUP_CHAT_ID_ENV:
        return DEFAULT_GROUP_CHAT_ID_ENV

    async with db_lock:
        raw = _get_meta_sync(META_DEFAULT_GROUP_KEY)
    if not raw:
        return None
    try:
        v = int(str(raw).strip())
        return v if v else None
    except ValueError:
        return None


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


def thread_id_from_update(update: Update | None) -> int | None:
    if not update:
        return None
    msg = getattr(update, "effective_message", None)
    if not msg:
        return None
    return getattr(msg, "message_thread_id", None)


# ----------------------------
# Board meta pack/unpack (message_id + thread_id)
# ----------------------------
def _pack_board_meta(message_id: int, thread_id: int | None) -> str:
    return f"{int(message_id)}:{'' if thread_id is None else int(thread_id)}"


def _unpack_board_meta(value: str) -> tuple[int | None, int | None]:
    try:
        msg_part, thread_part = (value.split(":", 1) + [""])[:2]
        msg_id = int(msg_part) if msg_part.strip() else None
        thread_id = int(thread_part) if thread_part.strip() else None
        return msg_id, thread_id
    except Exception:
        return None, None


# ----------------------------
# DM cleanup: delete the last "extra" DM messages when a new button is pressed
# ----------------------------
def _last_extra_dm_key(user_id: int) -> str:
    return f"last_extra_dm_msgs:{user_id}"


async def delete_previous_extra_dm(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    key = _last_extra_dm_key(user_id)
    async with db_lock:
        raw = _get_meta_sync(key)

    if not raw:
        return

    ids: list[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue

    for mid in ids:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=mid)
        except Exception:
            pass

    await del_meta(key)


async def store_extra_dm_messages(user_id: int, message_ids: list[int]):
    key = _last_extra_dm_key(user_id)
    payload = ",".join(str(m) for m in message_ids if m)
    if not payload:
        await del_meta(key)
        return
    await set_meta(key, payload)


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
    fixed = await get_default_group_chat_id()
    if fixed:
        return fixed

    if update and update.effective_chat and update.effective_user:
        if update.effective_chat.type in ("group", "supergroup"):
            return update.effective_chat.id
        return await get_user_last_chat(update.effective_user.id)

    return None


# ----------------------------
# DM-only sending (group notice 5s) + reply in same topic
# ----------------------------
async def send_dm_only(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    parse_mode: str | None = None,
    reply_markup=None,
):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return

    if chat.type == "private":
        await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
        return

    try:
        await save_user_context(user.id, chat.id)
    except Exception as e:
        print("save_user_context failed:", repr(e))

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

    tid = thread_id_from_update(update)
    try:
        if dm_ok:
            notice = await context.bot.send_message(
                chat_id=chat.id,
                text="📩 Sent you a DM.",
                message_thread_id=tid,
            )
        else:
            notice = await context.bot.send_message(
                chat_id=chat.id,
                text="❗ I couldn't DM you. Please open the bot in private and press Start, then try again.",
                message_thread_id=tid,
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
        [InlineKeyboardButton("🎬 Top Video", callback_data="nav:topvideo")],
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
        [InlineKeyboardButton("🎬 Top Video", callback_data="nav:topvideo")],
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
        [InlineKeyboardButton("🎬 Top Video", callback_data="nav:topvideo")],
        [InlineKeyboardButton("🏠 Home", callback_data="nav:home")],
    ])


def help_text() -> str:
    return (
        "📌 *Leaderboard Bot — Command Center*\n\n"
        "*Leaderboards*\n"
        f"• `/leaderboard` — Monthly Top {LEADERBOARD_LIMIT}\n"
        f"• `/leaderboard all` — All-time Top {LEADERBOARD_LIMIT}\n\n"
        "*Stats*\n"
        "• `/stats` — Your totals\n\n"
        "*History*\n"
        "• `/hof` — Hall of Fame (Top 3 each month)\n\n"
        "*Reactions*\n"
        "• ❤️ counted as *reactions received* on your media\n"
        "• `/topvideo` — DM the most reacted video\n\n"
        "*Group Board*\n"
        "• `/board` — Create/refresh the permanent group leaderboard (admin)\n"
        "• `/setgroup` — Set this group as the default group (admin)\n"
        "• `/resetstats` — Reset stats for this group (admin)\n\n"
        "*Admin*\n"
        "• `/forceaward` — Award pending media\n\n"
        "⏳ Media counts *24 hours after posting*."
    )


def rank_badge(i: int) -> str:
    if i == 1:
        return "🥇"
    if i == 2:
        return "🥈"
    if i == 3:
        return "🥉"
    keycaps = {
        4: "4️⃣", 5: "5️⃣", 6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣", 10: "🔟"
    }
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
                all_text_count, all_media_count, all_total_count,
                react_count, all_react_count
            )
            VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0)
        """, (chat_id, user_id, username))


def _award_media(chat_id: int, user_id: int, username: str, n: int):
    _ensure_user_row(chat_id, user_id, username)
    cursor.execute("""
        UPDATE users
        SET media_count = media_count + ?,
            all_media_count = all_media_count + ?,
            username = ?
        WHERE chat_id = ? AND user_id = ?
    """, (n, n, username, chat_id, user_id))


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
            # ✅ CHANGED: snapshot Top 3 by reactions first (then media)
            cursor.execute("""
                SELECT user_id, username, media_count, react_count
                FROM users
                WHERE chat_id = ?
                ORDER BY react_count DESC, media_count DESC
                LIMIT 3
            """, (chat_id,))
            top3 = cursor.fetchall()

            # ✅ CHANGED: store reactions as hall_of_fame.total_count
            for idx, (user_id, username, media_c, react_c) in enumerate(top3, start=1):
                cursor.execute("""
                    INSERT OR REPLACE INTO hall_of_fame
                    (chat_id, month, rank, user_id, username, total_count, text_count, media_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (chat_id, honor_month, idx, user_id, username, react_c, 0, media_c))

            cursor.execute("""
                UPDATE users
                SET media_count = 0,
                    react_count = 0
                WHERE chat_id = ?
            """, (chat_id,))

        _set_meta_sync("last_reset_month", new_month)
        conn.commit()


# ----------------------------
# Unified leaderboard renderer (same style everywhere) — Top 10
# ----------------------------
def group_board_key(chat_id: int) -> str:
    return f"group_lb_msg_id:{chat_id}"


async def render_leaderboard(chat_id: int, show_all_time: bool) -> str:
    await ensure_month_is_current()

    async with db_lock:
        if show_all_time:
            # ✅ CHANGED: sort by reactions first (then media)
            cursor.execute("""
                SELECT username, all_media_count, all_react_count
                FROM users
                WHERE chat_id = ?
                ORDER BY all_react_count DESC, all_media_count DESC
                LIMIT ?
            """, (chat_id, LEADERBOARD_LIMIT))
            rows = cursor.fetchall()
        else:
            # ✅ CHANGED: sort by reactions first (then media)
            cursor.execute("""
                SELECT username, media_count, react_count
                FROM users
                WHERE chat_id = ?
                ORDER BY react_count DESC, media_count DESC
                LIMIT ?
            """, (chat_id, LEADERBOARD_LIMIT))
            rows = cursor.fetchall()

    title = (
        f"🏆 *Leaderboard — All-Time (Top {LEADERBOARD_LIMIT})*"
        if show_all_time
        else f"🏆 *Leaderboard — This Month (Top {LEADERBOARD_LIMIT})*"
    )

    # ✅ (optional but consistent) clarify primary ranking
    subtitle = "_❤️ reactions received • 🖼 media (tiebreaker)_"

    if not rows:
        return f"{title}\n\nNo data yet! (Media counts after 24h.)"

    out = f"{title}\n{subtitle}\n\n"
    for i, (username, media_c, react_c) in enumerate(rows, start=1):
        name = username or "Unknown"
        out += (
            f"{rank_badge(i)} *{name}*\n"
            f"   ❤️ {react_c}  •  🖼 {media_c}\n\n"
        )
    return out.strip()


# ----------------------------
# Permanent group leaderboard (single message that updates) — ✅ stays in the topic
# ----------------------------
async def update_group_leaderboard(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    *,
    preferred_thread_id: int | None = None,
):
    await ensure_month_is_current()
    text = await render_leaderboard(chat_id, show_all_time=False)

    key = group_board_key(chat_id)
    async with db_lock:
        stored_raw = _get_meta_sync(key)

    stored_msg_id, stored_thread_id = (None, None)
    if stored_raw:
        stored_msg_id, stored_thread_id = _unpack_board_meta(stored_raw)

    # If /board is called in a different topic, move the board there
    if stored_msg_id and preferred_thread_id is not None and stored_thread_id != preferred_thread_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=int(stored_msg_id))
        except Exception:
            pass
        stored_msg_id = None
        stored_thread_id = None

    # Try edit existing
    if stored_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(stored_msg_id),
                text=text,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            return
        except Exception as e:
            print("edit_group_leaderboard failed (will recreate):", repr(e))
            stored_msg_id = None
            stored_thread_id = None

    # Create new message in the preferred topic
    try:
        msg = await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
            message_thread_id=preferred_thread_id,
        )
    except Exception as e:
        print("send_group_leaderboard failed:", repr(e))
        return

    # Store message_id + thread_id
    async with db_lock:
        _set_meta_sync(key, _pack_board_meta(msg.message_id, preferred_thread_id))
        conn.commit()

    # Try pin
    try:
        await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
    except Exception:
        pass


# ----------------------------
# Tracking messages (non-command only) — media only
# ----------------------------
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
    if not is_media:
        return

    created_at_utc = int(time_mod.time())

    is_video = bool(msg.video)
    if msg.document and (msg.document.mime_type or "").startswith("video/"):
        is_video = True

    async with db_lock:
        cursor.execute("""
            INSERT OR IGNORE INTO pending_messages
            (chat_id, message_id, user_id, username, kind, created_at_utc)
            VALUES (?, ?, ?, ?, 'media', ?)
        """, (chat_id, msg.message_id, user.id, username, created_at_utc))

        cursor.execute("""
            INSERT OR REPLACE INTO messages
            (chat_id, message_id, author_id, author_username, is_video, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (chat_id, msg.message_id, user.id, username, 1 if is_video else 0, created_at_utc))

        conn.commit()


# ----------------------------
# Reaction tracking (reactions received)
# ----------------------------
async def track_reactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mr = getattr(update, "message_reaction", None)
    if not mr or not mr.chat or not mr.user:
        return

    chat_id = mr.chat.id
    message_id = mr.message_id
    reactor_id = mr.user.id
    now = int(time_mod.time())

    old_list = mr.old_reaction or []
    new_list = mr.new_reaction or []

    def emoji_of(r):
        return getattr(r, "emoji", None) or str(r)

    old_set = {emoji_of(r) for r in old_list}
    new_set = {emoji_of(r) for r in new_list}

    added = new_set - old_set
    removed = old_set - new_set

    if not added and not removed:
        return

    async with db_lock:
        cursor.execute("""
            SELECT author_id, COALESCE(author_username, 'Unknown')
            FROM messages
            WHERE chat_id = ? AND message_id = ?
        """, (chat_id, message_id))
        row = cursor.fetchone()
        if not row:
            return

        author_id, author_username = row
        _ensure_user_row(chat_id, author_id, author_username)

        for emoji in added:
            cursor.execute("""
                INSERT OR IGNORE INTO reactions
                (chat_id, message_id, reactor_id, emoji, created_at_utc)
                VALUES (?, ?, ?, ?, ?)
            """, (chat_id, message_id, reactor_id, emoji, now))

            if cursor.rowcount:
                cursor.execute("""
                    UPDATE users
                    SET react_count = react_count + 1,
                        all_react_count = all_react_count + 1,
                        username = ?
                    WHERE chat_id = ? AND user_id = ?
                """, (author_username, chat_id, author_id))

                cursor.execute("""
                    INSERT INTO message_reaction_totals (chat_id, message_id, total_reactions)
                    VALUES (?, ?, 1)
                    ON CONFLICT(chat_id, message_id)
                    DO UPDATE SET total_reactions = total_reactions + 1
                """, (chat_id, message_id))

        for emoji in removed:
            cursor.execute("""
                DELETE FROM reactions
                WHERE chat_id = ? AND message_id = ? AND reactor_id = ? AND emoji = ?
            """, (chat_id, message_id, reactor_id, emoji))

            if cursor.rowcount:
                cursor.execute("""
                    UPDATE users
                    SET react_count = MAX(react_count - 1, 0),
                        all_react_count = MAX(all_react_count - 1, 0),
                        username = ?
                    WHERE chat_id = ? AND user_id = ?
                """, (author_username, chat_id, author_id))

                cursor.execute("""
                    INSERT INTO message_reaction_totals (chat_id, message_id, total_reactions)
                    VALUES (?, ?, 0)
                    ON CONFLICT(chat_id, message_id)
                    DO UPDATE SET total_reactions = MAX(total_reactions - 1, 0)
                """, (chat_id, message_id))

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
            SELECT chat_id, user_id, MAX(username) as username, COUNT(*)
            FROM pending_messages
            WHERE created_at_utc <= ?
              AND kind = 'media'
            GROUP BY chat_id, user_id
        """, (cutoff,))
        rows = cursor.fetchall()

        if not rows:
            return

        for chat_id, user_id, username, n in rows:
            _award_media(chat_id, user_id, username, n)

        cursor.execute("DELETE FROM pending_messages WHERE created_at_utc <= ?", (cutoff,))
        conn.commit()

    group_id = await get_default_group_chat_id()
    if group_id:
        await update_group_leaderboard(context, group_id)


async def monthly_reset_job(context: ContextTypes.DEFAULT_TYPE):
    await monthly_reset_internal()
    group_id = await get_default_group_chat_id()
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
        await send_dm_only(update, context, "❗ Set DEFAULT_GROUP_CHAT_ID (env) or run /setgroup in your group.")
        return

    args = [a.lower() for a in (context.args or [])]
    show_all_time = len(args) >= 1 and args[0] in ("all", "alltime", "lifetime")
    view = "all" if show_all_time else "month"

    text = await render_leaderboard(group_chat_id, show_all_time)
    await send_dm_only(update, context, text, parse_mode="Markdown", reply_markup=leaderboard_keyboard(view))


async def render_stats(chat_id: int, user_id: int) -> str:
    async with db_lock:
        cursor.execute("""
            SELECT media_count, react_count,
                   all_media_count, all_react_count
            FROM users
            WHERE chat_id = ? AND user_id = ?
        """, (chat_id, user_id))
        row = cursor.fetchone()

    if not row:
        return "You have no stats yet! (Media counts after 24h.)"

    m, r, am, ar = row
    return (
        "📊 Your Stats\n\n"
        "📅 This Month\n"
        f"• 🖼 Media: {m}\n"
        f"• ❤️ Reactions received: {r}\n\n"
        "🕰 All-Time\n"
        f"• 🖼 Media: {am}\n"
        f"• ❤️ Reactions received: {ar}\n\n"
        "⏳ Media is awarded after 24 hours."
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await ensure_month_is_current()

    group_chat_id = await resolve_group_chat_id(update)
    if not group_chat_id:
        await send_dm_only(update, context, "❗ Set DEFAULT_GROUP_CHAT_ID (env) or run /setgroup in your group.")
        return

    text = await render_stats(group_chat_id, update.effective_user.id)
    await send_dm_only(update, context, text, reply_markup=secondary_keyboard())


async def render_hof(chat_id: int) -> str:
    async with db_lock:
        # ✅ CHANGED: include total_count (reactions score) + media_count
        cursor.execute("""
            SELECT month, rank, username, total_count, media_count
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
    for month, rank, username, reacts, media_c in rows:
        if month != current:
            current = month
            out += f"📅 {month}\n"
        # ✅ CHANGED: show reactions as primary score
        out += f"  {rank_badge(rank)} {username} — ❤️ {reacts}  •  🖼 {media_c}\n"
        if rank == 3:
            out += "\n"
    return out


async def hof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    group_chat_id = await resolve_group_chat_id(update)
    if not group_chat_id:
        await send_dm_only(update, context, "❗ Set DEFAULT_GROUP_CHAT_ID (env) or run /setgroup in your group.")
        return

    text = await render_hof(group_chat_id)
    await send_dm_only(update, context, text, reply_markup=secondary_keyboard())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)
    await send_dm_only(update, context, help_text(), parse_mode="Markdown", reply_markup=home_keyboard())


# ----------------------------
# Top reacted video (DM + button)
# ----------------------------
async def get_top_reacted_video(chat_id: int):
    async with db_lock:
        cursor.execute("""
            SELECT m.message_id,
                   COALESCE(m.author_username, 'Unknown') AS author,
                   COALESCE(t.total_reactions, 0) AS reacts
            FROM messages m
            LEFT JOIN message_reaction_totals t
              ON t.chat_id = m.chat_id AND t.message_id = m.message_id
            WHERE m.chat_id = ? AND m.is_video = 1
            ORDER BY reacts DESC, m.created_at_utc DESC
            LIMIT 1
        """, (chat_id,))
        row = cursor.fetchone()

    if not row:
        return None
    mid, author, reacts = row
    return int(mid), str(author), int(reacts)


async def topvideo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    user = update.effective_user
    if not user:
        return

    group_chat_id = await resolve_group_chat_id(update)
    if not group_chat_id:
        await send_dm_only(update, context, "❗ Set DEFAULT_GROUP_CHAT_ID (env) or run /setgroup in your group.")
        return

    top = await get_top_reacted_video(group_chat_id)
    if not top:
        await send_dm_only(update, context, "🎬 No reacted videos found yet.", reply_markup=secondary_keyboard())
        return

    mid, author, reacts = top

    await delete_previous_extra_dm(context, user.id)

    sent_ids: list[int] = []

    dm_ok = False
    try:
        copied = await context.bot.copy_message(
            chat_id=user.id,
            from_chat_id=group_chat_id,
            message_id=mid,
        )
        sent_ids.append(copied.message_id)
        dm_ok = True
    except Exception as e:
        print("copy_message failed, trying forward:", repr(e))
        try:
            forwarded = await context.bot.forward_message(
                chat_id=user.id,
                from_chat_id=group_chat_id,
                message_id=mid,
            )
            sent_ids.append(forwarded.message_id)
            dm_ok = True
        except Exception as e2:
            print("forward_message failed:", repr(e2))

    if dm_ok:
        summary = await context.bot.send_message(
            chat_id=user.id,
            text=f"🎬 *Top reacted video*\n❤️ {reacts} reactions\n👤 @{author}",
            parse_mode="Markdown",
            disable_web_page_preview=True,
            reply_markup=secondary_keyboard(),
        )
        sent_ids.append(summary.message_id)
        await store_extra_dm_messages(user.id, sent_ids)

        if update.effective_chat and update.effective_chat.type != "private":
            tid = thread_id_from_update(update)
            try:
                notice = await context.bot.send_message(
                    update.effective_chat.id,
                    "📩 Sent you the top video in DM.",
                    message_thread_id=tid,
                )
                schedule_delete(context, update.effective_chat.id, notice.message_id, DM_NOTICE_SECONDS)
            except Exception:
                pass
        return

    await send_dm_only(
        update,
        context,
        "❗ I couldn't DM you the video. Please open the bot in private and press Start, then try again.",
        reply_markup=secondary_keyboard(),
    )


# ----------------------------
# Group-only admin commands
# ----------------------------
async def setgroup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin (group): set this group as the default group (stored in DB meta)."""
    await safe_delete_message(update.message)

    chat = update.effective_chat
    user = update.effective_user
    if not chat or chat.type not in ("group", "supergroup") or not user:
        return

    member = await chat.get_member(user.id)
    if member.status not in ("administrator", "creator"):
        try:
            msg = await context.bot.send_message(chat.id, "❌ Admins only.", message_thread_id=thread_id_from_update(update))
            schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    await set_meta(META_DEFAULT_GROUP_KEY, str(chat.id))

    try:
        msg = await context.bot.send_message(
            chat.id,
            f"✅ Default group set to this chat:\n`{chat.id}`\n\n(You can now use commands in DM.)",
            parse_mode="Markdown",
            message_thread_id=thread_id_from_update(update),
        )
        schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
    except Exception:
        pass


async def resetstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin (group): reset stats for THIS group only."""
    await safe_delete_message(update.message)

    chat = update.effective_chat
    user = update.effective_user
    if not chat or chat.type not in ("group", "supergroup") or not user:
        return

    member = await chat.get_member(user.id)
    if member.status not in ("administrator", "creator"):
        try:
            msg = await context.bot.send_message(chat.id, "❌ Admins only.", message_thread_id=thread_id_from_update(update))
            schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    chat_id = chat.id

    async with db_lock:
        cursor.execute("DELETE FROM users WHERE chat_id = ?", (chat_id,))
        cursor.execute("DELETE FROM pending_messages WHERE chat_id = ?", (chat_id,))
        cursor.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
        cursor.execute("DELETE FROM reactions WHERE chat_id = ?", (chat_id,))
        cursor.execute("DELETE FROM message_reaction_totals WHERE chat_id = ?", (chat_id,))
        cursor.execute("DELETE FROM hall_of_fame WHERE chat_id = ?", (chat_id,))
        cursor.execute("DELETE FROM meta WHERE key = ?", (f"group_lb_msg_id:{chat_id}",))
        conn.commit()

    await update_group_leaderboard(context, chat_id)

    try:
        msg = await context.bot.send_message(
            chat.id,
            "✅ Reset stats for this group and refreshed the board.",
            message_thread_id=thread_id_from_update(update),
        )
        schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
    except Exception:
        pass


async def forceaward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    chat = update.effective_chat
    user = update.effective_user

    member = await chat.get_member(user.id)
    if member.status not in ("administrator", "creator"):
        try:
            msg = await context.bot.send_message(chat.id, "❌ Admins only.", message_thread_id=thread_id_from_update(update))
            schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    await ensure_month_is_current()

    async with db_lock:
        cursor.execute("""
            SELECT user_id, MAX(username) as username, COUNT(*)
            FROM pending_messages
            WHERE chat_id = ?
              AND kind = 'media'
            GROUP BY user_id
        """, (chat.id,))
        rows = cursor.fetchall()

        if rows:
            for user_id, username, n in rows:
                _award_media(chat.id, user_id, username, n)

            cursor.execute("DELETE FROM pending_messages WHERE chat_id = ?", (chat.id,))
            conn.commit()

    await update_group_leaderboard(context, chat.id)

    try:
        msg = await context.bot.send_message(
            chat.id,
            "✅ Awarded pending media and refreshed the board.",
            message_thread_id=thread_id_from_update(update),
        )
        schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
    except Exception:
        pass


async def board(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_delete_message(update.message)

    chat = update.effective_chat
    user = update.effective_user

    if not chat or chat.type not in ("group", "supergroup"):
        await send_dm_only(update, context, "Use /board inside the group.")
        return

    member = await chat.get_member(user.id)
    if member.status not in ("administrator", "creator"):
        try:
            msg = await context.bot.send_message(chat.id, "❌ Admins only.", message_thread_id=thread_id_from_update(update))
            schedule_delete(context, chat.id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    # ✅ Create/refresh board in the SAME topic the command was typed in
    tid = thread_id_from_update(update)
    await update_group_leaderboard(context, chat.id, preferred_thread_id=tid)

    try:
        msg = await context.bot.send_message(
            chat.id,
            "📌 Leaderboard board created/updated.",
            message_thread_id=tid,
        )
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

    if query.message.chat.type != "private":
        try:
            msg = await context.bot.send_message(chat_id=query.message.chat_id, text="📩 Please use the DM I sent you.")
            schedule_delete(context, query.message.chat_id, msg.message_id, DM_NOTICE_SECONDS)
        except Exception:
            pass
        return

    await delete_previous_extra_dm(context, query.from_user.id)

    group_chat_id = await get_default_group_chat_id()
    if not group_chat_id:
        group_chat_id = await get_user_last_chat(query.from_user.id)

    if not group_chat_id:
        await query.edit_message_text(
            "❗ Set DEFAULT_GROUP_CHAT_ID (env) or run /setgroup in your group.",
            reply_markup=home_keyboard(),
        )
        return

    data = query.data or ""

    if data in ("nav:home", "nav:help"):
        await query.edit_message_text(help_text(), reply_markup=home_keyboard(), parse_mode="Markdown")
        return

    if data == "lb:month":
        text = await render_leaderboard(group_chat_id, show_all_time=False)
        await query.edit_message_text(text, reply_markup=leaderboard_keyboard("month"), parse_mode="Markdown")
        return

    if data == "lb:all":
        text = await render_leaderboard(group_chat_id, show_all_time=True)
        await query.edit_message_text(text, reply_markup=leaderboard_keyboard("all"), parse_mode="Markdown")
        return

    if data == "nav:topvideo":
        top = await get_top_reacted_video(group_chat_id)
        if not top:
            await query.edit_message_text("🎬 No reacted videos found yet.", reply_markup=secondary_keyboard())
            return

        mid, author, reacts = top

        sent_ids: list[int] = []
        sent = False
        try:
            copied = await context.bot.copy_message(
                chat_id=query.from_user.id,
                from_chat_id=group_chat_id,
                message_id=mid,
            )
            sent_ids.append(copied.message_id)
            sent = True
        except Exception as e:
            print("copy_message failed in button, trying forward:", repr(e))
            try:
                forwarded = await context.bot.forward_message(
                    chat_id=query.from_user.id,
                    from_chat_id=group_chat_id,
                    message_id=mid,
                )
                sent_ids.append(forwarded.message_id)
                sent = True
            except Exception as e2:
                print("forward_message failed in button:", repr(e2))

        if sent:
            await store_extra_dm_messages(query.from_user.id, sent_ids)
            await query.edit_message_text(
                f"✅ Sent the top reacted video!\n\n❤️ {reacts} reactions\n👤 @{author}",
                reply_markup=secondary_keyboard(),
            )
        else:
            await query.edit_message_text(
                "❗ I couldn't DM the video. Please open the bot in private and press Start, then try again.",
                reply_markup=secondary_keyboard(),
            )
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
            BotCommand("leaderboard", f"DM: monthly leaderboard (Top {LEADERBOARD_LIMIT}; add 'all' for all-time)"),
            BotCommand("stats", "DM: your stats"),
            BotCommand("hof", "DM: hall of fame"),
            BotCommand("topvideo", "DM: most reacted video"),
            BotCommand("help", "DM: show commands"),
            BotCommand("board", "Admin (group): create/refresh the permanent leaderboard board"),
            BotCommand("forceaward", "Admin (group): award pending media"),
            BotCommand("setgroup", "Admin (group): set this group as default"),
            BotCommand("resetstats", "Admin (group): reset stats for this group"),
        ])
    except Exception:
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

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, track_messages), group=1)
    app.add_handler(MessageReactionHandler(track_reactions), group=1)

    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("hof", hof))
    app.add_handler(CommandHandler("topvideo", topvideo))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("commands", help_command))

    app.add_handler(CommandHandler("board", board))
    app.add_handler(CommandHandler("forceaward", forceaward))
    app.add_handler(CommandHandler("setgroup", setgroup))
    app.add_handler(CommandHandler("resetstats", resetstats))

    app.add_handler(CallbackQueryHandler(on_button))

    app.job_queue.run_repeating(award_matured_messages, interval=60 * 60, first=30)
    app.job_queue.run_monthly(
        monthly_reset_job,
        when=time(hour=0, minute=5, tzinfo=TZ),
        day=1,
    )

    async def _startup_board(ctx: ContextTypes.DEFAULT_TYPE):
        gid = await get_default_group_chat_id()
        if gid:
            await update_group_leaderboard(ctx, gid)

    app.job_queue.run_once(_startup_board, when=5)

    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
