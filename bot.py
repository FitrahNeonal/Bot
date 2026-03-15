import asyncio
import logging
import sqlite3
import time

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, BadRequest, TelegramError
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from config import TOKEN

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── UI ─────────────────────────────────────────────────────────────────────
FEEDBACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("📝 Beri Feedback", url="https://feedbackneo.vercel.app")]
])

CARI_PARTNER = ReplyKeyboardMarkup(
    [["🚀 Cari partner"]],
    resize_keyboard=True,
    input_field_placeholder="🚀 Cari partner"
)

# ─── FIX 1: Persistent storage (SQLite) ─────────────────────────────────────
# Data tidak hilang saat bot restart.

def init_db():
    con = sqlite3.connect("bot_state.db")
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_chats (
            user_id   INTEGER PRIMARY KEY,
            partner_id INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS waiting_users (
            user_id   INTEGER PRIMARY KEY,
            joined_at REAL NOT NULL
        )
    """)
    con.commit()
    con.close()

def db_add_waiting(user_id: int):
    con = sqlite3.connect("bot_state.db")
    con.execute("INSERT OR IGNORE INTO waiting_users VALUES (?, ?)", (user_id, time.time()))
    con.commit(); con.close()

def db_remove_waiting(user_id: int):
    con = sqlite3.connect("bot_state.db")
    con.execute("DELETE FROM waiting_users WHERE user_id = ?", (user_id,))
    con.commit(); con.close()

def db_is_waiting(user_id: int) -> bool:
    con = sqlite3.connect("bot_state.db")
    row = con.execute("SELECT 1 FROM waiting_users WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row is not None

def db_pop_any_waiting(exclude: int) -> int | None:
    """Ambil satu user dari waiting list (selain `exclude`), lalu hapus dari list."""
    con = sqlite3.connect("bot_state.db")
    row = con.execute(
        "SELECT user_id FROM waiting_users WHERE user_id != ? ORDER BY joined_at LIMIT 1",
        (exclude,)
    ).fetchone()
    if row:
        con.execute("DELETE FROM waiting_users WHERE user_id = ?", (row[0],))
        con.commit()
    con.close()
    return row[0] if row else None

def db_add_chat(user_id: int, partner_id: int):
    con = sqlite3.connect("bot_state.db")
    con.execute("INSERT OR REPLACE INTO active_chats VALUES (?, ?)", (user_id, partner_id))
    con.execute("INSERT OR REPLACE INTO active_chats VALUES (?, ?)", (partner_id, user_id))
    con.commit(); con.close()

def db_get_partner(user_id: int) -> int | None:
    con = sqlite3.connect("bot_state.db")
    row = con.execute("SELECT partner_id FROM active_chats WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row[0] if row else None

def db_remove_chat(user_id: int, partner_id: int | None = None):
    con = sqlite3.connect("bot_state.db")
    con.execute("DELETE FROM active_chats WHERE user_id = ?", (user_id,))
    if partner_id:
        con.execute("DELETE FROM active_chats WHERE user_id = ?", (partner_id,))
    con.commit(); con.close()

# ─── FIX 2: asyncio.Lock – cegah race condition saat matchmaking ─────────────
match_lock = asyncio.Lock()

# ─── Helper: putuskan chat karena partner tidak bisa dihubungi ───────────────
async def _force_disconnect(user_id: int, partner_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Panggil saat Forbidden/error fatal — bersihkan state kedua sisi."""
    db_remove_chat(user_id, partner_id)
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kayaknya partner kamu udah nge-block bot ini. Chat diputus otomatis.\n\nPakai /find buat nyari yang baru.</i>",
            parse_mode="HTML"
        )
    except TelegramError:
        pass

# ─── Handlers ────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "👤 <b>Anonymous Chat</b>\n"
            "Chat sama orang random, tanpa ketahuan siapa kamu.\n\n"
            "Tekan tombol di bawah buat mulai.\n"
            "/help kalau butuh panduan.\n\n"
            "💬 Ada saran? → https://feedbackneo.vercel.app"
        ),
        parse_mode="HTML",
        reply_markup=CARI_PARTNER
    )


async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if db_get_partner(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kamu masih nyambung sama partner sekarang.</i>\nPakai /skip kalau mau ganti.",
            parse_mode="HTML"
        )
        return

    if db_is_waiting(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="🔎 <i>Masih nyari nih, tunggu bentar...</i>\nPakai /stop kalau mau batal.",
            parse_mode="HTML"
        )
        return

    # FIX 2: Lock saat matchmaking supaya tidak ada race condition
    async with match_lock:
        partner = db_pop_any_waiting(exclude=user_id)

        if partner:
            db_add_chat(user_id, partner)

    if partner:
        msg = (
            "✅ <b>Ketemu!</b> Sekarang kamu lagi chat sama orang asing.\n\n"
            "/skip — ganti partner\n"
            "/stop — cabut dari chat\n\n"
            "<code>https://t.me/anonyneo_bot</code>"
        )
        await context.bot.send_message(chat_id=user_id, text=msg, parse_mode="HTML")
        await context.bot.send_message(chat_id=partner, text=msg, parse_mode="HTML")
        logger.info("Matched: %s <-> %s", user_id, partner)
    else:
        db_add_waiting(user_id)
        await context.bot.send_message(
            chat_id=user_id,
            text="🔎 <i>Lagi nyariin partner buat kamu...</i>\nPakai /stop kalau mau batal.",
            parse_mode="HTML"
        )


async def message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    # Tombol cari partner
    if update.message.text == "🚀 Cari partner":
        await find(update, context)
        return

    user_id = update.effective_user.id
    partner = db_get_partner(user_id)

    if not partner:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "<i>Kamu belum punya partner. 👀\n\n"
                "Pakai /find dulu buat nyari seseorang.</i>\n\n"
                "https://feedbackneo.vercel.app"
            ),
            parse_mode="HTML"
        )
        return

    # FIX 3: Tangkap error spesifik, bukan except kosong
    try:
        await context.bot.copy_message(
            chat_id=partner,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )
    except Forbidden:
        # Partner memblokir bot → putuskan chat
        logger.warning("User %s blocked the bot. Disconnecting from %s.", partner, user_id)
        await _force_disconnect(user_id, partner, context)
    except BadRequest as e:
        logger.error("BadRequest saat forward pesan: %s", e)
    except TelegramError as e:
        logger.error("TelegramError saat forward pesan: %s", e)


async def skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Kalau lagi waiting, langsung batalkan dulu
    db_remove_waiting(user_id)

    partner = db_get_partner(user_id)

    if partner:
        db_remove_chat(user_id, partner)

        await context.bot.send_message(
            chat_id=user_id,
            text="🔎 <i>Oke, nyari yang baru...</i>",
            parse_mode="HTML"
        )
        await context.bot.send_message(
            chat_id=partner,
            text="💨 <i>Partner kamu cabut. Gak apa-apa, masih banyak ikan di laut.</i>\n\n/find — cari partner baru",
            parse_mode="HTML",
            reply_markup=FEEDBACK_BUTTON
        )
    
    # Langsung cari partner baru
    await find(update, context)


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Kalau lagi di waiting list
    if db_is_waiting(user_id):
        db_remove_waiting(user_id)
        await context.bot.send_message(
            chat_id=user_id,
            text="🛑 <i>Pencarian dibatalkan. Santuy.</i>",
            parse_mode="HTML"
        )
        return

    partner = db_get_partner(user_id)

    if not partner:
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kamu lagi gak nyambung sama siapa-siapa.\n\nPakai /find buat mulai.</i>",
            parse_mode="HTML"
        )
        return

    db_remove_chat(user_id, partner)

    await context.bot.send_message(
        chat_id=user_id,
        text="👋 <i>Chat selesai. Makasih udah mampir!</i>",
        parse_mode="HTML",
        reply_markup=FEEDBACK_BUTTON
    )
    await context.bot.send_message(
        chat_id=partner,
        text=(
            "💨 <i>Partner kamu udah cabut.</i>\n\n"
            "/find — cari partner baru\n\n"
            "Btw, ada feedback buat kami? Bebas banget."
        ),
        parse_mode="HTML",
        reply_markup=FEEDBACK_BUTTON
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "<b>Anonymous Chat — Cara Pakai</b>\n\n"
            "<b>1. Cari partner</b>\n"
            "Ketik /find atau tekan tombol <b>🚀 Cari partner</b>.\n\n"
            "<b>2. Mulai chat</b>\n"
            "Begitu partner ketemu, langsung kirim pesan aja.\n"
            "Identitas kamu tetap anonim.\n\n"
            "<b>3. Ganti partner</b>\n"
            "/skip — disconnect dan langsung nyari yang baru.\n\n"
            "<b>4. Keluar</b>\n"
            "/stop — akhiri chat.\n\n"
            "⚠️ <b>Jangan</b> spam, NSFW, atau nyebarin info pribadi orang.\n\n"
            "Ada masukan? Feedback kamu sangat berarti."
        ),
        parse_mode="HTML",
        reply_markup=FEEDBACK_BUTTON
    )


def main():
    init_db()  # Inisialisasi database saat pertama jalan

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("find", find))
    app.add_handler(CommandHandler("skip", skip))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(MessageHandler(~filters.COMMAND, message))

    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
