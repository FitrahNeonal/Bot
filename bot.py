import asyncio
import logging
import os
import time

import libsql_experimental as libsql

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

# ─── Turso config ────────────────────────────────────────────────────────────
TURSO_URL   = os.environ["TURSO_URL"]
TURSO_TOKEN = os.environ["TURSO_TOKEN"]
ADMIN_ID    = 7396627060

def get_con():
    return libsql.connect(database=TURSO_URL, auth_token=TURSO_TOKEN)

def query_turso(sql: str) -> list:
    url = TURSO_URL.replace("libsql://", "https://") + "/v2/pipeline"
    import urllib.request, json as _json
    data = _json.dumps({"requests": [{"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}).encode()
    req = urllib.request.Request(url, data=data, headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as res:
        rows = _json.loads(res.read())["results"][0]["response"]["result"]["rows"]
    return [[col["value"] for col in row] for row in rows]

# ─── UI ─────────────────────────────────────────────────────────────────────
FEEDBACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("📝 Beri Feedback", url="https://feedbackneo.vercel.app")]
])

CARI_PARTNER = ReplyKeyboardMarkup(
    [["🚀 Cari partner"], ["📊 Stats"]],
    resize_keyboard=True,
    input_field_placeholder="🚀 Cari partner"
)

# ─── Database (Turso) ────────────────────────────────────────────────────────

def init_db():
    con = get_con()
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            first_seen  REAL NOT NULL,
            referred_by INTEGER DEFAULT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS referrals (
            referrer_id  INTEGER NOT NULL,
            referred_id  INTEGER NOT NULL,
            referred_at  REAL NOT NULL,
            PRIMARY KEY (referrer_id, referred_id)
        )
    """)
    con.commit()
    con.close()

def db_add_waiting(user_id: int):
    con = get_con()
    con.execute("INSERT OR IGNORE INTO waiting_users VALUES (?, ?)", (user_id, time.time()))
    con.commit(); con.close()

def db_remove_waiting(user_id: int):
    con = get_con()
    con.execute("DELETE FROM waiting_users WHERE user_id = ?", (user_id,))
    con.commit(); con.close()

def db_is_waiting(user_id: int) -> bool:
    con = get_con()
    row = con.execute("SELECT 1 FROM waiting_users WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row is not None

def db_pop_any_waiting(exclude: int) -> int | None:
    """Ambil satu user dari waiting list (selain `exclude`), lalu hapus dari list."""
    con = get_con()
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
    con = get_con()
    con.execute("INSERT OR REPLACE INTO active_chats VALUES (?, ?)", (user_id, partner_id))
    con.execute("INSERT OR REPLACE INTO active_chats VALUES (?, ?)", (partner_id, user_id))
    con.commit(); con.close()

def db_get_partner(user_id: int) -> int | None:
    con = get_con()
    row = con.execute("SELECT partner_id FROM active_chats WHERE user_id = ?", (user_id,)).fetchone()
    con.close()
    return row[0] if row else None

def db_remove_chat(user_id: int, partner_id: int | None = None):
    con = get_con()
    con.execute("DELETE FROM active_chats WHERE user_id = ?", (user_id,))
    if partner_id:
        con.execute("DELETE FROM active_chats WHERE user_id = ?", (partner_id,))
    con.commit(); con.close()

def db_get_stats() -> dict:
    con = get_con()
    waiting  = con.execute("SELECT COUNT(*) FROM waiting_users").fetchone()[0]
    chatting = con.execute("SELECT COUNT(*) FROM active_chats").fetchone()[0] // 2
    total    = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    con.close()
    return {"waiting": waiting, "chatting": chatting, "total": total}

def db_register_user(user_id: int, referred_by: int | None = None):
    con = get_con()
    is_new = con.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)).fetchone() is None
    if is_new:
        con.execute(
            "INSERT INTO users VALUES (?, ?, ?)",
            (user_id, time.time(), referred_by)
        )
        if referred_by:
            con.execute(
                "INSERT OR IGNORE INTO referrals VALUES (?, ?, ?)",
                (referred_by, user_id, time.time())
            )
    con.commit(); con.close()
    return is_new

def db_get_referral_count(user_id: int) -> int:
    con = get_con()
    count = con.execute(
        "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)
    ).fetchone()[0]
    con.close()
    return count

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
    user_id = update.effective_user.id

    # Cek apakah ada referral parameter (?start=ref_USERID)
    referred_by = None
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg[4:])
                if referrer_id != user_id:  # gak bisa refer diri sendiri
                    referred_by = referrer_id
            except ValueError:
                pass

    is_new = db_register_user(user_id, referred_by)

    # Kalau user baru dan ada referrer, kasih notif ke si pengundang
    if is_new and referred_by:
        ref_count = db_get_referral_count(referred_by)
        try:
            await context.bot.send_message(
                chat_id=referred_by,
                text=(
                    f"🎉 <b>Seseorang join lewat link kamu!</b>\n\n"
                    f"Total yang kamu ajak: <b>{ref_count}</b> orang. Mantap!"
                ),
                parse_mode="HTML"
            )
        except TelegramError:
            pass

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "👤 <b>Anonymous Chat</b>\n"
            "Chat sama orang random, tanpa ketahuan siapa kamu.\n\n"
            "Tekan tombol di bawah buat mulai.\n"
            "/help kalau butuh panduan.\n\n"
            f"👥 Udah <b>{db_get_stats()['total']}</b> orang yang pernah mampir.\n\n"
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
        s = db_get_stats()
        online = s["chatting"] * 2 + s["waiting"]
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"🔎 <i>Lagi nyariin partner buat kamu...</i>\n"
                f"Ada <b>{online}</b> orang online sekarang.\n\n"
                "Pakai /stop kalau mau batal."
            ),
            parse_mode="HTML"
        )


async def message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    # Tombol cari partner
    if update.message.text == "🚀 Cari partner":
        await find(update, context)
        return

    # Tombol stats
    if update.message.text == "📊 Stats":
        await stats(update, context)
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


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = db_get_stats()
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "📊 <b>Status Bot</b>\n\n"
            f"💬 Lagi chat: <b>{s['chatting']}</b> pasang\n"
            f"🔎 Lagi nyari: <b>{s['waiting']}</b> orang\n"
            f"👥 Total pengguna: <b>{s['total']}</b> orang\n\n"
            "<i>Makin rame makin seru — ajak temenmu!</i>"
        ),
        parse_mode="HTML"
    )


async def invite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ref_count = db_get_referral_count(user_id)
    link = f"https://t.me/anonyneo_bot?start=ref_{user_id}"

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "🔗 <b>Ajak temenmu!</b>\n\n"
            f"Link invite kamu:\n<code>{link}</code>\n\n"
            f"👥 Udah <b>{ref_count}</b> orang yang join lewat kamu.\n\n"
            "<i>Makin banyak yang join, makin cepet dapet partner!</i>"
        ),
        parse_mode="HTML"
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
            "<b>5. Ajak teman</b>\n"
            "/invite — dapat link untuk ajak temenmu.\n\n"
            "<b>6. Statistik</b>\n"
            "/stats — lihat berapa orang yang lagi online.\n\n"
            "⚠️ <b>Jangan</b> spam, NSFW, atau nyebarin info pribadi orang.\n\n"
            "Ada masukan? Feedback kamu sangat berarti."
        ),
        parse_mode="HTML",
        reply_markup=FEEDBACK_BUTTON
    )


def is_admin(update: Update) -> bool:
    return update.effective_user.id == ADMIN_ID


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    args = context.args
    if not args:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                "🛠 <b>Admin Panel</b>\n\n"
                "/admin stats — statistik lengkap\n"
                "/admin users — user terbaru\n"
                "/admin broadcast &lt;pesan&gt; — kirim pesan ke semua user"
            ),
            parse_mode="HTML"
        )
        return

    cmd = args[0].lower()

    # ── /admin stats ──────────────────────────────────────────────
    if cmd == "stats":
        s = db_get_stats()
        total_referrals = query_turso("SELECT COUNT(*) FROM referrals")[0][0]
        total_chats     = query_turso("SELECT COUNT(*) FROM active_chats")[0][0] // 2

        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                "📊 <b>Admin Stats</b>\n\n"
                f"👥 Total user: <b>{s['total']}</b>\n"
                f"💬 Lagi chat: <b>{s['chatting']}</b> pasang\n"
                f"🔎 Lagi waiting: <b>{s['waiting']}</b> orang\n"
                f"🔗 Total referral: <b>{total_referrals}</b>\n"
                f"💡 Pasang aktif sekarang: <b>{total_chats}</b>"
            ),
            parse_mode="HTML"
        )

    # ── /admin users ──────────────────────────────────────────────
    elif cmd == "users":
        rows = query_turso(
            "SELECT user_id, first_seen, referred_by FROM users ORDER BY first_seen DESC LIMIT 20"
        )
        if not rows:
            await context.bot.send_message(chat_id=ADMIN_ID, text="Belum ada user.")
            return

        lines = ["👥 <b>20 User Terbaru</b>\n"]
        for user_id, first_seen, referred_by in rows:
            import datetime
            tgl = datetime.datetime.fromtimestamp(first_seen).strftime("%d/%m %H:%M")
            ref = f" (ref: {referred_by})" if referred_by else ""
            lines.append(f"• <code>{user_id}</code>{ref} — {tgl}")

        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text="\n".join(lines),
            parse_mode="HTML"
        )

    # ── /admin broadcast <pesan> ──────────────────────────────────
    elif cmd == "broadcast":
        if len(args) < 2:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text="⚠️ Format: /admin broadcast &lt;pesan&gt;",
                parse_mode="HTML"
            )
            return

        pesan = " ".join(args[1:])
        rows  = query_turso("SELECT user_id FROM users")
        success = 0

        for (user_id,) in rows:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"📢 {pesan}",
                    parse_mode="HTML"
                )
                success += 1
            except Exception:
                pass

        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"✅ Broadcast selesai — {success}/{len(rows)} user berhasil."
        )

    else:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text="⚠️ Command tidak dikenal. Ketik /admin untuk bantuan."
        )


async def notify_online(app):
    """Kirim notif ke semua yang lagi aktif chat saat bot nyala."""
    rows = query_turso("SELECT DISTINCT user_id FROM active_chats")
    for (user_id,) in rows:
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="✅ <i>Bot udah nyala lagi, lanjut chat!</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass
    logger.info("Notif startup dikirim ke %d user.", len(rows))


def main():
    init_db()

    app = ApplicationBuilder().token(TOKEN).post_init(notify_online).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("invite", invite))
    app.add_handler(CommandHandler("find", find))
    app.add_handler(CommandHandler("skip", skip))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(MessageHandler(~filters.COMMAND, message))

    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
