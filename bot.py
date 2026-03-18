import asyncio
import logging
import os
import time

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, BadRequest, TelegramError
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)
from config import TOKEN

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
TURSO_URL      = os.environ["TURSO_URL"]
TURSO_TOKEN    = os.environ["TURSO_TOKEN"]
ADMIN_ID       = 7396627060
MSG_THRESHOLD  = 5
TIME_THRESHOLD = 600
RECONNECT_TTL  = 21600
REPORT_LIMIT   = 3
STREAK_LIMIT   = 3

# ─── UI ──────────────────────────────────────────────────────────────────────
FEEDBACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("📝 Beri Feedback", url="https://feedbackneo.vercel.app")]
])

CARI_PARTNER = ReplyKeyboardMarkup(
    [["🚀 Cari partner"], ["📊 Stats"]],
    resize_keyboard=True,
    input_field_placeholder="🚀 Cari partner"
)

def btn_waiting():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Batalkan", callback_data="cancel_find")]])

def btn_chat():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⏭ Skip", callback_data="skip"),
        InlineKeyboardButton("🛑 Stop", callback_data="stop"),
    ]])

def btn_confirm_skip():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yakin skip", callback_data="confirm_skip"),
        InlineKeyboardButton("❌ Gak jadi", callback_data="cancel_action"),
    ]])

def btn_confirm_stop():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yakin stop", callback_data="confirm_stop"),
        InlineKeyboardButton("❌ Gak jadi", callback_data="cancel_action"),
    ]])

def btn_report_reason():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("💬 Spam", callback_data="report_spam"),
        InlineKeyboardButton("🔞 Sange", callback_data="report_sange"),
        InlineKeyboardButton("❌ Batal", callback_data="cancel_action"),
    ]])

def btn_reconnect(partner_id: int):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Hubungkan lagi", callback_data=f"reconnect_{partner_id}")
    ]])

def btn_find_again():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔍 Cari partner baru", callback_data="find_again")
    ]])

def btn_gender_pref():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("👨 Cowok", callback_data="findgender_cowok"),
        InlineKeyboardButton("👩 Cewek", callback_data="findgender_cewek"),
        InlineKeyboardButton("🎲 Random", callback_data="findgender_random"),
    ]])

def btn_after_stop(partner_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Hubungkan lagi", callback_data=f"reconnect_{partner_id}")],
        [InlineKeyboardButton("🚩 Report", callback_data=f"report_after_{partner_id}")],
        [InlineKeyboardButton("📝 Beri Feedback", url="https://feedbackneo.vercel.app")]
    ])

def btn_gender():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("👨 Cowok", callback_data="set_gender_cowok"),
        InlineKeyboardButton("👩 Cewek", callback_data="set_gender_cewek"),
        InlineKeyboardButton("🙈 Rahasia", callback_data="set_gender_rahasia"),
    ]])

def btn_kota():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Jakarta", callback_data="set_kota_Jakarta"),
         InlineKeyboardButton("Surabaya", callback_data="set_kota_Surabaya"),
         InlineKeyboardButton("Bandung", callback_data="set_kota_Bandung")],
        [InlineKeyboardButton("Medan", callback_data="set_kota_Medan"),
         InlineKeyboardButton("Makassar", callback_data="set_kota_Makassar"),
         InlineKeyboardButton("Semarang", callback_data="set_kota_Semarang")],
        [InlineKeyboardButton("Palembang", callback_data="set_kota_Palembang"),
         InlineKeyboardButton("Tangerang", callback_data="set_kota_Tangerang"),
         InlineKeyboardButton("Depok", callback_data="set_kota_Depok")],
        [InlineKeyboardButton("Bekasi", callback_data="set_kota_Bekasi"),
         InlineKeyboardButton("Yogyakarta", callback_data="set_kota_Yogyakarta"),
         InlineKeyboardButton("Malang", callback_data="set_kota_Malang")],
        [InlineKeyboardButton("🗺️ Lainnya", callback_data="set_kota_lainnya"),
         InlineKeyboardButton("⏭ Lewati", callback_data="skip_kota")],
    ])

def btn_umur():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("< 17", callback_data="set_umur_<17"),
         InlineKeyboardButton("17-20", callback_data="set_umur_17-20"),
         InlineKeyboardButton("21-24", callback_data="set_umur_21-24")],
        [InlineKeyboardButton("25-29", callback_data="set_umur_25-29"),
         InlineKeyboardButton("30+", callback_data="set_umur_30+")],
        [InlineKeyboardButton("⏭ Lewati", callback_data="skip_umur")],
    ])

def btn_profile_edit():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Edit Gender", callback_data="edit_gender"),
         InlineKeyboardButton("✏️ Edit Kota", callback_data="edit_kota")],
        [InlineKeyboardButton("✏️ Edit Umur", callback_data="edit_umur")],
    ])

def btn_skip_umur():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⏭ Lewati", callback_data="skip_umur")
    ]])

# ─── Database ────────────────────────────────────────────────────────────────
def execute_turso(sql: str, params: list = None) -> list:
    import urllib.request, json as _json
    stmt = {"sql": sql}
    if params:
        stmt["args"] = [
            {"type": "null"} if p is None else {"type": "text", "value": str(p)}
            for p in params
        ]
    data = _json.dumps({"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]}).encode()
    req = urllib.request.Request(
        TURSO_URL.replace("libsql://", "https://") + "/v2/pipeline",
        data=data,
        headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as res:
        result = _json.loads(res.read())["results"][0]["response"]["result"]
    return [[col.get("value") for col in row] for row in result["rows"]]

def init_db():
    for sql in [
        """CREATE TABLE IF NOT EXISTS active_chats (
            user_id INTEGER PRIMARY KEY,
            partner_id INTEGER NOT NULL,
            msg_count INTEGER DEFAULT 0,
            started_at REAL NOT NULL)""",
        """CREATE TABLE IF NOT EXISTS waiting_users (
            user_id INTEGER PRIMARY KEY,
            joined_at REAL NOT NULL,
            gender_pref TEXT DEFAULT NULL)""",
        """CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_seen REAL NOT NULL,
            referred_by INTEGER DEFAULT NULL,
            banned INTEGER DEFAULT 0,
            skip_streak INTEGER DEFAULT 0,
            last_skip REAL DEFAULT 0,
            gender TEXT DEFAULT NULL,
            kota TEXT DEFAULT NULL,
            umur TEXT DEFAULT NULL)""",
        """CREATE TABLE IF NOT EXISTS referrals (
            referrer_id INTEGER NOT NULL,
            referred_id INTEGER NOT NULL,
            referred_at REAL NOT NULL,
            PRIMARY KEY (referrer_id, referred_id))""",
        """CREATE TABLE IF NOT EXISTS reports (
            reporter_id INTEGER NOT NULL,
            reported_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            reported_at REAL NOT NULL,
            PRIMARY KEY (reporter_id, reported_id))""",
        """CREATE TABLE IF NOT EXISTS reconnect_requests (
            user_id INTEGER NOT NULL,
            partner_id INTEGER NOT NULL,
            requested_at REAL NOT NULL,
            PRIMARY KEY (user_id, partner_id))""",
    ]:
        execute_turso(sql)

def db_add_waiting(user_id: int, gender_pref: str | None = None):
    execute_turso(
        "INSERT OR IGNORE INTO waiting_users VALUES (?, ?, ?)",
        [user_id, time.time(), gender_pref]
    )

def db_remove_waiting(user_id: int):
    execute_turso("DELETE FROM waiting_users WHERE user_id = ?", [user_id])

def db_is_waiting(user_id: int) -> bool:
    return len(execute_turso("SELECT 1 FROM waiting_users WHERE user_id = ?", [user_id])) > 0

def db_pop_any_waiting(exclude: int, gender_pref: str | None = None) -> int | None:
    """Ambil user dari waiting list. Kalau gender_pref diisi, filter by gender."""
    if gender_pref and gender_pref != "random":
        # Cari yang gender-nya sesuai preferensi
        rows = execute_turso("""
            SELECT w.user_id FROM waiting_users w
            JOIN users u ON w.user_id = u.user_id
            WHERE w.user_id != ? AND u.gender = ?
            ORDER BY w.joined_at LIMIT 1
        """, [exclude, gender_pref])
    else:
        rows = execute_turso(
            "SELECT user_id FROM waiting_users WHERE user_id != ? ORDER BY joined_at LIMIT 1",
            [exclude]
        )
    if rows:
        partner = int(rows[0][0])
        execute_turso("DELETE FROM waiting_users WHERE user_id = ?", [partner])
        return partner
    return None

def db_get_waiting_since(user_id: int) -> float | None:
    rows = execute_turso("SELECT joined_at FROM waiting_users WHERE user_id = ?", [user_id])
    return float(rows[0][0]) if rows else None

def db_get_gender_pref(user_id: int) -> str | None:
    rows = execute_turso("SELECT gender_pref FROM waiting_users WHERE user_id = ?", [user_id])
    return rows[0][0] if rows else None

def db_add_chat(user_id: int, partner_id: int):
    now = time.time()
    execute_turso("INSERT OR REPLACE INTO active_chats VALUES (?, ?, 0, ?)", [user_id, partner_id, now])
    execute_turso("INSERT OR REPLACE INTO active_chats VALUES (?, ?, 0, ?)", [partner_id, user_id, now])

def db_get_partner(user_id: int) -> int | None:
    rows = execute_turso("SELECT partner_id FROM active_chats WHERE user_id = ?", [user_id])
    return int(rows[0][0]) if rows else None

def db_remove_chat(user_id: int, partner_id: int | None = None):
    execute_turso("DELETE FROM active_chats WHERE user_id = ?", [user_id])
    if partner_id:
        execute_turso("DELETE FROM active_chats WHERE user_id = ?", [partner_id])

def db_increment_msg(user_id: int):
    execute_turso("UPDATE active_chats SET msg_count = msg_count + 1 WHERE user_id = ?", [user_id])

def db_get_chat_info(user_id: int) -> dict | None:
    rows = execute_turso(
        "SELECT partner_id, msg_count, started_at FROM active_chats WHERE user_id = ?", [user_id]
    )
    if not rows:
        return None
    return {
        "partner_id": int(rows[0][0]),
        "msg_count": int(rows[0][1] or 0),
        "started_at": float(rows[0][2] or 0),
    }

def db_needs_confirmation(user_id: int) -> bool:
    info = db_get_chat_info(user_id)
    if not info:
        return False
    return info["msg_count"] >= MSG_THRESHOLD or (time.time() - info["started_at"]) >= TIME_THRESHOLD

def db_get_stats() -> dict:
    waiting  = int(execute_turso("SELECT COUNT(*) FROM waiting_users")[0][0] or 0)
    chatting = int(execute_turso("SELECT COUNT(*) FROM active_chats")[0][0] or 0) // 2
    total    = int(execute_turso("SELECT COUNT(*) FROM users")[0][0] or 0)
    return {"waiting": waiting, "chatting": chatting, "total": total}

def db_register_user(user_id: int, referred_by: int | None = None) -> bool:
    rows = execute_turso("SELECT 1 FROM users WHERE user_id = ?", [user_id])
    is_new = len(rows) == 0
    if is_new:
        execute_turso("INSERT INTO users VALUES (?, ?, ?, 0, 0, 0, NULL, NULL, NULL)", [user_id, time.time(), referred_by])
        if referred_by:
            execute_turso("INSERT OR IGNORE INTO referrals VALUES (?, ?, ?)", [referred_by, user_id, time.time()])
    return is_new

def db_get_referral_count(user_id: int) -> int:
    rows = execute_turso("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", [user_id])
    return int(rows[0][0] or 0)

def db_get_profile(user_id: int) -> dict | None:
    rows = execute_turso(
        "SELECT gender, kota, umur FROM users WHERE user_id = ?", [user_id]
    )
    if not rows:
        return None
    return {
        "gender": rows[0][0],
        "kota":   rows[0][1],
        "umur":   rows[0][2],
    }

def db_set_gender(user_id: int, gender: str):
    execute_turso("UPDATE users SET gender = ? WHERE user_id = ?", [gender, user_id])

def db_set_kota(user_id: int, kota: str):
    execute_turso("UPDATE users SET kota = ? WHERE user_id = ?", [kota, user_id])

def db_set_umur(user_id: int, umur: str):
    execute_turso("UPDATE users SET umur = ? WHERE user_id = ?", [umur, user_id])

def db_has_gender(user_id: int) -> bool:
    rows = execute_turso("SELECT gender FROM users WHERE user_id = ?", [user_id])
    return bool(rows and rows[0][0] is not None)

def db_is_banned(user_id: int) -> bool:
    rows = execute_turso("SELECT banned FROM users WHERE user_id = ?", [user_id])
    return bool(rows and int(rows[0][0] or 0) == 1)

def db_ban_user(user_id: int):
    execute_turso("UPDATE users SET banned = 1 WHERE user_id = ?", [user_id])

def db_update_skip_streak(user_id: int) -> int:
    rows = execute_turso("SELECT skip_streak, last_skip FROM users WHERE user_id = ?", [user_id])
    if not rows:
        return 0
    streak    = int(rows[0][0] or 0)
    last_skip = float(rows[0][1] or 0)
    now       = time.time()
    if now - last_skip > 60:
        streak = 0
    streak += 1
    execute_turso("UPDATE users SET skip_streak = ?, last_skip = ? WHERE user_id = ?", [streak, now, user_id])
    return streak

def db_reset_skip_streak(user_id: int):
    execute_turso("UPDATE users SET skip_streak = 0 WHERE user_id = ?", [user_id])

def db_add_report(reporter_id: int, reported_id: int, reason: str) -> int:
    execute_turso(
        "INSERT OR IGNORE INTO reports VALUES (?, ?, ?, ?)",
        [reporter_id, reported_id, reason, time.time()]
    )
    rows = execute_turso("SELECT COUNT(*) FROM reports WHERE reported_id = ?", [reported_id])
    return int(rows[0][0] or 0)

def db_request_reconnect(user_id: int, partner_id: int):
    execute_turso(
        "INSERT OR REPLACE INTO reconnect_requests VALUES (?, ?, ?)",
        [user_id, partner_id, time.time()]
    )

def db_check_reconnect(user_a: int, user_b: int) -> bool:
    now = time.time()
    rows_a = execute_turso(
        "SELECT requested_at FROM reconnect_requests WHERE user_id = ? AND partner_id = ?", [user_a, user_b]
    )
    rows_b = execute_turso(
        "SELECT requested_at FROM reconnect_requests WHERE user_id = ? AND partner_id = ?", [user_b, user_a]
    )
    if not rows_a or not rows_b:
        return False
    return now - float(rows_a[0][0]) < RECONNECT_TTL and now - float(rows_b[0][0]) < RECONNECT_TTL

def db_clear_reconnect(user_a: int, user_b: int):
    execute_turso(
        "DELETE FROM reconnect_requests WHERE (user_id = ? AND partner_id = ?) OR (user_id = ? AND partner_id = ?)",
        [user_a, user_b, user_b, user_a]
    )

# ─── Locks ───────────────────────────────────────────────────────────────────
match_lock = asyncio.Lock()

# ─── Helpers ─────────────────────────────────────────────────────────────────
async def _remove_inline_buttons(context, chat_id: int, message_id: int):
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id, message_id=message_id, reply_markup=None
        )
    except TelegramError:
        pass

async def _force_disconnect(user_id: int, partner_id: int, context):
    db_remove_chat(user_id, partner_id)
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kayaknya partner kamu udah nge-block bot ini. Chat diputus otomatis.</i>",
            parse_mode="HTML",
            reply_markup=btn_find_again()
        )
    except TelegramError:
        pass

async def _do_find(user_id: int, context, gender_pref: str | None = None):
    if db_is_banned(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="🚫 <i>Akun kamu kena ban karena laporan dari pengguna lain.</i>",
            parse_mode="HTML"
        )
        return

    # Cek gender dulu — wajib sebelum find
    if not db_has_gender(user_id):
        context.user_data["after_gender"] = "find"
        context.user_data["after_onboarding"] = "find"
        await context.bot.send_message(
            chat_id=user_id,
            text="👤 Sebelum mulai, kamu itu?",
            reply_markup=btn_gender()
        )
        return

    if db_get_partner(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kamu masih nyambung sama partner sekarang.</i>\nPakai /next kalau mau ganti.",
            parse_mode="HTML"
        )
        return

    if db_is_waiting(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="🔎 <i>Masih nyari nih, tunggu bentar...</i>",
            parse_mode="HTML"
        )
        return

    async with match_lock:
        partner = db_pop_any_waiting(exclude=user_id, gender_pref=gender_pref)
        if partner:
            db_add_chat(user_id, partner)

    if partner:
        msg = (
            "✅ <b>Ketemu!</b> Sekarang kamu lagi chat sama orang asing.\n\n"
            "<code>https://t.me/anonyneo_bot</code>"
        )
        await context.bot.send_message(chat_id=user_id,  text=msg, parse_mode="HTML", reply_markup=btn_chat())
        await context.bot.send_message(chat_id=partner,  text=msg, parse_mode="HTML", reply_markup=btn_chat())
        logger.info("Matched: %s <-> %s", user_id, partner)
    else:
        db_add_waiting(user_id, gender_pref)
        s = db_get_stats()
        online = s["chatting"] * 2 + s["waiting"]
        pref_text = f" (nyari: {gender_pref})" if gender_pref and gender_pref != "random" else ""
        await context.bot.send_message(
            chat_id=user_id,
            text=f"🔎 <i>Lagi nyariin partner buat kamu{pref_text}...</i>\nAda <b>{online}</b> orang online sekarang.",
            parse_mode="HTML",
            reply_markup=btn_waiting()
        )
        # Schedule fallback ke random setelah 2 menit
        if gender_pref and gender_pref != "random":
            context.application.job_queue.run_once(
                _fallback_to_random,
                when=120,
                data={"user_id": user_id, "original_pref": gender_pref},
                name=f"fallback_{user_id}"
            )

async def _fallback_to_random(context):
    """Dipanggil setelah 2 menit kalau belum dapat partner dengan gender preference."""
    data    = context.job.data
    user_id = data["user_id"]

    # Cek apakah masih waiting dan belum dapat partner
    if not db_is_waiting(user_id):
        return

    # Cek apakah masih punya preferensi yang sama
    current_pref = db_get_gender_pref(user_id)
    if current_pref != data["original_pref"]:
        return

    # Hapus dari waiting dan cari ulang tanpa filter
    db_remove_waiting(user_id)
    await context.bot.send_message(
        chat_id=user_id,
        text="⏰ <i>Gak ada yang cocok nih. Nyambungin ke random aja ya!</i>",
        parse_mode="HTML"
    )
    await _do_find(user_id, context, gender_pref=None)


async def _do_skip(user_id: int, context):
    partner = db_get_partner(user_id)
    if partner:
        db_remove_chat(user_id, partner)
        streak = db_update_skip_streak(user_id)
        await context.bot.send_message(chat_id=user_id, text="🔎 <i>Oke, nyari yang baru...</i>", parse_mode="HTML")
        await context.bot.send_message(
            chat_id=partner,
            text="💨 <i>Partner kamu cabut.</i>",
            parse_mode="HTML",
            reply_markup=btn_reconnect(user_id)
        )
        if streak >= STREAK_LIMIT:
            await context.bot.send_message(
                chat_id=user_id,
                text="💡 <i>Psst, coba ngobrol dulu sebelum skip — siapa tau cocok!</i>",
                parse_mode="HTML"
            )
            db_reset_skip_streak(user_id)
    await _do_find(user_id, context)

async def _do_stop(user_id: int, context):
    partner = db_get_partner(user_id)
    if not partner:
        return
    db_remove_chat(user_id, partner)
    await context.bot.send_message(
        chat_id=user_id,
        text="👋 <i>Chat selesai. Makasih udah mampir!</i>",
        parse_mode="HTML",
        reply_markup=btn_after_stop(partner)
    )
    await context.bot.send_message(
        chat_id=partner,
        text="💨 <i>Partner kamu udah cabut.</i>\n\nBtw, ada feedback buat kami? Bebas banget.",
        parse_mode="HTML",
        reply_markup=btn_after_stop(user_id)
    )

# ─── Handlers ────────────────────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    referred_by = None
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            try:
                referrer_id = int(arg[4:])
                if referrer_id != user_id:
                    referred_by = referrer_id
            except ValueError:
                pass

    is_new = db_register_user(user_id, referred_by)
    if is_new and referred_by:
        ref_count = db_get_referral_count(referred_by)
        try:
            await context.bot.send_message(
                chat_id=referred_by,
                text=f"🎉 <b>Seseorang join lewat link kamu!</b>\n\nTotal yang kamu ajak: <b>{ref_count}</b> orang. Mantap!",
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
    await _do_find(update.effective_user.id, context)


async def findgender(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if db_get_partner(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kamu masih nyambung sama partner sekarang.</i>",
            parse_mode="HTML"
        )
        return

    if db_is_waiting(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="🔎 <i>Masih nyari nih, tunggu bentar...</i>",
            parse_mode="HTML"
        )
        return

    if not db_has_gender(user_id):
        context.user_data["after_gender"] = "findgender"
        await context.bot.send_message(
            chat_id=user_id,
            text="👤 Sebelum mulai, kamu itu?",
            reply_markup=btn_gender()
        )
        return

    await context.bot.send_message(
        chat_id=user_id,
        text="🔍 Mau chat sama siapa?",
        reply_markup=btn_gender_pref()
    )


async def message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    # Cek waiting broadcast dari admin
    if update.effective_user.id == ADMIN_ID and context.user_data.get("waiting_broadcast"):
        context.user_data["waiting_broadcast"] = False
        pesan = update.message.text
        rows  = execute_turso("SELECT user_id FROM users WHERE banned = 0")
        success = 0
        for (user_id,) in rows:
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"📢 <b>Pengumuman</b>\n\n{pesan}\n\n— owner",
                    parse_mode="HTML"
                )
                success += 1
            except Exception:
                pass
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"✅ Broadcast selesai — {success}/{len(rows)} user berhasil."
        )
        return

    user_id = update.effective_user.id

    # Cek waiting kota lainnya
    if context.user_data.get("waiting_kota"):
        context.user_data["waiting_kota"] = False
        kota = update.message.text.strip()
        db_set_kota(user_id, kota)
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ Kota disimpan: <b>{kota}</b>\n\nUmur kamu? (opsional)",
            parse_mode="HTML",
            reply_markup=btn_umur()
        )
        return

    if update.message.text == "🚀 Cari partner":
        await find(update, context)
        return
    if update.message.text == "📊 Stats":
        await stats(update, context)
        return

    partner = db_get_partner(user_id)

    if not partner:
        await context.bot.send_message(
            chat_id=user_id,
            text="<i>Kamu belum punya partner. 👀\n\nPakai /find dulu buat nyari seseorang.</i>",
            parse_mode="HTML"
        )
        return

    db_increment_msg(user_id)

    try:
        await context.bot.copy_message(
            chat_id=partner,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )
    except Forbidden:
        logger.warning("User %s blocked the bot. Disconnecting from %s.", partner, user_id)
        await _force_disconnect(user_id, partner, context)
    except BadRequest as e:
        logger.error("BadRequest: %s", e)
    except TelegramError as e:
        logger.error("TelegramError: %s", e)


async def skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    db_remove_waiting(user_id)
    if db_needs_confirmation(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ Kamu udah lumayan lama ngobrol sama partner ini.\nYakin mau skip?",
            reply_markup=btn_confirm_skip()
        )
        return
    await _do_skip(user_id, context)


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if db_is_waiting(user_id):
        db_remove_waiting(user_id)
        await context.bot.send_message(chat_id=user_id, text="🛑 <i>Pencarian dibatalkan. Santuy.</i>", parse_mode="HTML")
        return
    if not db_get_partner(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kamu lagi gak nyambung sama siapa-siapa.\n\nPakai /find buat mulai.</i>",
            parse_mode="HTML"
        )
        return
    if db_needs_confirmation(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ Kamu udah lumayan lama ngobrol sama partner ini.\nYakin mau stop?",
            reply_markup=btn_confirm_stop()
        )
        return
    await _do_stop(user_id, context)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    data    = query.data

    await query.answer()
    await _remove_inline_buttons(context, query.message.chat_id, query.message.message_id)

    if data == "cancel_find":
        if db_is_waiting(user_id):
            db_remove_waiting(user_id)
            await context.bot.send_message(chat_id=user_id, text="🛑 <i>Pencarian dibatalkan. Santuy.</i>", parse_mode="HTML")
        return

    if data == "find_again":
        await _do_find(user_id, context)
        return

    if data.startswith("findgender_"):
        pref = data.split("findgender_")[1]  # cowok, cewek, atau random
        await _do_find(user_id, context, gender_pref=pref)
        return

    if data == "skip":
        if not db_get_partner(user_id):
            return
        if db_needs_confirmation(user_id):
            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ Kamu udah lumayan lama ngobrol sama partner ini.\nYakin mau skip?",
                reply_markup=btn_confirm_skip()
            )
        else:
            await _do_skip(user_id, context)
        return

    if data == "confirm_skip":
        if not db_get_partner(user_id):
            await context.bot.send_message(chat_id=user_id, text="⚠️ <i>Kamu sudah tidak punya partner.</i>", parse_mode="HTML")
            return
        await _do_skip(user_id, context)
        return

    if data == "stop":
        if db_is_waiting(user_id):
            db_remove_waiting(user_id)
            await context.bot.send_message(chat_id=user_id, text="🛑 <i>Pencarian dibatalkan. Santuy.</i>", parse_mode="HTML")
            return
        if not db_get_partner(user_id):
            return
        if db_needs_confirmation(user_id):
            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ Kamu udah lumayan lama ngobrol sama partner ini.\nYakin mau stop?",
                reply_markup=btn_confirm_stop()
            )
        else:
            await _do_stop(user_id, context)
        return

    if data == "confirm_stop":
        if not db_get_partner(user_id):
            await context.bot.send_message(chat_id=user_id, text="⚠️ <i>Kamu sudah tidak punya partner.</i>", parse_mode="HTML")
            return
        await _do_stop(user_id, context)
        return

    if data == "cancel_action":
        await context.bot.send_message(
            chat_id=user_id,
            text="👍 <i>Oke, lanjut chat aja.</i>",
            parse_mode="HTML",
            reply_markup=btn_chat()
        )
        return

    if data == "report":
        if not db_get_partner(user_id):
            return
        await context.bot.send_message(
            chat_id=user_id,
            text="🚩 Laporkan partner kamu karena?",
            reply_markup=btn_report_reason()
        )
        return

    # Report setelah chat selesai — partner_id disertakan di callback data
    if data.startswith("report_after_"):
        try:
            reported_id = int(data.split("_")[2])
        except (IndexError, ValueError):
            return
        await context.bot.send_message(
            chat_id=user_id,
            text="🚩 Laporkan karena?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💬 Spam", callback_data=f"do_report_spam_{reported_id}"),
                InlineKeyboardButton("🔞 Sange", callback_data=f"do_report_sange_{reported_id}"),
                InlineKeyboardButton("❌ Batal", callback_data="cancel_action"),
            ]])
        )
        return

    if data.startswith("do_report_"):
        parts = data.split("_")
        try:
            reason      = parts[2]  # spam atau sange
            reported_id = int(parts[3])
        except (IndexError, ValueError):
            return
        total_reports = db_add_report(user_id, reported_id, reason)
        await context.bot.send_message(chat_id=user_id, text="✅ <i>Laporan dikirim. Terima kasih!</i>", parse_mode="HTML")
        logger.info("Report after: %s melaporkan %s (%s) — total: %d", user_id, reported_id, reason, total_reports)
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"🚩 <b>Report baru</b>\nDari: <code>{user_id}</code>\nDilaporkan: <code>{reported_id}</code>\nAlasan: {reason}\nTotal report: {total_reports}",
                parse_mode="HTML"
            )
        except TelegramError:
            pass
        if total_reports >= REPORT_LIMIT:
            db_ban_user(reported_id)
            await context.bot.send_message(chat_id=user_id, text="🚫 <i>User tersebut telah di-ban.</i>", parse_mode="HTML")
            try:
                await context.bot.send_message(chat_id=reported_id, text="🚫 <i>Akun kamu telah di-ban karena banyak laporan.</i>", parse_mode="HTML")
            except TelegramError:
                pass
        return

    if data in ("report_spam", "report_sange"):
        partner = db_get_partner(user_id)
        if not partner:
            await context.bot.send_message(chat_id=user_id, text="⚠️ <i>Kamu sudah tidak punya partner.</i>", parse_mode="HTML")
            return
        reason        = "spam" if data == "report_spam" else "sange"
        total_reports = db_add_report(user_id, partner, reason)
        await context.bot.send_message(chat_id=user_id, text="✅ <i>Laporan dikirim. Terima kasih!</i>", parse_mode="HTML")
        logger.info("Report: %s melaporkan %s (%s) — total: %d", user_id, partner, reason, total_reports)
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"🚩 <b>Report baru</b>\nDari: <code>{user_id}</code>\nDilaporkan: <code>{partner}</code>\nAlasan: {reason}\nTotal report: {total_reports}",
                parse_mode="HTML"
            )
        except TelegramError:
            pass
        if total_reports >= REPORT_LIMIT:
            db_ban_user(partner)
            db_remove_chat(user_id, partner)
            await context.bot.send_message(
                chat_id=user_id,
                text="🚫 <i>Partner kamu telah di-ban. Kamu diputuskan dari chat ini.</i>",
                parse_mode="HTML",
                reply_markup=btn_find_again()
            )
            try:
                await context.bot.send_message(
                    chat_id=partner,
                    text="🚫 <i>Akun kamu telah di-ban karena banyak laporan dari pengguna lain.</i>",
                    parse_mode="HTML"
                )
            except TelegramError:
                pass
        return

    if data.startswith("reconnect_"):
        try:
            partner_id = int(data.split("_")[1])
        except (IndexError, ValueError):
            return

        if db_get_partner(user_id) or db_get_partner(partner_id):
            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ <i>Salah satu dari kamu sedang dalam chat lain.</i>",
                parse_mode="HTML"
            )
            return

        rows = execute_turso(
            "SELECT requested_at FROM reconnect_requests WHERE user_id = ? AND partner_id = ?",
            [partner_id, user_id]
        )
        if rows and time.time() - float(rows[0][0]) > RECONNECT_TTL:
            await context.bot.send_message(
                chat_id=user_id,
                text="⏰ <i>Link reconnect sudah expired (6 jam).</i>",
                parse_mode="HTML"
            )
            return

        db_request_reconnect(user_id, partner_id)

        if db_check_reconnect(user_id, partner_id):
            db_clear_reconnect(user_id, partner_id)
            db_add_chat(user_id, partner_id)
            msg = "🔄 <b>Reconnected!</b> Kalian tersambung lagi."
            await context.bot.send_message(chat_id=user_id,    text=msg, parse_mode="HTML", reply_markup=btn_chat())
            await context.bot.send_message(chat_id=partner_id, text=msg, parse_mode="HTML", reply_markup=btn_chat())
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text="🔄 <i>Permintaan reconnect dikirim! Menunggu partner menyetujui...</i>",
                parse_mode="HTML"
            )
            try:
                await context.bot.send_message(
                    chat_id=partner_id,
                    text="🔄 <i>Partner kamu sebelumnya ingin terhubung lagi!</i>",
                    parse_mode="HTML",
                    reply_markup=btn_reconnect(user_id)
                )
            except TelegramError:
                pass
        return

    # ── Profile ───────────────────────────────────────────────────
    if data.startswith("set_gender_"):
        gender = data.split("set_gender_")[1]
        db_set_gender(user_id, gender)
        after = context.user_data.get("after_gender")
        context.user_data["after_gender"] = None

        if after in ("find", "findgender"):
            context.user_data["after_onboarding"] = "find"
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✅ Gender disimpan: <b>{gender}</b>\n\nKamu dari kota mana? (opsional)",
                parse_mode="HTML",
                reply_markup=btn_kota()
            )
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✅ Gender diupdate: <b>{gender}</b>",
                parse_mode="HTML"
            )
        return

    if data.startswith("set_kota_"):
        kota = data.split("set_kota_")[1]
        if kota == "lainnya":
            context.user_data["waiting_kota"] = True
            await context.bot.send_message(
                chat_id=user_id,
                text="📍 Ketik nama kotamu:",
            )
        else:
            db_set_kota(user_id, kota)
            await context.bot.send_message(
                chat_id=user_id,
                text=f"✅ Kota disimpan: <b>{kota}</b>\n\nUmur kamu? (opsional)",
                parse_mode="HTML",
                reply_markup=btn_umur()
            )
        return

    if data == "skip_kota":
        await context.bot.send_message(
            chat_id=user_id,
            text="Oke dilewati!\n\nUmur kamu? (opsional)",
            reply_markup=btn_umur()
        )
        return

    if data.startswith("set_umur_"):
        umur = data.split("set_umur_")[1]
        db_set_umur(user_id, umur)
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ Umur disimpan: <b>{umur}</b>\n\nProfil kamu sudah lengkap! 🎉",
            parse_mode="HTML"
        )
        after = context.user_data.pop("after_onboarding", None)
        if after == "find":
            await _do_find(user_id, context)
        return

    if data == "skip_umur":
        await context.bot.send_message(
            chat_id=user_id,
            text="Oke dilewati! Profil kamu sudah disimpan. 🎉"
        )
        after = context.user_data.pop("after_onboarding", None)
        if after == "find":
            await _do_find(user_id, context)
        return

    if data in ("edit_gender",):
        context.user_data["after_gender"] = "profile"
        await context.bot.send_message(
            chat_id=user_id,
            text="👤 Pilih gender kamu:",
            reply_markup=btn_gender()
        )
        return

    if data == "edit_kota":
        await context.bot.send_message(
            chat_id=user_id,
            text="📍 Pilih kota kamu:",
            reply_markup=btn_kota()
        )
        return

    if data == "edit_umur":
        await context.bot.send_message(
            chat_id=user_id,
            text="🎂 Umur kamu? (opsional)",
            reply_markup=btn_umur()
        )
        return


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
    user_id   = update.effective_user.id
    ref_count = db_get_referral_count(user_id)
    link      = f"https://t.me/anonyneo_bot?start=ref_{user_id}"
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
            "<b>3. Ganti / stop / report</b>\n"
            "Gunakan tombol <b>⏭ Skip</b> atau /next, <b>🛑 Stop</b>, atau <b>🚩 Report</b> di bawah pesan.\n\n"
            "<b>4. Hubungkan lagi</b>\n"
            "Setelah chat selesai, ada tombol <b>🔄 Hubungkan lagi</b> kalau mau balik ke partner yang sama.\n"
            "Berlaku 6 jam, dan harus disetujui kedua pihak.\n\n"
            "<b>5. Ajak teman</b>\n"
            "/invite — dapat link untuk ajak temenmu.\n\n"
            "<b>6. Profil</b>\n"
            "/profile — lihat dan edit profil kamu.\n\n"
            "<b>7. Statistik</b>\n"
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

    if cmd == "stats":
        s             = db_get_stats()
        total_refs    = int(execute_turso("SELECT COUNT(*) FROM referrals")[0][0] or 0)
        total_reports = int(execute_turso("SELECT COUNT(*) FROM reports")[0][0] or 0)
        total_banned  = int(execute_turso("SELECT COUNT(*) FROM users WHERE banned = 1")[0][0] or 0)

        # Gender stats
        gender_rows = execute_turso("SELECT gender, COUNT(*) FROM users WHERE gender IS NOT NULL GROUP BY gender")
        gender_text = "\n".join([f"  {r[0]}: {r[1]}" for r in gender_rows]) or "  belum ada data"

        # Top kota
        kota_rows = execute_turso("SELECT kota, COUNT(*) as c FROM users WHERE kota IS NOT NULL GROUP BY kota ORDER BY c DESC LIMIT 5")
        kota_text = "\n".join([f"  {r[0]}: {r[1]}" for r in kota_rows]) or "  belum ada data"

        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                "📊 <b>Admin Stats</b>\n\n"
                f"👥 Total user: <b>{s['total']}</b>\n"
                f"💬 Lagi chat: <b>{s['chatting']}</b> pasang\n"
                f"🔎 Lagi waiting: <b>{s['waiting']}</b> orang\n"
                f"🔗 Total referral: <b>{total_refs}</b>\n"
                f"🚩 Total report: <b>{total_reports}</b>\n"
                f"🚫 Total banned: <b>{total_banned}</b>\n\n"
                f"⚧ <b>Gender:</b>\n{gender_text}\n\n"
                f"🗺️ <b>Top kota:</b>\n{kota_text}"
            ),
            parse_mode="HTML"
        )

    elif cmd == "users":
        rows = execute_turso(
            "SELECT user_id, first_seen, referred_by FROM users ORDER BY first_seen DESC LIMIT 20"
        )
        if not rows:
            await context.bot.send_message(chat_id=ADMIN_ID, text="Belum ada user.")
            return
        lines = ["👥 <b>20 User Terbaru</b>\n"]
        for user_id, first_seen, referred_by in rows:
            import datetime
            try:
                tgl = datetime.datetime.fromtimestamp(float(first_seen)).strftime("%d/%m %H:%M")
            except (TypeError, ValueError):
                tgl = "?"
            ref = f" (ref: {referred_by})" if referred_by else ""
            lines.append(f"• <code>{user_id}</code>{ref} — {tgl}")
        await context.bot.send_message(chat_id=ADMIN_ID, text="\n".join(lines), parse_mode="HTML")

    elif cmd == "broadcast":
        context.user_data["waiting_broadcast"] = True
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text="📝 Kirim pesannya sekarang (bisa multiline):",
        )

    else:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text="⚠️ Command tidak dikenal. Ketik /admin untuk bantuan."
        )


async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    p = db_get_profile(user_id)
    if not p:
        await context.bot.send_message(chat_id=user_id, text="⚠️ <i>Data kamu tidak ditemukan.</i>", parse_mode="HTML")
        return

    gender = p["gender"] or "—"
    kota   = p["kota"]   or "—"
    umur   = p["umur"]   or "—"

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "👤 <b>Profil kamu</b>\n\n"
            f"⚧ Gender: <b>{gender}</b>\n"
            f"📍 Kota: <b>{kota}</b>\n"
            f"🎂 Umur: <b>{umur}</b>"
        ),
        parse_mode="HTML",
        reply_markup=btn_profile_edit()
    )


async def notify_online(app):
    rows = execute_turso("SELECT DISTINCT user_id FROM active_chats")
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
    app.add_handler(CommandHandler("profile", profile))
    app.add_handler(CommandHandler("find", find))
    app.add_handler(CommandHandler("findgender", findgender))
    app.add_handler(CommandHandler("next", skip))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(~filters.COMMAND, message))

    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
