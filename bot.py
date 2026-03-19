import asyncio
import logging
import os
import random
import time

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
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

# ─── Matched Messages (Ramadan Edition) ──────────────────────────────────────
MATCHED_MESSAGES = [
    # Reminder ibadah
    "Fokus cari berkah, bukan cari celah buat maksiat. 🚫",
    "Sahur tadi makan apa? Semoga kuat puasanya sampai maghrib! ☀️",
    "Udah dzuhur belum? Ngobrol boleh, sholat jangan sampai lupa. 🕌",
    "Ramadan momen buat jadi manusia lebih baik, mulai dari chat ini. 🌟",
    "Sambil nunggu buka, ngobrol yang berfaedah ya. 🌙",
    "Siapa tau dari ngobrol ini dapet temen diskusi terbaik kamu. 🤝",
    "Ngobrol yang bikin pahala nambah, bukan malah berkurang. ✨",
    "Partner lu berhak dapet chat yang bikin adem hati, bukan bikin emosi. 🧘",
    "Inget, malaikat Rakib-Atid lagi lembur nyatat amal di bulan Ramadan. 📝",
    "Sopan dikit, malaikat lagi keliling nyebar rahmat, jangan malah maksiat. 🌧️",

    # Anti-sange
    "Jangan bikin partner ilfeel gara-gara chat lu nggak beradab. 🤢",
    "Sange = auto batal pahala puasa jalur VIP. 🔥",
    "Sange itu penyakit, obatnya cuma tobat nasuha. 💊",
    "Puasa itu latihan jadi orang sabar, bukan latihan jadi orang sange. 🧘",
    "Puasa woi, tahan nafsunya jangan sange. 🌙",
    "Sange di bulan puasa? Malu sama anak TPA sebelah. 👶",
    "Otak lu lagi puasa juga kan? Jangan disuapin hal haram. 🧠",
    "Kalau niat ngomong jorok, mending tutup app terus tadarus. 📖",
    "Jangan norak, lagi bulan puasa jangan bahas yang porno. 🚫",
    "Jangan mesum, ini bulan Ramadan bukan bulan bejat. 🌴",

    # Roasting halus
    "Ramadan kareem, bukan Ramadan maksiat. Bedain dong. 🌴",
    "Chat ini gratis, tapi dosa tetap ada harganya. 😇",
    "Kalau mau maksiat, tutup app ini dulu. Makasih. 🙏",
    "Inget, yang kamu ketik itu amal. Pilih yang berpahala. ⌨️",
    "Jangan spam, malaikat juga capek nyatatnya. 😮‍💨",
    "Niat chat buat apa? Kalau bukan yang baik, mending tidur aja. 😴",
    "Di bulan penuh berkah ini, yuk jadi manusia yang nggak nyebelin. 🤲",
    "Partner kamu juga lagi puasa. Jangan malah bikin buka puasa lebih awal karena emosi. 😤",

    # Wholesome
    "Siapa tau partner ini yang bakal ingetin kamu buat sahur. 🍽️",
    "Ngobrol yang baik, siapa tau jadi amal jariyah. 💫",
    "Yuk jadiin Ramadan ini lebih bermakna, mulai dari obrolan yang positif. 🌙",
    "Kalau bisa bikin partner senyum, itu udah sedekah namanya. 😊",
    "Satu chat baik di bulan Ramadan nilainya berlipat ganda. Manfaatin dong. 📈",
    "Jaga lisan (dan jari) ya, baik di dunia nyata maupun di sini. 🤍",
]

# ─── Would You Rather — Bank Soal ────────────────────────────────────────────
MAX_ROUNDS = 10
GAME_TIMEOUT = 600  # 10 menit

WYR_QUESTIONS = [
    ("Ketahuan kentut di lift penuh orang", "Ketahuan ngupil di meeting kantor"),
    ("Jadi orang paling ganteng/cantik tapi bau badan parah", "Biasa aja tapi wangi banget"),
    ("Hidup tanpa internet selamanya", "Hidup tanpa AC/kipas selamanya"),
    ("Makan nasi pake es krim setiap hari", "Minum kopi pake garam setiap hari"),
    ("Ketahuan stalking mantan", "Ketahuan nangis nonton film kartun"),
    ("Bisa baca pikiran orang tapi semua orang bisa baca pikiran kamu", "Tidak bisa baca pikiran siapapun"),
    ("Dikejar mantan yang creepy", "Dikejar debt collector"),
    ("Jadi orang paling lucu tapi jelek", "Jadi orang paling ganteng/cantik tapi boring"),
    ("Tidur di kasur mewah tapi selalu mimpi buruk", "Tidur di lantai tapi selalu mimpi indah"),
    ("Ketahuan gegoleran sambil dengerin lagu galau sendirian", "Ketahuan nyanyi-nyanyi sendiri di kamar mandi"),
    ("Chat duluan ke crush", "Nungguin crush chat duluan sampai kiamat"),
    ("Jadi viral karena hal memalukan", "Tidak pernah viral sama sekali"),
    ("Punya teman banyak tapi semua fake", "Punya teman sedikit tapi tulus"),
    ("Kerja keras tapi miskin", "Kerja santai tapi kaya"),
    ("Diputusin lewat chat", "Diputusin di depan umum"),
    ("Tau tanggal kematian kamu", "Tidak tau sama sekali"),
    ("Bisa terbang tapi harus telanjang", "Tidak bisa terbang tapi pakai baju"),
    ("Seumur hidup makan makanan yang sama tiap hari", "Tidak pernah makan makanan favorit lagi"),
    ("Punya pacar yang over-protektif", "Punya pacar yang cuek banget"),
    ("Ketahuan masih pakai sepatu bolong", "Ketahuan masih pakai dompet karton"),
    ("Jadi orang paling pintar tapi tidak ada yang percaya", "Jadi orang biasa tapi semua orang percaya kamu"),
    ("Hidup di dunia tanpa musik", "Hidup di dunia tanpa film"),
    ("Bisa berbicara semua bahasa tapi tidak bisa baca tulis", "Bisa baca tulis semua bahasa tapi tidak bisa ngomong"),
    ("Punya ingatan sempurna tentang semua hal buruk", "Lupa semua kenangan indah"),
    ("Ketahuan ghosting teman", "Ketahuan bohong ke orang tua"),
    ("Jadi orang kaya tapi kesepian", "Jadi orang biasa tapi dikelilingi orang yang sayang"),
    ("Tidak bisa tidur lebih dari 4 jam sehari", "Tidak bisa bangun sebelum 12 siang"),
    ("Punya mantan yang masih sering DM", "Punya mantan yang totally ghosting kamu"),
    ("Ketahuan masih takut gelap", "Ketahuan masih takut laba-laba"),
    ("Hidup tanpa sosmed selamanya", "Hidup tanpa smartphone tapi boleh sosmed di PC"),
    ("Tau semua rahasia orang sekitar kamu", "Semua orang tau semua rahasia kamu"),
    ("Dijodohin sama orang tua", "Cari jodoh sendiri tapi susah banget"),
    ("Kerja dari rumah selamanya", "Kerja di kantor selamanya"),
    ("Bisa makan apapun tanpa gemuk", "Bisa begadang tanpa mengantuk"),
    ("Ketahuan PHP-in orang", "Ketahuan di-PHP-in orang"),
    ("Punya gebetan yang tidak tau kamu ada", "Tidak punya gebetan tapi semua orang naksir kamu"),
    ("Hidup di zaman dinosaurus", "Hidup di zaman robot mengambil alih dunia"),
    ("Jadi orang paling populer tapi tidak punya privasi", "Jadi orang tidak dikenal tapi bebas banget"),
    ("Bisa waktu balik ke masa lalu tapi tidak bisa ubah apapun", "Bisa lihat masa depan tapi tidak bisa ubah apapun"),
    ("Ketahuan ngemil tengah malam sendirian", "Ketahuan rebahan seharian tanpa mandi"),
    ("Punya skill apapun tapi tidak bisa belajar hal baru", "Tidak punya skill tapi bisa belajar apapun super cepat"),
    ("Dikenal sebagai orang yang selalu telat", "Dikenal sebagai orang yang terlalu lebay"),
    ("Chat sama crush tapi typo mulu", "Tidak bisa chat sama crush sama sekali"),
    ("Bisa terbang tapi cuma setinggi 30cm dari tanah", "Tidak bisa terbang tapi bisa lari 100km/jam"),
    ("Jadi orang yang selalu salah paham", "Jadi orang yang selalu disalahpahami"),
    ("Punya teman yang suka spoiler film", "Punya teman yang suka minta rekomendasi tapi tidak pernah nonton"),
    ("Ketahuan nonton drakor sambil nangis", "Ketahuan nonton film horor sambil teriak-teriak"),
    ("Hidup tanpa kopi selamanya", "Hidup tanpa mie instan selamanya"),
    ("Bisa baca semua buku dalam semalam tapi lupa besoknya", "Baca satu buku seminggu tapi ingat selamanya"),
    ("Jadi orang yang selalu overthinking", "Jadi orang yang tidak pernah mikir panjang"),
]

CARI_PARTNER = ReplyKeyboardMarkup(
    [["🚀 Cari partner"], ["📊 Stats"]],
    resize_keyboard=True,
    input_field_placeholder="🚀 Cari partner"
)

def btn_game_invite():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Mau!", callback_data="game_accept"),
        InlineKeyboardButton("❌ Gak mau", callback_data="game_decline"),
    ]])

def btn_game_answer(round_num: int):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🅰️ Pilihan A", callback_data=f"game_answer_A_{round_num}"),
        InlineKeyboardButton("🅱️ Pilihan B", callback_data=f"game_answer_B_{round_num}"),
    ]])

def btn_game_next():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("▶️ Ronde berikutnya", callback_data="game_next"),
        InlineKeyboardButton("❌ Selesai", callback_data="game_end"),
    ]])

def btn_game_replay():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Main lagi", callback_data="game_replay"),
        InlineKeyboardButton("❌ Selesai", callback_data="game_end"),
    ]])

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
            umur TEXT DEFAULT NULL,
            total_chats INTEGER DEFAULT 0,
            total_duration REAL DEFAULT 0,
            longest_chat REAL DEFAULT 0)""",
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
    execute_turso("INSERT OR REPLACE INTO active_chats (user_id, partner_id, msg_count, started_at) VALUES (?, ?, 0, ?)", [user_id, partner_id, now])
    execute_turso("INSERT OR REPLACE INTO active_chats (user_id, partner_id, msg_count, started_at) VALUES (?, ?, 0, ?)", [partner_id, user_id, now])

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
        "started_at": float(rows[0][2]) if rows[0][2] else time.time(),
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
        execute_turso("INSERT INTO users VALUES (?, ?, ?, 0, 0, 0, NULL, NULL, NULL, 0, 0, 0)", [user_id, time.time(), referred_by])
        if referred_by:
            execute_turso("INSERT OR IGNORE INTO referrals VALUES (?, ?, ?)", [referred_by, user_id, time.time()])
    return is_new

def db_get_referral_count(user_id: int) -> int:
    rows = execute_turso("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", [user_id])
    return int(rows[0][0] or 0)

def db_get_profile(user_id: int) -> dict | None:
    rows = execute_turso(
        "SELECT gender, kota, umur, first_seen, total_chats, total_duration, longest_chat FROM users WHERE user_id = ?", [user_id]
    )
    if not rows:
        return None
    return {
        "gender":        rows[0][0],
        "kota":          rows[0][1],
        "umur":          rows[0][2],
        "first_seen":    float(rows[0][3]) if rows[0][3] else None,
        "total_chats":   int(rows[0][4] or 0),
        "total_duration": float(rows[0][5] or 0),
        "longest_chat":  float(rows[0][6] or 0),
    }

def db_update_stats(user_id: int, duration: float):
    execute_turso("""
        UPDATE users SET
            total_chats = total_chats + 1,
            total_duration = total_duration + ?,
            longest_chat = MAX(longest_chat, ?)
        WHERE user_id = ?
    """, [duration, duration, user_id])

# ─── Game DB ──────────────────────────────────────────────────────────────────
def db_create_game(user_id: int, partner_id: int, question_id: int):
    now = time.time()
    execute_turso("INSERT OR REPLACE INTO game_sessions (user_id, partner_id, question_id, answer, round, started_at) VALUES (?, ?, ?, NULL, 1, ?)", [user_id, partner_id, question_id, now])
    execute_turso("INSERT OR REPLACE INTO game_sessions (user_id, partner_id, question_id, answer, round, started_at) VALUES (?, ?, ?, NULL, 1, ?)", [partner_id, user_id, question_id, now])

def db_get_game(user_id: int) -> dict | None:
    rows = execute_turso("SELECT partner_id, question_id, answer, round, started_at FROM game_sessions WHERE user_id = ?", [user_id])
    if not rows:
        return None
    return {
        "partner_id":  int(rows[0][0]),
        "question_id": int(rows[0][1]),
        "answer":      rows[0][2],
        "round":       int(rows[0][3] or 1),
        "started_at":  float(rows[0][4]),
    }

def db_set_game_answer(user_id: int, answer: str):
    execute_turso("UPDATE game_sessions SET answer = ? WHERE user_id = ?", [answer, user_id])

def db_next_game_round(user_id: int, partner_id: int, question_id: int, round_num: int):
    execute_turso("UPDATE game_sessions SET question_id = ?, answer = NULL, round = ?, started_at = ? WHERE user_id = ?", [question_id, round_num, time.time(), user_id])
    execute_turso("UPDATE game_sessions SET question_id = ?, answer = NULL, round = ?, started_at = ? WHERE user_id = ?", [question_id, round_num, time.time(), partner_id])

def db_delete_game(user_id: int, partner_id: int):
    execute_turso("DELETE FROM game_sessions WHERE user_id = ? OR user_id = ?", [user_id, partner_id])

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
        tip = random.choice(MATCHED_MESSAGES)
        msg = (
            "✅ <b>Partner ketemu! Nikmati obrolan kalian.</b>\n\n"
            f"<b><i>{tip}</i></b>"
        )
        # Hapus ReplyKeyboard untuk user_id (dia langsung matched, belum lewat waiting)
        await context.bot.send_message(chat_id=user_id, text="🔗 Nyambung!", reply_markup=ReplyKeyboardRemove())
        await context.bot.send_message(chat_id=user_id, text=msg, parse_mode="HTML", reply_markup=btn_chat())
        await context.bot.send_message(chat_id=partner, text=msg, parse_mode="HTML", reply_markup=btn_chat())
        logger.info("Matched: %s <-> %s", user_id, partner)
    else:
        db_add_waiting(user_id, gender_pref)
        s = db_get_stats()
        online = s["chatting"] * 2 + s["waiting"]
        pref_text = f" (nyari: {gender_pref})" if gender_pref and gender_pref != "random" else ""
        await context.bot.send_message(
            chat_id=user_id,
            text=f"🔎 <i>Lagi nyariin partner buat kamu{pref_text}...</i>\nAda <b>{online}</b> orang online sekarang.\n\nKetik /stop untuk batalkan.",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove()
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

    # Hitung durasi chat
    info = db_get_chat_info(user_id)
    duration = (time.time() - info["started_at"]) if info else 0

    db_remove_chat(user_id, partner)

    # Hapus game kalau ada
    if db_get_game(user_id):
        db_delete_game(user_id, partner)
        try:
            await context.bot.send_message(chat_id=partner, text="🎮 <i>Game selesai karena partner disconnect.</i>", parse_mode="HTML")
        except TelegramError:
            pass

    # Update statistik kedua user
    db_update_stats(user_id, duration)
    db_update_stats(partner, duration)

    await context.bot.send_message(
        chat_id=user_id,
        text="👋 <i>Chat selesai.</i>",
        parse_mode="HTML"
    )
    await context.bot.send_message(
        chat_id=user_id,
        text="Makasih udah mampir! Semoga obrolannya berkesan. 🌙",
        reply_markup=btn_after_stop(partner)
    )
    await context.bot.send_message(
        chat_id=user_id,
        text="🚀 Mau cari lagi?",
        reply_markup=CARI_PARTNER
    )
    await context.bot.send_message(
        chat_id=partner,
        text="💨 <i>Partner kamu udah cabut.</i>",
        parse_mode="HTML"
    )
    await context.bot.send_message(
        chat_id=partner,
        text="Semoga obrolannya berkesan ya! 🌙",
        reply_markup=btn_after_stop(user_id)
    )
    await context.bot.send_message(
        chat_id=partner,
        text="🚀 Mau cari lagi?",
        reply_markup=CARI_PARTNER
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
                    text=f"📢 <b>Pengumuman</b>\n\n{pesan}\n\n— <b><i>owner</i></b>",
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
        await context.bot.send_message(
            chat_id=user_id,
            text="🛑 <i>Pencarian dibatalkan. Santuy.</i>",
            parse_mode="HTML",
            reply_markup=CARI_PARTNER
        )
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
            await context.bot.send_message(
                chat_id=user_id,
                text="🛑 <i>Pencarian dibatalkan. Santuy.</i>",
                parse_mode="HTML"
            )
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
            await context.bot.send_message(
                chat_id=user_id,
                text="🛑 <i>Pencarian dibatalkan. Santuy.</i>",
                parse_mode="HTML"
            )
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

    # ── Game callbacks ────────────────────────────────────────────
    if data.startswith("game_"):
        await _handle_game_callbacks(data, user_id, query, context)
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
            text="📝 Kirim pesannya sekarang (bisa multiline):\n\n<i>Ketik /cancel untuk batalkan.</i>",
            parse_mode="HTML"
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

    # Format join date
    if p["first_seen"]:
        import datetime
        join_date = datetime.datetime.fromtimestamp(p["first_seen"]).strftime("%-d %B %Y")
    else:
        join_date = "—"

    # Format total duration
    total_sec = int(p["total_duration"])
    total_str = f"{total_sec // 3600} jam {(total_sec % 3600) // 60} menit" if total_sec >= 3600 else f"{total_sec // 60} menit"

    # Format longest chat
    long_sec = int(p["longest_chat"])
    long_str = f"{long_sec // 3600} jam {(long_sec % 3600) // 60} menit" if long_sec >= 3600 else f"{long_sec // 60} menit"

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "👤 <b>Profil kamu</b>\n\n"
            f"⚧ {gender}  •  📍 {kota}  •  🎂 {umur}\n\n"
            "───────────────\n"
            "✨ <b>Statistik</b>\n\n"
            f"› 💬 Sudah ngobrol <b>{p['total_chats']}x</b>\n"
            f"› ⏱ Total waktu: <b>{total_str}</b>\n"
            f"› 🏆 Chat terlama: <b>{long_str}</b>\n"
            f"› 📅 Join sejak <b>{join_date}</b>"
        ),
        parse_mode="HTML",
        reply_markup=btn_profile_edit()
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if context.user_data.get("waiting_broadcast"):
        context.user_data["waiting_broadcast"] = False
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text="❌ Broadcast dibatalkan."
        )


async def _send_game_question(bot, user_id: int, partner_id: int, round_num: int, question_id: int):
    q = WYR_QUESTIONS[question_id]
    text = (
        f"🎮 <b>Would You Rather — Ronde {round_num}</b>\n\n"
        f"Pilih salah satu:\n\n"
        f"🅰️ {q[0]}\n\n"
        f"🅱️ {q[1]}"
    )
    await bot.send_message(chat_id=user_id, text=text, parse_mode="HTML", reply_markup=btn_game_answer(round_num))
    await bot.send_message(chat_id=partner_id, text=text, parse_mode="HTML", reply_markup=btn_game_answer(round_num))


def _get_next_question(used: list) -> int:
    """Ambil question_id yang belum pernah dipakai di sesi ini."""
    available = [i for i in range(len(WYR_QUESTIONS)) if i not in used]
    if not available:
        # Semua soal sudah dipakai, reset
        available = list(range(len(WYR_QUESTIONS)))
    return random.choice(available)


async def game(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    partner = db_get_partner(user_id)

    if not partner:
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kamu harus punya partner dulu buat main game. Pakai /find dulu!</i>",
            parse_mode="HTML"
        )
        return

    if db_get_game(user_id):
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kalian lagi ada game yang belum selesai!</i>",
            parse_mode="HTML"
        )
        return

    await context.bot.send_message(
        chat_id=user_id,
        text="📨 <i>Undangan game dikirim ke partner kamu...</i>",
        parse_mode="HTML"
    )
    await context.bot.send_message(
        chat_id=partner,
        text="🎮 <b>Partner kamu ngajak main Would You Rather!</b>\n\nMau main bareng?",
        parse_mode="HTML",
        reply_markup=btn_game_invite()
    )
    context.application.bot_data[f"game_invite_{partner}"] = user_id


async def _handle_game_callbacks(data: str, user_id: int, query, context):

    # ── Terima invite ─────────────────────────────────────────────
    if data == "game_accept":
        inviter = context.application.bot_data.get(f"game_invite_{user_id}")
        logger.info("game_accept: user_id=%s inviter=%s (type=%s)", user_id, inviter, type(inviter))
        if not inviter:
            await query.answer("Undangan sudah expired.")
            return
        context.application.bot_data.pop(f"game_invite_{user_id}", None)

        partner_of_user = db_get_partner(user_id)
        partner_of_inviter = db_get_partner(inviter)
        logger.info("game_accept: partner_of_user=%s partner_of_inviter=%s", partner_of_user, partner_of_inviter)
        if partner_of_user != inviter or partner_of_inviter != user_id:
            await query.answer("Kalian sudah tidak terhubung.")
            return

        question_id = _get_next_question([])
        session_key = f"game_used_{min(user_id, inviter)}_{max(user_id, inviter)}"
        context.application.bot_data[session_key] = [question_id]
        db_create_game(user_id, inviter, question_id)

        await query.answer("Game dimulai!")
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(chat_id=inviter, text="✅ <i>Partner setuju! Game dimulai...</i>", parse_mode="HTML")
        await _send_game_question(context.bot, user_id, inviter, 1, question_id)
        return

    # ── Tolak invite ──────────────────────────────────────────────
    if data == "game_decline":
        inviter = context.application.bot_data.get(f"game_invite_{user_id}")
        context.application.bot_data.pop(f"game_invite_{user_id}", None)
        await query.answer("Oke, gak jadi.")
        await query.edit_message_reply_markup(reply_markup=None)
        if inviter:
            await context.bot.send_message(chat_id=inviter, text="😔 <i>Partner kamu gak mau main game.</i>", parse_mode="HTML")
        return

    # ── Jawab pertanyaan ──────────────────────────────────────────
    if data.startswith("game_answer_"):
        parts = data.split("_")
        answer = parts[2]
        round_num = int(parts[3])

        game_data = db_get_game(user_id)
        if not game_data:
            await query.answer("Game sudah selesai.")
            return

        if time.time() - game_data["started_at"] > GAME_TIMEOUT:
            db_delete_game(user_id, game_data["partner_id"])
            session_key = f"game_used_{min(user_id, game_data['partner_id'])}_{max(user_id, game_data['partner_id'])}"
            context.application.bot_data.pop(session_key, None)
            await query.answer("Game timeout!")
            await query.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(chat_id=user_id, text="⏰ <i>Game selesai karena timeout.</i>", parse_mode="HTML")
            try:
                await context.bot.send_message(chat_id=game_data["partner_id"], text="⏰ <i>Game selesai karena timeout.</i>", parse_mode="HTML")
            except TelegramError:
                pass
            return

        if game_data["round"] != round_num:
            await query.answer("Ronde sudah berlalu.")
            return

        if game_data["answer"]:
            await query.answer("Kamu sudah jawab, tunggu partner! ⏳")
            return

        db_set_game_answer(user_id, answer)
        await query.answer("✅ Jawaban tersimpan! Tunggu partner...")
        await query.edit_message_reply_markup(reply_markup=None)

        # Cek partner dulu sebelum kirim notif (Bug 1 & 2 fix)
        partner_game = db_get_game(game_data["partner_id"])
        if partner_game and partner_game["answer"]:
            partner_answer = partner_game["answer"]
            q = WYR_QUESTIONS[game_data["question_id"]]
            user_choice = q[0] if answer == "A" else q[1]
            partner_choice = q[0] if partner_answer == "A" else q[1]
            result_text = "🎉 <b>Sama!</b> Kalian kompak banget haha" if answer == partner_answer else "😮 <b>Beda!</b> Seru nih, ada yang perlu dijelasin nih"
            reveal = (
                f"📊 <b>Hasil Ronde {round_num}</b>\n\n"
                f"Kamu: <b>{answer} — {user_choice}</b>\n"
                f"Partner: <b>{partner_answer} — {partner_choice}</b>\n\n"
                f"{result_text}"
            )
            max_rounds = 999 if user_id == ADMIN_ID or game_data["partner_id"] == ADMIN_ID else MAX_ROUNDS
            if round_num >= max_rounds:
                db_delete_game(user_id, game_data["partner_id"])
                session_key = f"game_used_{min(user_id, game_data['partner_id'])}_{max(user_id, game_data['partner_id'])}"
                context.application.bot_data.pop(session_key, None)
                await context.bot.send_message(chat_id=user_id, text=reveal + "\n\n🏁 <b>Game selesai!</b>", parse_mode="HTML", reply_markup=btn_game_replay())
                await context.bot.send_message(chat_id=game_data["partner_id"], text=reveal + "\n\n🏁 <b>Game selesai!</b>", parse_mode="HTML", reply_markup=btn_game_replay())
            else:
                await context.bot.send_message(chat_id=user_id, text=reveal, parse_mode="HTML", reply_markup=btn_game_next())
                await context.bot.send_message(chat_id=game_data["partner_id"], text=reveal, parse_mode="HTML", reply_markup=btn_game_next())
        else:
            await context.bot.send_message(chat_id=game_data["partner_id"], text="👀 <i>Partner sudah jawab, sekarang giliran kamu!</i>", parse_mode="HTML")
        return

    # ── Ronde berikutnya ──────────────────────────────────────────
    if data == "game_next":
        game_data = db_get_game(user_id)
        if not game_data:
            await query.answer("Game sudah selesai.")
            return
        partner_id = game_data["partner_id"]
        await query.answer("Siap!")
        await query.edit_message_reply_markup(reply_markup=None)
        context.application.bot_data[f"game_next_ready_{user_id}"] = True
        if context.application.bot_data.get(f"game_next_ready_{partner_id}"):
            context.application.bot_data.pop(f"game_next_ready_{user_id}", None)
            context.application.bot_data.pop(f"game_next_ready_{partner_id}", None)
            next_round = game_data["round"] + 1
            session_key = f"game_used_{min(user_id, partner_id)}_{max(user_id, partner_id)}"
            used = context.application.bot_data.get(session_key, [])
            question_id = _get_next_question(used)
            used.append(question_id)
            context.application.bot_data[session_key] = used
            db_next_game_round(user_id, partner_id, question_id, next_round)
            await _send_game_question(context.bot, user_id, partner_id, next_round, question_id)
        else:
            await context.bot.send_message(chat_id=partner_id, text="✅ <i>Partner sudah siap ronde berikutnya!</i>", parse_mode="HTML")
        return

    # ── Main lagi ─────────────────────────────────────────────────
    if data == "game_replay":
        partner = db_get_partner(user_id)
        if not partner:
            await query.answer("Kamu sudah tidak punya partner.")
            return
        await query.answer("Siap!")
        await query.edit_message_reply_markup(reply_markup=None)
        context.application.bot_data[f"game_replay_ready_{user_id}"] = True
        if context.application.bot_data.get(f"game_replay_ready_{partner}"):
            context.application.bot_data.pop(f"game_replay_ready_{user_id}", None)
            context.application.bot_data.pop(f"game_replay_ready_{partner}", None)
            session_key = f"game_used_{min(user_id, partner)}_{max(user_id, partner)}"
            context.application.bot_data.pop(session_key, None)
            question_id = _get_next_question([])
            context.application.bot_data[session_key] = [question_id]
            db_create_game(user_id, partner, question_id)
            await _send_game_question(context.bot, user_id, partner, 1, question_id)
        else:
            await context.bot.send_message(chat_id=partner, text="🔄 <i>Partner mau main lagi! Pencet \'Main lagi\' kalau kamu juga mau.</i>", parse_mode="HTML")
        return

    # ── Akhiri game ───────────────────────────────────────────────
    if data == "game_end":
        game_data = db_get_game(user_id)
        if game_data:
            db_delete_game(user_id, game_data["partner_id"])
            session_key = f"game_used_{min(user_id, game_data['partner_id'])}_{max(user_id, game_data['partner_id'])}"
            context.application.bot_data.pop(session_key, None)
            context.application.bot_data.pop(f'game_replay_ready_{game_data["partner_id"]}', None)
            context.application.bot_data.pop(f'game_next_ready_{game_data["partner_id"]}', None)
            try:
                await context.bot.send_message(chat_id=game_data["partner_id"], text="🎮 <i>Partner mengakhiri game.</i>", parse_mode="HTML")
            except TelegramError:
                pass
        context.application.bot_data.pop(f"game_replay_ready_{user_id}", None)
        context.application.bot_data.pop(f"game_next_ready_{user_id}", None)
        await query.answer("Game selesai!")
        await query.edit_message_reply_markup(reply_markup=None)
        return


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
    app.add_handler(CommandHandler("game", game))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(~filters.COMMAND, message))

    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
