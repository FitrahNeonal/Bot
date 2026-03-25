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
# REPORT_LIMIT dihapus — keputusan ban sekarang manual oleh admin
STREAK_LIMIT   = 3

# ─── UI ──────────────────────────────────────────────────────────────────────
FEEDBACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("📝 Beri Feedback", url="https://feedbackneo.vercel.app")]
])

# ─── Matched Messages (Ramadan Edition) ──────────────────────────────────────
MATCHED_MESSAGES = [
    # Wholesome / positif
    "Siapa tau partner ini yang paling nyambung sama kamu. 🤝",
    "Stranger hari ini, siapa tau bestie besok. 👀",
    "Awkward dua detik pertama itu normal. Terusin aja. 🫠",
    "Plot twist: partner kamu ternyata lebih seru dari ekspektasi. 🎭",
    "Kalau ga klik, ya gapapa. Masih ada /find. 🚀",
    "Semoga obrolannya lebih seru dari yang dibayangkan. 🤞",
    "Siapa tau dari sini dapet teman diskusi terbaik kamu. 💬",
    "Kalau bisa bikin partner senyum duluan, kamu menang. 😊",
    "Obrolan yang baik dimulai dari niat yang baik. 🌟",
    "Ga ada yang tau siapa di ujung sana — itu yang bikin seru. 🎲",

    # Gen z / etika chat
    "Jangan PHP-in partner, karma itu nyata. 😇",
    "Kalau mau ghosting, minimal bilang dulu. Adab dong. 👋",
    "Red flag pertama: langsung nanya nomor. Jangan. 🚩",
    "Jangan spam, nanti di-skip sebelum sempat ngobrol. 💀",
    "Yang kamu ketik itu kesan pertama. Pilih kata yang bener. ⌨️",
    "Kalau ga tau mau ngobrol apa, tanya kabar dulu. Basic tapi works. 🙂",
    "Jangan judge dari pesan pertama — orang butuh warming up. 🔥",
    "Jangan langsung aneh. Kamu ga tau siapa yang di ujung sana. 😶",
    "First impression itu penting. Jangan buang percuma. ✨",
    "Kalau partnernya asik, jangan langsung cabut setelah satu chat. 👻",
    "Bukan tempat cari pacar — tapi kalau jodoh, terserah alam. 😭",
    "Kalau vibes-nya off dari awal, skip aja. Gapapa. ✌️",
    "Ini bukan Tinder. Tapi siapa yang tau. 🤷",
    "Partner kamu random — jangan kaget kalau vibes-nya beda banget. 🎲",

    # Sedikit dewasa / singgung
    "Kalau isi kepalamu cuma satu hal, tutup app ini dulu. 🧠",
    "Orang yang beneran menarik itu ngobrolnya seru, bukan langsung modus. 😏",
    "Flirting boleh, tapi tahu batas. Beda tipis antara charming sama creepy. 💅",
    "Kalau partnernya gasuka digituin, ya berhenti. Sesederhana itu. 🙂",
    "Niat cari koneksi atau cari yang lain? Kalau yang kedua, good luck. 😅",
    "Anonim bikin berani — tapi tetep ada batasnya ya. 🫡",
    "Modus boleh, asal halus. Yang norak itu auto ilfeel. 💀",
    "Jangan sok polos kalau dari awal niatnya udah beda. 😌",
    "Chemistry itu ga bisa dipaksa. Tapi bisa diusahain. 🔥",
    "Kalau orangnya nyaman, jangan malah dibikin risih. 🤌",
    "Kata-kata bisa bikin orang betah atau kabur. Pilih dengan bijak. 🗣️",
    "Kalau ngerasa nyambung, jangan langsung overthinking. Nikmatin aja. 🌊",
    "Deep talk sama stranger kadang lebih jujur dari sama orang terdekat. 🤫",
    "Kalau mau flirt, minimal baca situasi dulu. Jangan asal tembak. 🎯",
    "Ada yang nyari koneksi, ada yang nyari yang lain. Bedain dong. 🙃",
]

# ─── Would You Rather — Bank Soal ────────────────────────────────────────────
MAX_ROUNDS = 10
GAME_TIMEOUT = 600  # 10 menit

WYR_QUESTIONS = [
    # Situationship / hubungan modern
    ("Punya situationship yang ga jelas statusnya tapi exciting", "Punya hubungan yang jelas statusnya tapi boring banget"),
    ("Jadi dry texter tapi orang tetap kejar-kejar kamu", "Bales chat cepet tapi sering dianggap terlalu available"),
    ("Dighosting orang yang kamu suka tanpa penjelasan", "Dijelasin alasannya tapi alasannya nyakitin banget"),
    ("Punya mantan yang sekarang jadi bestie kamu", "Putus dan benar-benar tidak ada kontak sama sekali selamanya"),
    ("Jadi orang yang selalu kasih mixed signals tanpa sadar", "Selalu salah baca mixed signals orang lain"),
    ("Ketahuan masih stalking mantan sama mantan itu sendiri", "Ketahuan masih stalking mantan sama pacar baru kamu"),
    ("Punya situationship berbulan-bulan yang tiba-tiba menghilang", "Ditolak terang-terangan di awal tapi dengan sopan"),
    ("Suka sama orang yang cuma ada pas dia butuh kamu", "Suka sama orang yang terlalu available sampai bikin sesak"),
    ("Jadi yang selalu DM duluan tapi ga pernah di-DM duluan", "Selalu ditunggu tapi ga pernah ada yang berani DM kamu duluan"),
    ("Punya teman yang jadi gebetan tapi takut rusakin pertemanan", "Ga pernah punya perasaan ke siapapun karena takut kecewa"),

    # Gen Z & teknologi
    ("Hidup tanpa AI selamanya di era sekarang", "Semua keputusan hidupmu ditentukan AI"),
    ("Jadi content creator tapi isinya ga ada yang kamu suka", "Jadi penonton setia tapi ga pernah dikenal siapapun"),
    ("Viral sekali tapi videonya memalukan", "Posting konsisten 2 tahun tapi ga pernah dilirik"),
    ("Punya 1 juta followers tapi semua orang benci kamu", "Punya 100 followers yang beneran sayang kamu"),
    ("Hidup di era tanpa meme", "Hidup di era tanpa musik digital"),
    ("Semua DM kamu terbaca orang tua kamu", "Semua story kamu dilihat mantan kamu"),
    ("Chat kamu sama crush dibaca semua teman-temanmu", "Chat kamu sama teman-temanmu dibaca crush kamu"),
    ("Jadi orang yang FYP-nya selalu relevan tapi hidup terasa hampa", "Jadi orang yang FYP-nya kacau tapi hidupnya purposeful"),
    ("Hidup tanpa earphone seumur hidup", "Hidup tanpa kamera HP seumur hidup"),
    ("Semua orang bisa lihat screen time kamu", "Semua orang bisa lihat riwayat pencarian kamu"),

    # Indonesia banget
    ("Ditanya kapan nikah di setiap lebaran seumur hidup", "Ditanya kapan punya kerja tetap di setiap kumpul keluarga"),
    ("Kos di Jakarta dengan gaji UMR", "Pulang kampung tapi ga ada lapangan kerja"),
    ("Jadi yang paling sukses di keluarga tapi ga bisa hidup tenang", "Hidup tenang tapi selalu dibanding-bandingin sama yang lebih sukses"),
    ("Macet 3 jam tiap hari tapi kerja di kota impian", "Ga pernah macet tapi kerja di kota yang kamu ga suka"),
    ("Kuliah di kampus bagus tapi salah jurusan", "Kuliah di jurusan yang kamu suka tapi kampusnya biasa aja"),
    ("Kerja sesuai passion tapi gaji kecil", "Kerja ga sesuai passion tapi gaji besar"),
    ("Punya tabungan banyak tapi ga bisa nikmatin hidup", "Selalu nikmatin hidup tapi tabungan selalu nol"),
    ("Jadi anak pertama dengan semua tanggung jawabnya", "Jadi anak bungsu dengan semua manjanya"),

    # FOMO & sosial
    ("Tahu semua gossip tapi ga bisa cerita ke siapapun", "Ga tau gossip apapun tapi semua orang cerita ke kamu"),
    ("Ikut acara yang ga kamu mau karena FOMO", "Skip semua acara tapi selalu nyesel lihat story orang lain"),
    ("Jadi orang yang selalu ada di setiap momen teman tapi kelelahan", "Jadi orang yang sering absen tapi selalu dirinduin"),
    ("Punya teman grup yang toxic tapi seru", "Ga punya grup teman tapi ga ada drama"),
    ("Diundang ke semua acara tapi ga pernah bisa hadir", "Ga pernah diundang tapi selalu bisa hadir kalau diundang"),
    ("Jadi orang yang ceritanya selalu di-one up orang lain", "Jadi orang yang selalu dengerin cerita orang tanpa bisa cerita sendiri"),

    # Pilihan hidup absurd baru
    ("Bisa skip antrian di mana saja tapi harus pakai kostum badut", "Ga bisa skip antrian tapi selalu ketemu orang seru waktu antri"),
    ("Semua makanan yang kamu makan rasanya sama tapi enak", "Bisa makan semua makanan tapi ga pernah kenyang"),
    ("Bisa bicara sama hewan tapi mereka selalu jujur dan kadang nyakitin", "Ga bisa bicara sama hewan tapi mereka selalu suka kamu"),
    ("Cuaca selalu sempurna tapi kamu ga bisa keluar rumah", "Bebas kemana saja tapi cuacanya selalu ga enak"),
    ("Bisa terbang tapi cuma waktu orang lain lagi tidur", "Ga bisa terbang tapi bisa teleport ke tempat yang pernah kamu datangi"),
    ("Punya uang tak terbatas tapi di kota yang kamu ga suka", "Hidup di kota impian tapi selalu kekurangan uang"),
    ("Hidup di dunia tanpa senin", "Hidup di dunia tanpa jumat"),
    ("Waktu tidur kamu terasa 10 menit padahal 8 jam", "Tidur 8 jam tapi terasa 3 hari"),

    # Dilemma sehari-hari Gen Z
    ("Typo nama orang waktu mention di grup besar", "Salah kirim pesan ke orang yang salah"),
    ("Ketahuan skip story orang padahal tadi liat", "Ketahuan liat story orang berkali-kali dan orang itu notice"),
    ("Baca chat orang tapi notif-nya keburu muncul di layar kunci", "Ketahuan udah baca tapi ga bales-bales"),
    ("Ketawa ga tepat waktu di situasi serius", "Muka datar di situasi yang harusnya bahagia"),
    ("Selalu jadi tempat curhat tapi ga ada yang nanya balik kabarmu", "Ga ada yang curhat ke kamu karena dianggap terlalu sibuk"),
    ("Ketahuan pake filter tebal waktu video call mendadak", "Keliatan bete padahal sebenernya lagi serius mikir"),

    # Spicy tapi halus
    ("Jatuh buat seseorang yang kamu kenal dari chat doang, ga pernah ketemu", "Ga bisa suka sama orang yang belum ketemu langsung"),
    ("Punya tension sama seseorang tapi ga ada yang berani mulai duluan", "Langsung jujur tapi ternyata salah baca situasi total"),
    ("Orang yang bikin kamu berdebar tapi jelas ga baik buat kamu", "Orang yang baik banget buat kamu tapi ga ada chemistry sama sekali"),
    ("Suka orang yang mysterious dan susah ditebak", "Suka orang yang terbuka dan predictable"),
    ("Disukai orang yang ga kamu suka tapi mereka sempurna di atas kertas", "Menyukai orang yang kurang sempurna tapi bikin nyaman"),
    ("Ketemu orang yang nyambung banget tapi kalian ga akan pernah bisa bersama", "Ga pernah nemu orang yang beneran nyambung sama kamu"),
    ("Jadi orang yang terlalu gampang attach sama kebaikan orang", "Jadi orang yang susah attach meskipun orang itu tulus"),
    ("Punya one night deep conversation yang kamu inget seumur hidup", "Punya hubungan panjang tapi ga ada satu momen pun yang memorable"),
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

def btn_notify_opt_in():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔔 Iya, kabarin aku", callback_data="notify_yes"),
        InlineKeyboardButton("🚫 Gak usah", callback_data="notify_no"),
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
        """CREATE TABLE IF NOT EXISTS notify_list (
            user_id  INTEGER PRIMARY KEY,
            added_at REAL NOT NULL
        )""",
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
        """CREATE TABLE IF NOT EXISTS game_sessions (
            user_id INTEGER PRIMARY KEY,
            partner_id INTEGER NOT NULL,
            question_id INTEGER NOT NULL,
            answer TEXT DEFAULT NULL,
            round INTEGER DEFAULT 1,
            started_at REAL NOT NULL)""",
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
        rows = execute_turso("""
            SELECT w.user_id FROM waiting_users w
            JOIN users u ON w.user_id = u.user_id
            WHERE w.user_id != ?
            AND u.gender = ?
            AND w.user_id NOT IN (SELECT user_id FROM active_chats)
            ORDER BY w.joined_at LIMIT 1
        """, [exclude, gender_pref])
    else:
        rows = execute_turso("""
            SELECT user_id FROM waiting_users
            WHERE user_id != ?
            AND user_id NOT IN (SELECT user_id FROM active_chats)
            ORDER BY joined_at LIMIT 1
        """, [exclude])
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
            longest_chat = CASE WHEN ? > longest_chat THEN ? ELSE longest_chat END
        WHERE user_id = ?
    """, [duration, duration, duration, user_id])

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

def db_add_notify(user_id: int):
    execute_turso(
        "INSERT OR REPLACE INTO notify_list VALUES (?, ?)",
        [user_id, time.time()]
    )

def db_pop_notify_all(ttl: int = 10800) -> list:
    cutoff = time.time() - ttl
    execute_turso("DELETE FROM notify_list WHERE added_at < ?", [cutoff])
    rows = execute_turso("SELECT user_id FROM notify_list")
    execute_turso("DELETE FROM notify_list")
    return [int(r[0]) for r in rows]

def db_clear_reconnect(user_a: int, user_b: int):
    execute_turso(
        "DELETE FROM reconnect_requests WHERE (user_id = ? AND partner_id = ?) OR (user_id = ? AND partner_id = ?)",
        [user_a, user_b, user_b, user_a]
    )

# ─── Locks ───────────────────────────────────────────────────────────────────
match_lock = asyncio.Lock()

# ─── Chat Log (in-memory, maks 5 pesan terakhir per user) ────────────────────
# Format: {user_id: [{"type": "text"/"photo"/etc, "text": "...", "from": user_id}]}
chat_log: dict[int, list] = {}
CHAT_LOG_MAX = 5

def log_message(user_id: int, msg_type: str, content: str):
    if user_id not in chat_log:
        chat_log[user_id] = []
    chat_log[user_id].append({"type": msg_type, "content": content})
    if len(chat_log[user_id]) > CHAT_LOG_MAX:
        chat_log[user_id].pop(0)

def get_log(user_id: int) -> list:
    return chat_log.get(user_id, [])

def clear_log(user_id: int):
    chat_log.pop(user_id, None)

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
    clear_log(user_id)
    clear_log(partner_id)
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

        # Ping notify list
        to_notify = db_pop_notify_all()
        for notify_id in to_notify:
            if notify_id == user_id:
                continue
            if db_is_waiting(notify_id):
                continue
            try:
                await context.bot.send_message(
                    chat_id=notify_id,
                    text="👋 <i>Ada yang lagi nyari partner nih! Mau /find lagi?</i>",
                    parse_mode="HTML"
                )
            except TelegramError:
                pass

        s = db_get_stats()
        online = s["chatting"] * 2 + s["waiting"]
        pref_text = f" (nyari: {gender_pref})" if gender_pref and gender_pref != "random" else ""
        await context.bot.send_message(
            chat_id=user_id,
            text=f"🔎 <i>Lagi scanning{pref_text}... bentar lagi ketemu nih.</i>\n<b>{online}</b> orang online sekarang.\n\nKetik /stop untuk batalkan.\n💡 <i>Mau dinotif kalau ada yang nyari? Batalkan dulu lalu pilih 🔔</i>",
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
        clear_log(user_id)
        clear_log(partner)
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
    clear_log(user_id)
    clear_log(partner)

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
        text="Makasih udah mampir! Semoga ada yang nyantol dari obrolannya. 🎭",
        reply_markup=btn_after_stop(partner)
    )
    await context.bot.send_message(
        chat_id=user_id,
        text="🚀 Mau cari lagi?",
        reply_markup=CARI_PARTNER
    )
    await context.bot.send_message(
        chat_id=partner,
        text="💨 <i>Partner cabut duluan. Ghosted in real time 💀</i>",
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

    s = db_get_stats()

    if is_new:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "🎭 <b>Anonyneo</b>\n\n"
                "Ngobrol anonim sama stranger. Ga ada nama, ga ada akun, ga ada jejak.\n"
                "Cuma kamu, dia, dan obrolan yang (mungkin) lebih jujur dari biasanya.\n\n"
                f"👥 <b>{s['total']}</b> orang udah nyobain.\n"
                f"💬 <b>{s['chatting']}</b> pasang lagi ngobrol sekarang.\n\n"
                "Siap? Ketik /find buat langsung nyari partner. 🚀"
            ),
            parse_mode="HTML",
            reply_markup=CARI_PARTNER
        )
    else:
        profile = db_get_profile(user_id)
        total_chats = profile["total_chats"] if profile else 0

        if total_chats == 0:
            comeback_text = "Belum sempet ngobrol sama siapa-siapa nih. Yuk cobain sekarang! 👀"
        elif total_chats < 5:
            comeback_text = f"Udah {total_chats}x ngobrol sama stranger. Baru pemanasan nih. 🔥"
        elif total_chats < 20:
            comeback_text = f"<b>{total_chats} obrolan</b> dan balik lagi — kayaknya ketagihan. 😏"
        else:
            comeback_text = f"<b>{total_chats} obrolan</b>?? Kamu ini reguler sejati. 🏆"

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                f"👋 <b>Eh, balik lagi!</b>\n\n"
                f"{comeback_text}\n\n"
                f"💬 <b>{s['chatting']}</b> pasang lagi ngobrol sekarang.\n"
                f"🔎 <b>{s['waiting']}</b> orang lagi nunggu partner.\n\n"
                "Langsung /find aja? 🚀"
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

    # Handle forward bukti report
    if context.user_data.get("waiting_evidence"):
        pending = context.user_data.get("pending_report")
        if not pending:
            context.user_data["waiting_evidence"] = False
            return
        if "evidence_msgs" not in context.user_data:
            context.user_data["evidence_msgs"] = []
        context.user_data["evidence_msgs"].append(update.message.message_id)
        count = len(context.user_data["evidence_msgs"])
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ <i>{count} pesan diterima. Mau kirim lagi atau udah?</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Selesai kirim bukti", callback_data="report_done_evidence"),
            ]])
        )
        return

    partner = db_get_partner(user_id)

    if not partner:
        await context.bot.send_message(
            chat_id=user_id,
            text="<i>Lah, ngobrol sama siapa? /find dulu dong 👀</i>",
            parse_mode="HTML"
        )
        return

    db_increment_msg(user_id)

    # Log pesan untuk keperluan report
    msg = update.message
    if msg.text:
        log_message(user_id, "text", msg.text)
    elif msg.photo:
        log_message(user_id, "foto", "[foto]")
    elif msg.sticker:
        log_message(user_id, "stiker", msg.sticker.emoji or "[stiker]")
    elif msg.voice:
        log_message(user_id, "voice", "[pesan suara]")
    elif msg.video:
        log_message(user_id, "video", "[video]")
    elif msg.document:
        log_message(user_id, "file", f"[file: {msg.document.file_name or '?'}]")
    else:
        log_message(user_id, "lainnya", "[pesan]")

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
        await context.bot.send_message(
            chat_id=user_id,
            text="🔔 <i>Mau aku kabarin kalau ada yang lagi nyari partner?</i>",
            parse_mode="HTML",
            reply_markup=btn_notify_opt_in()
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

    if data == "notify_yes":
        db_add_notify(user_id)
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ <i>Siap! Aku kabarin kalau ada yang nyari.</i>",
            parse_mode="HTML"
        )
        return

    if data == "notify_no":
        await context.bot.send_message(
            chat_id=user_id,
            text="👍 <i>Oke, santuy.</i>",
            parse_mode="HTML"
        )
        return

    if data == "cancel_find":
        if db_is_waiting(user_id):
            db_remove_waiting(user_id)
            await context.bot.send_message(
                chat_id=user_id,
                text="🛑 <i>Pencarian dibatalkan. Santuy.</i>",
                parse_mode="HTML"
            )
            await context.bot.send_message(
                chat_id=user_id,
                text="🔔 <i>Mau aku kabarin kalau ada yang lagi nyari partner?</i>",
                parse_mode="HTML",
                reply_markup=btn_notify_opt_in()
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
        # Simpan pending report, tawarin forward bukti
        context.user_data["pending_report"] = {"reported_id": reported_id, "reason": reason}
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ <i>Laporan dikirim. Makasih udah lapor!</i>\n\nMau kasih bukti percakapan ke admin?",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📨 Forward bukti", callback_data="report_send_evidence"),
                InlineKeyboardButton("⏭ Skip", callback_data="report_skip_evidence"),
            ]])
        )
        return

    if data in ("report_spam", "report_sange"):
        partner = db_get_partner(user_id)
        if not partner:
            await context.bot.send_message(chat_id=user_id, text="⚠️ <i>Kamu sudah tidak punya partner.</i>", parse_mode="HTML")
            return
        reason = "spam" if data == "report_spam" else "sange"
        context.user_data["pending_report"] = {"reported_id": partner, "reason": reason}
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ <i>Laporan dikirim. Makasih udah lapor!</i>\n\nMau kasih bukti percakapan ke admin?",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📨 Forward bukti", callback_data="report_send_evidence"),
                InlineKeyboardButton("⏭ Skip", callback_data="report_skip_evidence"),
            ]])
        )
        return

    if data == "report_send_evidence":
        pending = context.user_data.get("pending_report")
        if not pending:
            await context.bot.send_message(chat_id=user_id, text="⚠️ <i>Sesi laporan sudah tidak aktif.</i>", parse_mode="HTML")
            return
        if context.user_data.get("waiting_evidence"):
            # Sudah aktif, jangan reset — cukup ingatkan
            await context.bot.send_message(
                chat_id=user_id,
                text="📨 <i>Kamu sudah dalam mode kirim bukti. Forward pesan atau ketik /done.</i>",
                parse_mode="HTML"
            )
            return
        context.user_data["waiting_evidence"] = True
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "📨 <b>Cara kirim bukti:</b>\n\n"
                "Kirim screenshot atau pesan apapun sebagai bukti ke bot ini.\n\n"
                "Bisa kirim berapa aja. Kalau udah, ketik /done atau pencet tombol selesai.\n\n"
                "Bingung? Ya udah gapapa, ketik /done aja langsung. Dasar oon 🙃"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Batalkan", callback_data="report_cancel_evidence"),
            ]])
        )
        return

    if data == "report_done_evidence":
        if not context.user_data.get("waiting_evidence"):
            return
        await _send_report_with_evidence(user_id, context)
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ <i>Bukti dikirim ke admin. Makasih udah lapor!</i>",
            parse_mode="HTML"
        )
        return

    if data == "report_cancel_evidence":
        pending = context.user_data.get("pending_report")
        context.user_data["waiting_evidence"] = False
        context.user_data.pop("pending_report", None)
        context.user_data.pop("evidence_msgs", None)
        if pending:
            reported_id   = pending["reported_id"]
            reason        = pending["reason"]
            total_reports = db_add_report(user_id, reported_id, reason)
            logs          = get_log(reported_id)
            log_text      = "\n".join([f"  [{m['type']}] {m['content']}" for m in logs]) if logs else "  (tidak ada log)"
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=(
                        f"🚩 <b>Report Baru</b>\n\n"
                        f"👤 Pelapor: <code>{user_id}</code>\n"
                        f"🎯 Dilaporkan: <code>{reported_id}</code>\n"
                        f"📌 Alasan: <b>{reason}</b>\n"
                        f"📊 Total report: <b>{total_reports}</b>\n\n"
                        f"📋 <b>5 pesan terakhir si dilaporkan:</b>\n{log_text}\n\nAksi:"
                    ),
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🚫 Ban", callback_data=f"admin_ban_{reported_id}"),
                        InlineKeyboardButton("✅ Abaikan", callback_data=f"admin_ignore_{reported_id}"),
                    ]])
                )
            except TelegramError:
                pass
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ <i>Laporan terkirim tanpa bukti. Makasih!</i>",
            parse_mode="HTML"
        )
        return

    if data == "report_skip_evidence":
        pending = context.user_data.get("pending_report")
        if not pending:
            return
        reported_id = pending["reported_id"]
        reason      = pending["reason"]
        context.user_data.pop("pending_report", None)
        total_reports = db_add_report(user_id, reported_id, reason)
        logger.info("Report: %s melaporkan %s (%s) — total: %d", user_id, reported_id, reason, total_reports)
        # Kirim log otomatis ke admin
        logs = get_log(reported_id)
        log_text = "\n".join([f"  [{m['type']}] {m['content']}" for m in logs]) if logs else "  (tidak ada log)"
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"🚩 <b>Report Baru</b>\n\n"
                    f"👤 Pelapor: <code>{user_id}</code>\n"
                    f"🎯 Dilaporkan: <code>{reported_id}</code>\n"
                    f"📌 Alasan: <b>{reason}</b>\n"
                    f"📊 Total report: <b>{total_reports}</b>\n\n"
                    f"📋 <b>5 pesan terakhir si dilaporkan:</b>\n{log_text}\n\nAksi:"
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🚫 Ban", callback_data=f"admin_ban_{reported_id}"),
                    InlineKeyboardButton("✅ Abaikan", callback_data=f"admin_ignore_{reported_id}"),
                ]])
            )
        except TelegramError:
            pass
        return

    if data.startswith("admin_ban_"):
        if user_id != ADMIN_ID:
            return
        try:
            target_id = int(data.split("admin_ban_")[1])
        except (IndexError, ValueError):
            return
        db_ban_user(target_id)
        # Putuskan dari chat kalau lagi aktif
        partner = db_get_partner(target_id)
        if partner:
            db_remove_chat(target_id, partner)
            clear_log(target_id)
            clear_log(partner)
            try:
                await context.bot.send_message(chat_id=partner, text="⚠️ <i>Partner kamu diputus karena melanggar aturan.</i>", parse_mode="HTML", reply_markup=btn_find_again())
            except TelegramError:
                pass
        try:
            await context.bot.send_message(chat_id=target_id, text="🚫 <i>Akun kamu telah di-ban karena melanggar aturan.</i>", parse_mode="HTML")
        except TelegramError:
            pass
        try:
            await query.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(chat_id=ADMIN_ID, text=f"✅ User <code>{target_id}</code> berhasil di-ban.", parse_mode="HTML")
        except TelegramError:
            pass
        return

    if data.startswith("admin_ignore_"):
        if user_id != ADMIN_ID:
            return
        try:
            target_id = int(data.split("admin_ignore_")[1])
        except (IndexError, ValueError):
            return
        try:
            await query.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(chat_id=ADMIN_ID, text=f"👍 Report untuk <code>{target_id}</code> diabaikan.", parse_mode="HTML")
        except TelegramError:
            pass
        return

    if data.startswith("admin_unban_"):
        if user_id != ADMIN_ID:
            return
        try:
            target_id = int(data.split("admin_unban_")[1])
        except (IndexError, ValueError):
            return
        execute_turso("UPDATE users SET banned = 0 WHERE user_id = ?", [target_id])
        try:
            await query.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(chat_id=ADMIN_ID, text=f"✅ User <code>{target_id}</code> berhasil di-unban.", parse_mode="HTML")
            await context.bot.send_message(chat_id=target_id, text="✅ <i>Akun kamu sudah di-unban. Selamat datang kembali!</i>", parse_mode="HTML")
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
            "🎭 <b>Anonyneo — panduan</b>\n\n"
            "🚀 /find — cari partner random, langsung match\n"
            "⚧️ /findgender — pilih dulu mau ketemu cowok atau cewek\n"
            "⏭ /next — skip partner sekarang, cari yang baru\n"
            "🛑 /stop — akhiri chat\n"
            "🎮 /game — main Would You Rather bareng partner\n"
            "👤 /profile — lihat profil + statistik chat kamu\n"
            "📊 /stats — cek berapa orang lagi online\n"
            "🔗 /invite — dapetin link buat ajak temen\n\n"
            "<b>Cara mulai:</b>\n"
            "1. Ketik /find\n"
            "2. Tunggu partner ketemu (biasanya cepet)\n"
            "3. Langsung kirim pesan aja — identitas kamu aman, anonim total\n"
            "4. Kalau mau ganti partner, pencet tombol ⏭ Skip\n\n"
            "<b>Setelah chat selesai:</b>\n"
            "Ada tombol 🔄 Hubungkan lagi — kalau mau balik ke partner yang sama, dua-duanya harus pencet. Berlaku 6 jam.\n\n"
            "⚠️ Spam, konten dewasa, atau nyebarin info pribadi orang → langsung report. 3 report = auto-banned.\n\n"
            "———\n"
            "🐛 Ada bug atau pertanyaan?\n"
            "Join channel → @anonyneo\n"
            "Atau cek bio bot buat kontak owner langsung."
        ),
        parse_mode="HTML"
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
                "📊 /admin stats — statistik lengkap\n"
                "👥 /admin users — 20 user terbaru\n"
                "🚩 /admin reports — daftar report pending\n"
                "🚫 /admin banned — daftar user banned\n"
                "🔓 /admin unban &lt;user_id&gt; — unban user\n"
                "📢 /admin broadcast — kirim pesan ke semua user"
            ),
            parse_mode="HTML"
        )
        return

    cmd = args[0].lower()

    if cmd == "stats":
        try:
            import datetime
            s             = db_get_stats()
            total_refs    = int(execute_turso("SELECT COUNT(*) FROM referrals")[0][0] or 0)
            total_reports = int(execute_turso("SELECT COUNT(*) FROM reports")[0][0] or 0)
            total_banned  = int(execute_turso("SELECT COUNT(*) FROM users WHERE banned = 1")[0][0] or 0)
            notify_count  = int(execute_turso("SELECT COUNT(*) FROM notify_list")[0][0] or 0)

            # User join 7 hari terakhir
            week_ago = time.time() - 604800
            new_users = int(execute_turso("SELECT COUNT(*) FROM users WHERE first_seen > ?", [week_ago])[0][0] or 0)

            # Gender stats
            gender_rows = execute_turso("SELECT gender, COUNT(*) FROM users WHERE gender IS NOT NULL GROUP BY gender ORDER BY 2 DESC")
            gender_text = "  " + "  |  ".join([f"{r[0]}: <b>{r[1]}</b>" for r in gender_rows]) if gender_rows else "  belum ada data"

            # Top kota
            kota_rows = execute_turso("SELECT kota, COUNT(*) as c FROM users WHERE kota IS NOT NULL GROUP BY kota ORDER BY c DESC LIMIT 5")
            kota_text = "\n".join([f"  {i+1}. {r[0]} — <b>{r[1]}</b>" for i, r in enumerate(kota_rows)]) or "  belum ada data"

            # Top umur
            umur_rows = execute_turso("SELECT umur, COUNT(*) as c FROM users WHERE umur IS NOT NULL GROUP BY umur ORDER BY c DESC")
            umur_text = "  " + "  |  ".join([f"{r[0]}: <b>{r[1]}</b>" for r in umur_rows]) if umur_rows else "  belum ada data"

            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    "📊 <b>Stats</b>\n"
                    "──────────────────\n"
                    f"👥 Total user: <b>{s['total']}</b>  (+{new_users} minggu ini)\n"
                    f"💬 Lagi chat: <b>{s['chatting']}</b> pasang\n"
                    f"🔎 Lagi waiting: <b>{s['waiting']}</b> orang\n"
                    f"🔔 Notify list: <b>{notify_count}</b> orang\n"
                    "──────────────────\n"
                    f"🔗 Total referral: <b>{total_refs}</b>\n"
                    f"🚩 Total report: <b>{total_reports}</b>\n"
                    f"🚫 Total banned: <b>{total_banned}</b>\n"
                    "──────────────────\n"
                    f"⚧ <b>Gender:</b>\n{gender_text}\n\n"
                    f"🎂 <b>Umur:</b>\n{umur_text}\n\n"
                    f"🗺️ <b>Top kota:</b>\n{kota_text}"
                ),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error("admin stats error: %s", e)
            await context.bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ Gagal ambil stats: <code>{e}</code>", parse_mode="HTML")

    elif cmd == "users":
        import datetime
        rows = execute_turso(
            "SELECT user_id, first_seen, referred_by, total_chats FROM users ORDER BY first_seen DESC LIMIT 20"
        )
        if not rows:
            await context.bot.send_message(chat_id=ADMIN_ID, text="Belum ada user.")
            return
        lines = ["👥 <b>20 User Terbaru</b>\n"]
        for uid, first_seen, referred_by, total_chats in rows:
            try:
                tgl = datetime.datetime.fromtimestamp(float(first_seen)).strftime("%d/%m %H:%M")
            except (TypeError, ValueError):
                tgl = "?"
            ref  = f" · ref: {referred_by}" if referred_by else ""
            chat = f" · {total_chats}x chat" if total_chats else ""
            lines.append(f"• <code>{uid}</code>{ref}{chat} — {tgl}")
        await context.bot.send_message(chat_id=ADMIN_ID, text="\n".join(lines), parse_mode="HTML")

    elif cmd == "reports":
        rows = execute_turso(
            "SELECT reported_id, COUNT(*) as c, GROUP_CONCAT(DISTINCT reason) FROM reports GROUP BY reported_id ORDER BY c DESC LIMIT 20"
        )
        if not rows:
            await context.bot.send_message(chat_id=ADMIN_ID, text="✅ Tidak ada report.")
            return
        lines = ["🚩 <b>Report Pending</b>\n"]
        for reported_id, count, reasons in rows:
            is_banned = db_is_banned(int(reported_id))
            status = " 🚫 <i>(banned)</i>" if is_banned else ""
            lines.append(f"• <code>{reported_id}</code> — <b>{count}x</b> ({reasons}){status}")
        await context.bot.send_message(chat_id=ADMIN_ID, text="\n".join(lines), parse_mode="HTML")

    elif cmd == "banned":
        rows = execute_turso("SELECT user_id FROM users WHERE banned = 1")
        if not rows:
            await context.bot.send_message(chat_id=ADMIN_ID, text="✅ Tidak ada user yang di-ban.")
            return
        lines = ["🚫 <b>Banned Users</b>\n"]
        for (uid,) in rows:
            lines.append(
                f"• <code>{uid}</code>"
            )
        lines.append("\n<i>Gunakan /admin unban &lt;user_id&gt; untuk unban.</i>")
        await context.bot.send_message(chat_id=ADMIN_ID, text="\n".join(lines), parse_mode="HTML")

    elif cmd == "unban":
        if len(args) < 2:
            await context.bot.send_message(chat_id=ADMIN_ID, text="⚠️ Format: /admin unban &lt;user_id&gt;", parse_mode="HTML")
            return
        try:
            target_id = int(args[1])
        except ValueError:
            await context.bot.send_message(chat_id=ADMIN_ID, text="⚠️ user_id harus angka.")
            return
        execute_turso("UPDATE users SET banned = 0 WHERE user_id = ?", [target_id])
        try:
            await context.bot.send_message(chat_id=target_id, text="✅ <i>Akun kamu sudah di-unban. Selamat datang kembali!</i>", parse_mode="HTML")
        except TelegramError:
            pass
        await context.bot.send_message(chat_id=ADMIN_ID, text=f"✅ User <code>{target_id}</code> berhasil di-unban.", parse_mode="HTML")

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
    if total_sec == 0:
        total_str = "—"
    elif total_sec >= 3600:
        total_str = f"{total_sec // 3600} jam {(total_sec % 3600) // 60} menit"
    else:
        total_str = f"{total_sec // 60} menit"

    # Format longest chat
    long_sec = int(p["longest_chat"])
    if long_sec == 0:
        long_str = "—"
    elif long_sec >= 3600:
        long_str = f"{long_sec // 3600} jam {(long_sec % 3600) // 60} menit"
    else:
        long_str = f"{long_sec // 60} menit"

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
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(chat_id=inviter, text="✅ <i>Partner setuju! Game dimulai...</i>", parse_mode="HTML")
        await _send_game_question(context.bot, user_id, inviter, 1, question_id)
        return

    # ── Tolak invite ──────────────────────────────────────────────
    if data == "game_decline":
        inviter = context.application.bot_data.get(f"game_invite_{user_id}")
        context.application.bot_data.pop(f"game_invite_{user_id}", None)
        await query.answer("Oke, gak jadi.")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
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
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
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
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Cek partner dulu sebelum kirim notif (Bug 1 & 2 fix)
        partner_game = db_get_game(game_data["partner_id"])
        if partner_game and partner_game["answer"]:
            partner_answer = partner_game["answer"]
            q = WYR_QUESTIONS[game_data["question_id"]]

            # Perspektif user yang jawab duluan
            user_choice = q[0] if answer == "A" else q[1]
            partner_choice = q[0] if partner_answer == "A" else q[1]
            result_text = "🎉 <b>Sama!</b> Kalian kompak banget haha" if answer == partner_answer else "😮 <b>Beda!</b> Seru nih, ada yang perlu dijelasin nih"

            reveal_for_user = (
                f"📊 <b>Hasil Ronde {round_num}</b>\n\n"
                f"Kamu: <b>{answer} — {user_choice}</b>\n"
                f"Partner: <b>{partner_answer} — {partner_choice}</b>\n\n"
                f"{result_text}"
            )

            # Perspektif partner — Kamu dan Partner dibalik
            reveal_for_partner = (
                f"📊 <b>Hasil Ronde {round_num}</b>\n\n"
                f"Kamu: <b>{partner_answer} — {partner_choice}</b>\n"
                f"Partner: <b>{answer} — {user_choice}</b>\n\n"
                f"{result_text}"
            )

            max_rounds = 999 if user_id == ADMIN_ID or game_data["partner_id"] == ADMIN_ID else MAX_ROUNDS
            if round_num >= max_rounds:
                db_delete_game(user_id, game_data["partner_id"])
                session_key = f"game_used_{min(user_id, game_data['partner_id'])}_{max(user_id, game_data['partner_id'])}"
                context.application.bot_data.pop(session_key, None)
                await context.bot.send_message(chat_id=user_id, text=reveal_for_user + "\n\n🏁 <b>Game selesai!</b>", parse_mode="HTML", reply_markup=btn_game_replay())
                await context.bot.send_message(chat_id=game_data["partner_id"], text=reveal_for_partner + "\n\n🏁 <b>Game selesai!</b>", parse_mode="HTML", reply_markup=btn_game_replay())
            else:
                await context.bot.send_message(chat_id=user_id, text=reveal_for_user, parse_mode="HTML", reply_markup=btn_game_next())
                await context.bot.send_message(chat_id=game_data["partner_id"], text=reveal_for_partner, parse_mode="HTML", reply_markup=btn_game_next())
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
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
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
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
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
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return


async def _send_report_with_evidence(user_id: int, context, update=None):
    pending = context.user_data.get("pending_report")
    if not pending:
        return
    reported_id   = pending["reported_id"]
    reason        = pending["reason"]
    evidence_msgs = context.user_data.get("evidence_msgs", [])
    context.user_data["waiting_evidence"] = False
    context.user_data.pop("pending_report", None)
    context.user_data.pop("evidence_msgs", None)

    total_reports = db_add_report(user_id, reported_id, reason)
    logger.info("Report+evidence: %s melaporkan %s (%s) — total: %d", user_id, reported_id, reason, total_reports)

    logs = get_log(reported_id)
    log_text = "\n".join([f"  [{m['type']}] {m['content']}" for m in logs]) if logs else "  (tidak ada log)"

    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=(
                f"🚩 <b>Report Baru + Bukti</b>\n\n"
                f"👤 Pelapor: <code>{user_id}</code>\n"
                f"🎯 Dilaporkan: <code>{reported_id}</code>\n"
                f"📌 Alasan: <b>{reason}</b>\n"
                f"📊 Total report: <b>{total_reports}</b>\n\n"
                f"📋 <b>5 pesan terakhir si dilaporkan:</b>\n{log_text}\n\n"
                f"📨 <b>Bukti dari pelapor ({len(evidence_msgs)} pesan):</b>"
            ),
            parse_mode="HTML"
        )
        for msg_id in evidence_msgs:
            try:
                await context.bot.forward_message(
                    chat_id=ADMIN_ID,
                    from_chat_id=user_id,
                    message_id=msg_id
                )
            except TelegramError:
                pass
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text="Aksi:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🚫 Ban", callback_data=f"admin_ban_{reported_id}"),
                InlineKeyboardButton("✅ Abaikan", callback_data=f"admin_ignore_{reported_id}"),
            ]])
        )
    except TelegramError:
        pass

async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if context.user_data.get("waiting_evidence"):
        await _send_report_with_evidence(user_id, context)
        await context.bot.send_message(
            chat_id=user_id,
            text="✅ <i>Laporan selesai. Makasih udah lapor!</i>",
            parse_mode="HTML"
        )
    else:
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Ga ada yang lagi diproses.</i>",
            parse_mode="HTML"
        )


async def notify_online(app):
    # Notif yang lagi chat
    chatting = execute_turso("SELECT DISTINCT user_id FROM active_chats")
    for (user_id,) in chatting:
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="✅ <i>Bot udah nyala lagi, lanjut chat!</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass

    # Notif yang lagi waiting, lalu bersihkan waiting list
    waiting = execute_turso("SELECT user_id FROM waiting_users")
    execute_turso("DELETE FROM waiting_users")
    for (user_id,) in waiting:
        try:
            await app.bot.send_message(
                chat_id=user_id,
                text="✅ <i>Bot udah nyala lagi! Pencarian sebelumnya dibatalkan — pakai /find untuk mulai lagi ya.</i>",
                parse_mode="HTML"
            )
        except Exception:
            pass

    logger.info("Notif startup: %d chatting, %d waiting dibersihkan.", len(chatting), len(waiting))


def main():
    init_db()

    app = ApplicationBuilder().token(TOKEN).build()

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
    app.add_handler(CommandHandler("done", done_command))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(~filters.COMMAND, message))

    app.post_init = notify_online

    logger.info("Bot started.")
    app.run_polling()


if __name__ == "__main__":
    main()
