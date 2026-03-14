from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from config import TOKEN

FEEDBACK_BUTTON = InlineKeyboardMarkup([
    [InlineKeyboardButton("📝 Beri Feedback", url="https://feedbackneo.vercel.app")]
])

waiting_users = set()
active_chats = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        ["🚀 Cari partner"]
    ]

    reply_markup = ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True
    )

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=
        "🤖 <b>Anonymous Chat Bot</b>\n"
        "Temukan orang random dan chat secara anonim.\n"
        "🚀 Tekan tombol di bawah untuk mulai.\n\n"
        "💬 Punya saran?\n"
        "https://feedbackneo.vercel.app",
        parse_mode="HTML"
    )


async def find(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id in active_chats:
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <i>Kamu sudah memiliki partner.</i>\nGunakan /skip untuk mengganti.",
            parse_mode="HTML"
        )
        return

    if user_id in waiting_users:
        await context.bot.send_message(
            chat_id=user_id,
            text="🔎 <i>Sabar lagi nyari...</i>\nPake /stop kalo mau batalin.",
            parse_mode="HTML"
        )
        return

    partner = None

    for user in waiting_users:
        if user != user_id and user not in active_chats:
            partner = user
            break

    if partner:
        waiting_users.remove(partner)

        active_chats[user_id] = partner
        active_chats[partner] = user_id

        await context.bot.send_message(
            chat_id=user_id,
            text="💬 <b>Partner ditemukan!</b>\n\n/skip — <i>cari partner baru</i>\n/stop — <i>berhenti chat</i>\n\n<code>https://t.me/anonyneo_bot</code>",
            parse_mode="HTML"
        )

        await context.bot.send_message(
            chat_id=partner,
            text="💬 <b>Partner ditemukan!</b>\n\n/skip — <i>cari partner baru</i>\n/stop — <i>berhenti chat</i>\n\n<code>https://t.me/anonyneo_bot</code>",
            parse_mode="HTML"
        )

    else:
        waiting_users.add(user_id)

        await context.bot.send_message(
            chat_id=user_id,
            text="🔎 <i>Mencari partner...</i>\nGunakan /stop untuk membatalkan.",
            parse_mode="HTML"
        )

    print("Waiting:", waiting_users)
    print("Active:", active_chats)


async def message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not update.message:
        return
    
    if update.message.text == "🚀 Find a partner":
        await find(update, context)
        return

    user_id = update.effective_user.id

    if user_id not in active_chats:
        await context.bot.send_message(
            chat_id=user_id,
            text="<i>Kamu gapunya parter lol 😭\n\nGunakan /find untuk mencari partner.</i>\n\nhttps://feedbackneo.vercel.app",
            parse_mode="HTML"
        )
        return

    partner = active_chats.get(user_id)

    if not partner:
        return

    try:
        await context.bot.copy_message(
            chat_id=partner,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )
    except:
        pass


async def skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    waiting_users.discard(user_id)

    if user_id not in active_chats:
        await find(update, context)
        return

    partner = active_chats.get(user_id)

    active_chats.pop(user_id, None)
    active_chats.pop(partner, None)

    await context.bot.send_message(
        chat_id=user_id,
        text="🔎 <i>Mencari partner baru...</i>",
        parse_mode="HTML"
    )

    if partner:
        await context.bot.send_message(
            chat_id=partner,
            text="😞 <i>Partner kamu meninggalkan chat.</i>\n\n/find — <i>cari partner baru</i>",
            parse_mode="HTML",
            reply_markup=FEEDBACK_BUTTON
        )

    # langsung cari partner baru
    await find(update, context)


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id in waiting_users:
        waiting_users.discard(user_id)

        await context.bot.send_message(
            chat_id=user_id,
            text="😡 <i>Kamu menghentikan pencarian partner.</i>",
            parse_mode="HTML"
        )
        return

    if user_id not in active_chats:
        await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ Kamu belum terhubung dengan siapa pun.\n\nGunakan /find untuk mencari partner."
        )
        return

    partner = active_chats.get(user_id)

    active_chats.pop(user_id, None)
    active_chats.pop(partner, None)

    await context.bot.send_message(
    chat_id=user_id,
    text="💬 <i>Chat telah berakhir.\n\nTerima kasih sudah menggunakan bot ini.</i>",
    parse_mode="HTML",
    reply_markup=FEEDBACK_BUTTON
)

    if partner:
        await context.bot.send_message(
            chat_id=partner,
            text="😞 <i>Partner kamu meninggalkan chat.\n\n/find untuk mencari partner baru.</i>\n\n",
            parse_mode="HTML"
        )
        await context.bot.send_message(
            chat_id=partner,
            text="😞 <>Tolong feedbacknya dongg.. kalau mau kasih saran/apapun boleh, asbun jg oke",
            parse_mode="HTML",
            reply_markup=FEEDBACK_BUTTON
        )


def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("find", find))
    app.add_handler(CommandHandler("skip", skip))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(MessageHandler(~filters.COMMAND, message))

    app.run_polling()


if __name__ == "__main__":
    main()
