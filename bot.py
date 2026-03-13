from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from config import TOKEN

waiting_users = set()
active_chats = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=
        "<b>🤖 Anonymous Chat Bot</b>\n"
        "<i>Temukan orang random dan chat secara anonim.</i>\n\n"
        "Perintah yang tersedia:\n"
        "/find — cari partner\n"
        "/skip — ganti partner\n"
        "/stop — berhenti chat\n\n"
        "<i>Semua chat bersifat anonim, jadi aman.</i>",
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
            text="💬 <b>Partner ditemukan!</b>\n\n/skip — cari partner baru\n/stop — berhenti chat\n\n<code>https://t.me/anonyneo_bot</code>",
            parse_mode="HTML"
        )

        await context.bot.send_message(
            chat_id=partner,
            text="💬 <b>Partner ditemukan!</b>\n\n/skip — cari partner baru\n/stop — berhenti chat\n\n<code>https://t.me/anonyneo_bot</code>",
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

    user_id = update.effective_user.id

    if user_id not in active_chats:
        await context.bot.send_message(
            chat_id=user_id,
            text="Kamu gapunya parter lol 😭\n\nGunakan /find untuk mencari partner."
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
        await context.bot.send_message(
            chat_id=user_id,
            text="❔ <i>Kamu tidak sedang chat.</i>",
            parse_mode="HTML"
        )
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
            text="😞 <i>Partner kamu meninggalkan chat.</i>\n\n/find — cari partner baru",
            parse_mode="HTML"
        )

    # langsung cari partner baru
    await find(update, context)


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if user_id in waiting_users:
        waiting_users.discard(user_id)

        await context.bot.send_message(
            chat_id=user_id,
            text="😡 Kamu menghentikan pencarian partner."
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
        text="❌ Chat dihentikan."
    )

    if partner:
        await context.bot.send_message(
            chat_id=partner,
            text="😞 Partner kamu meninggalkan chat.\n\n/find untuk mencari partner baru."
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