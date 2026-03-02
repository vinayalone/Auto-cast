import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from src.config import API_ID, API_HASH, BOT_TOKEN, validate_env
from src.db import init_db, get_all_tasks
from src.scheduler import setup_scheduler, add_scheduler_job, scheduler
from src.handlers.callback import callback_router
from src.handlers.inputs import handle_input
from src.handlers.menu import show_main_menu
from src.auth import login_state
import src.globals as globals

logger = logging.getLogger(__name__)

app = Client(
    "autocast_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True  # don't write session file to disk (wiped on redeploy)
)
globals.app = app


@app.on_message(filters.command(["start", "manage"]))
async def start_command(client, message):
    uid = message.from_user.id
    logger.info(f"📨 /start received from uid={uid}")
    if uid not in globals.user_state:
        globals.user_state[uid] = {}
    from src.db import get_session
    if await get_session(uid):
        await show_main_menu(client, message, uid, force_new=True)
    else:
        await message.reply_text(
            "👋 **Welcome to AutoCast | Channel Manager!**\n\n"
            "Your ultimate tool for scheduling and managing content across your Telegram channels.\n\n"
            "👇 **Ready? Click 'Login' below!**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 Login Account", callback_data="login_start")]
            ])
        )


@app.on_message(filters.private & ~filters.command(["start", "manage"]))
async def private_message_handler(client, message):
    await handle_input(client, message)


@app.on_callback_query()
async def callback_query_handler(client, callback_query):
    await callback_router(client, callback_query)


async def main():
    validate_env()

    # init_db() uses CREATE TABLE IF NOT EXISTS — idempotent, no Alembic needed
    await init_db()
    logger.info("✅ Database ready")

    global scheduler
    scheduler = setup_scheduler()
    scheduler.start()

    tasks = await get_all_tasks()
    logger.info(f"📂 Loaded {len(tasks)} tasks from DB")
    for t in tasks:
        add_scheduler_job(t)

    await app.start()
    logger.info("✅ Bot started!")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
