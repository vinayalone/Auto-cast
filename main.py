import os
import sys
import logging
import asyncio
import datetime
import pytz
import asyncpg
import json
from io import BytesIO
from pyrogram import Client, filters, idle, errors, enums
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    Message, MessageEntity,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# --- ENVIRONMENT VARIABLE VALIDATION ---
def check_env_vars():
    required_vars = ["API_ID", "API_HASH", "BOT_TOKEN", "DATABASE_URL"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set all necessary environment variables before starting the bot.")
        sys.exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL") 

IST = pytz.timezone('Asia/Kolkata')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ManagerBot")

# --- INIT ---
app = Client("manager_v32_master", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
scheduler = None 
db_pool = None
queue_lock = None

# Global Cache
login_state = {}
user_state = {}

# --- DATABASE (AsyncPG) ---
async def get_db():
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
    return db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS userbot_tasks_v11 (
                task_id TEXT PRIMARY KEY, 
                owner_id BIGINT, 
                chat_id TEXT, 
                content_type TEXT, 
                content_text TEXT, 
                file_id TEXT, 
                entities TEXT, 
                pin BOOLEAN DEFAULT FALSE, 
                delete_old BOOLEAN DEFAULT FALSE, 
                repeat_interval TEXT, 
                start_time TEXT, 
                last_msg_id BIGINT,
                auto_delete_offset INTEGER DEFAULT 0,
                reply_target TEXT
            );
        ''')
        
        migrations = [
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS auto_delete_offset INTEGER DEFAULT 0",
            "ALTER TABLE userbot_tasks_v11 ADD COLUMN IF NOT EXISTS reply_target TEXT"
        ]
        
        for query in migrations:
            try:
                await conn.execute(query)
            except Exception as e:
                logger.warning(f"⚠️ Migration note (likely already exists): {e}")

    logger.info("📡 Database initialized: userbot_tasks_v11 is ready.")

async def migrate_to_v11():
    pool = await get_db()
    async with pool.acquire() as conn:
        logger.info("🔄 [MIGRATION] Checking for legacy data in 'tasks' table...")
        
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'tasks'
            );
        """)
        
        if table_exists:
            try:
                result = await conn.execute('''
                    INSERT INTO userbot_tasks_v11 (
                        task_id, owner_id, chat_id, content_type, content_text, 
                        file_id, entities, pin, delete_old, repeat_interval, 
                        start_time, last_msg_id
                    )
                    SELECT 
                        task_id, owner_id, chat_id, content_type, content_text, 
                        file_id, entities, pin, delete_old, repeat_interval, 
                        start_time, last_msg_id
                    FROM tasks
                    ON CONFLICT (task_id) DO NOTHING;
                ''')
                logger.info(f"✅ [MIGRATION] Success: {result}")
            except Exception as e:
                logger.error(f"❌ [MIGRATION] Failed to move data: {e}")
        else:
            logger.info("ℹ️ [MIGRATION] No legacy 'tasks' table found. Skipping.")

async def delete_sent_message(owner_id, chat_id, message_id):
    try:
        session = await get_session(owner_id)
        if not session:
            return

        async with Client(
            ":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session,
            device_model="AutoCast Client",
            system_version="PC",
            app_version="AutoCast Version"
        ) as user:
            await user.delete_messages(int(chat_id), message_id)
            logger.info(f"🗑️ Auto-delete success: Msg {message_id} in {chat_id}")

    except errors.MessageDeleteForbidden:
        logger.warning(f"⚠️ Auto-delete failed: Bot lacks delete permissions in {chat_id}.")
    except errors.ChatAdminRequired:
        logger.warning(f"⚠️ Auto-delete failed: Bot is not an admin in {chat_id}.")
    except errors.MessageNotModified:
        logger.info(f"ℹ️ Auto-delete: Message {message_id} in {chat_id} already deleted.")
    except errors.PeerIdInvalid:
        logger.warning(f"⚠️ Auto-delete failed: Invalid chat ID {chat_id}.")
    except Exception as e:
        logger.error(f"❌ Auto-delete failed with unexpected error for msg {message_id} in {chat_id}: {e}")

# FIX #1: save_task now saves AND updates auto_delete_offset in ON CONFLICT
async def save_task(t):
    pool = await get_db()
    await pool.execute("""
        INSERT INTO userbot_tasks_v11 (task_id, owner_id, chat_id, content_type, content_text, file_id, entities, pin, delete_old, repeat_interval, start_time, last_msg_id, auto_delete_offset, reply_target)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (task_id) DO UPDATE SET last_msg_id = $12, start_time = $11, auto_delete_offset = $13
    """, t['task_id'], t['owner_id'], t['chat_id'], t['content_type'], t['content_text'], t['file_id'], 
       t['entities'], t['pin'], t['delete_old'], t['repeat_interval'], t['start_time'], t['last_msg_id'],
       t.get('auto_delete_offset', 0), t.get('reply_target'))

# --- DB HELPERS ---
async def get_session(user_id):
    pool = await get_db()
    row = await pool.fetchrow("SELECT session_string FROM userbot_sessions WHERE user_id = $1", user_id)
    return row['session_string'] if row else None

async def save_session(user_id, session):
    pool = await get_db()
    await pool.execute("INSERT INTO userbot_sessions (user_id, session_string) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET session_string = $2", user_id, session)

async def del_session(user_id):
    pool = await get_db()
    await pool.execute("DELETE FROM userbot_sessions WHERE user_id = $1", user_id)

async def add_channel(user_id, cid, title):
    pool = await get_db()
    await pool.execute("INSERT INTO userbot_channels (user_id, channel_id, title) VALUES ($1, $2, $3) ON CONFLICT (user_id, channel_id) DO UPDATE SET title = $3", user_id, cid, title)

async def delete_all_user_data(user_id):
    pool = await get_db()
    
    session_str = await get_session(user_id)
    if session_str:
        try:
            async def fast_logout():
                async with Client(":memory:", api_id=API_ID, api_hash=API_HASH, session_string=session_str,
                                  device_model="AutoCast Client", 
                                  system_version="PC",
                                  app_version="AutoCast Version") as temp_user:
                    await temp_user.log_out()
            
            await asyncio.wait_for(fast_logout(), timeout=5.0)
            logger.info(f"✅ User {user_id} session terminated.")
            
        except Exception as e:
            logger.warning(f"⚠️ Session kill skipped (Error/Timeout): {e}")

    tasks = await pool.fetch("SELECT task_id FROM userbot_tasks_v11 WHERE owner_id = $1", user_id)
    if scheduler:
        for t in tasks:
            try: scheduler.remove_job(t['task_id'])
            except: pass
            
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE owner_id = $1", user_id)
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1", user_id) 
    await pool.execute("DELETE FROM userbot_sessions WHERE user_id = $1", user_id)

    if user_id in user_state: del user_state[user_id]
    if user_id in login_state: del login_state[user_id]
        
async def get_channels(user_id):
    pool = await get_db()
    return await pool.fetch("SELECT * FROM userbot_channels WHERE user_id = $1", user_id)

async def del_channel(user_id, cid):
    pool = await get_db() 
    tasks = await pool.fetch("SELECT task_id FROM userbot_tasks_v11 WHERE chat_id = $1", cid)
    if scheduler:
        for t in tasks:
            try: 
                scheduler.remove_job(t['task_id'])
            except: pass 
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE chat_id = $1", cid)
    await pool.execute("DELETE FROM userbot_channels WHERE user_id = $1 AND channel_id = $2", user_id, cid)

async def get_all_tasks():
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks_v11")]

async def get_user_tasks(user_id, chat_id):
    pool = await get_db()
    return [dict(x) for x in await pool.fetch("SELECT * FROM userbot_tasks_v11 WHERE owner_id = $1 AND chat_id = $2", user_id, chat_id)]

async def get_single_task(task_id):
    pool = await get_db()
    row = await pool.fetchrow("SELECT * FROM userbot_tasks_v11 WHERE task_id = $1", task_id)
    return dict(row) if row else None

async def delete_task(task_id):
    pool = await get_db()
    row = await pool.fetchrow("SELECT chat_id FROM userbot_tasks_v11 WHERE task_id = $1", task_id)
    await pool.execute("DELETE FROM userbot_tasks_v11 WHERE task_id = $1", task_id)
    return row['chat_id'] if row else None

async def update_last_msg(task_id, msg_id):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks_v11 SET last_msg_id = $1 WHERE task_id = $2", msg_id, task_id)

async def update_next_run(task_id, next_run_iso):
    pool = await get_db()
    await pool.execute("UPDATE userbot_tasks_v11 SET start_time = $1 WHERE task_id = $2", next_run_iso, task_id)

# --- UI HELPERS ---
async def show_main_menu(m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
        [InlineKeyboardButton("📢 My Channels", callback_data="list_channels")],
        [InlineKeyboardButton("➕ Add Channel (Forward Msg)", callback_data="add_channel_forward")],
        [InlineKeyboardButton("➕ Add Channel (By ID)", callback_data="add_channel_id")],
        [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
    ]
    await update_menu(m, "👋 **Welcome to AutoCast | Channel Manager!**\n\nYour central hub for managing scheduled posts across your Telegram channels. Select an option below to get started.", kb, uid, force_new)

async def show_channels(uid, m, force_new=False):
    chs = await get_channels(uid)
    if not chs:
        kb = [
            [InlineKeyboardButton("➕ Add One", callback_data="add_channel_forward")],
            [InlineKeyboardButton("➕ Add By ID", callback_data="add_channel_id")],
            [InlineKeyboardButton("🔙 Back", callback_data="menu_home")]
        ]
        await update_menu(m, "❌ **No Channels Linked Yet.**\n\nIt looks like you haven't linked any channels to AutoCast. Please use the '➕ Add Channel' options to get started!", kb, uid, force_new)
        return
    kb = []
    for c in chs: kb.append([InlineKeyboardButton(c['title'], callback_data=f"ch_{c['channel_id']}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="menu_home")])
    await update_menu(m, "**📢 Your Linked Channels**\n\nBelow is a list of channels you've linked with AutoCast. Select a channel to view its scheduled tasks or manage its settings.", kb, uid, force_new)

async def show_channel_options(uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    kb = [
        [InlineKeyboardButton("✍️ Schedule Post", callback_data=f"new_{cid}")],
        [InlineKeyboardButton(f"📅 Scheduled ({len(tasks)})", callback_data=f"tasks_{cid}")],
        [InlineKeyboardButton("🗑 Unlink", callback_data=f"rem_{cid}"), InlineKeyboardButton("🔙 Back", callback_data="list_channels")]
    ]
    await update_menu(m, f"⚙️ **Managing Channel**", kb, uid, force_new)

async def show_time_menu(m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("⚡️ Now (5s delay)", callback_data="time_0")],
        [InlineKeyboardButton("5 Minutes", callback_data="time_5"), InlineKeyboardButton("15 Minutes", callback_data="time_15")],
        [InlineKeyboardButton("30 Minutes", callback_data="time_30"), InlineKeyboardButton("1 Hour", callback_data="time_60")],
        [InlineKeyboardButton("Custom Date/Time", callback_data="time_custom")],
        [InlineKeyboardButton("🔙 Back", callback_data="menu_home")]
    ]
    await update_menu(m, "2️⃣ **Schedule Time**\n\nWhen would you like this post to be sent?", kb, uid, force_new)

async def ask_repetition(m, uid, force_new=False):
    kb = [
        [InlineKeyboardButton("Once (No Repeat)", callback_data="rep_0")],
        [InlineKeyboardButton("Every 1 Hour", callback_data="rep_60"), InlineKeyboardButton("Every 3 Hours", callback_data="rep_180")],
        [InlineKeyboardButton("Every 6 Hours", callback_data="rep_360"), InlineKeyboardButton("Every 12 Hours", callback_data="rep_720")],
        [InlineKeyboardButton("Every 24 Hours", callback_data="rep_1440")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_time")]
    ]
    await update_menu(m, "3️⃣ **Repetition**\n\nHow often should this post be repeated?", kb, uid, force_new)

async def ask_settings(m, uid, force_new=False):
    st = user_state[uid]
    queue = st.get("broadcast_queue")

    if queue:
        txt = ("4️⃣ **Batch Post Settings**\n\n"
               "**Legend:**\n"
               "📌 **Pin:** Pin message.\n"
               "🗑 **Del:** Delete previous post.\n"
               "⏰ **Off:** Minutes to delete *after* posting.\n\n"
               "👇 **Configure individual post settings:**")
        
        kb = []
        for i, post in enumerate(queue):
            p_stat = "ON" if post.get("pin") else "OFF"
            d_stat = "ON" if post.get("delete_old") else "OFF"
            offset = post.get("auto_delete_offset", 0)
            off_stat = f"{offset}m" if offset > 0 else "OFF"
            btn_txt = f"✅ Post #{i+1} | P: {p_stat} | D: {d_stat} | ⏰ {off_stat}"
            kb.append([InlineKeyboardButton(btn_txt, callback_data=f"cfg_q_{i}")])
        
        kb.append([InlineKeyboardButton("➡️ Confirm All", callback_data="goto_confirm")])
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="step_rep")])
        
        await update_menu(m, txt, kb, uid, force_new)
        return

    st.setdefault("pin", True)
    st.setdefault("del", True)
    offset = st.get("auto_delete_offset", 0)
    
    pin_icon = "✅" if st["pin"] else "❌"
    del_icon = "✅" if st["del"] else "❌"
    off_text = f"⏰ Delete: {offset}m After Post" if offset > 0 else "⏰ Auto-Delete: OFF"
    
    kb = [
        [InlineKeyboardButton(f"📌 Pin Msg: {pin_icon}", callback_data="toggle_pin")],
        [InlineKeyboardButton(f"🗑 Del Old: {del_icon}", callback_data="toggle_del")],
        [InlineKeyboardButton(off_text, callback_data="wizard_ask_offset")],
        [InlineKeyboardButton("➡️ Confirm", callback_data="goto_confirm")],
        [InlineKeyboardButton("🔙 Back", callback_data="step_rep")]
    ]
    
    msg_text = (f"4️⃣ **Settings**\n\n"
                f"Configure how your post behaves.\n"
                f"Auto-delete is currently: **{offset} minutes** after posting.")
                
    await update_menu(m, msg_text, kb, uid, force_new)
    
async def confirm_task(m, uid, force_new=False):
    st = user_state[uid]
    t_str = st["start_time"].strftime("%d-%b %I:%M %p")
    r_str = st["interval"] if st["interval"] else "Once"
    
    queue = st.get("broadcast_queue")
    
    if queue:
        type_str = f"📦 Batch ({len(queue)} Posts)"
        pin_count = sum(1 for p in queue if p['pin'])
        settings_str = f"📌 Pinning: {pin_count}/{len(queue)} Posts"
    else:
        type_map = {
            "text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video",
            "audio": "🎵 Audio", "voice": "🎙 Voice", "document": "📁 File",
            "poll": "📊 Poll", "animation": "🎞 GIF", "sticker": "✨ Sticker"
        }
        c_type = st.get('content_type', 'unknown')
        type_str = type_map.get(c_type, c_type.upper())
        settings_str = f"📌 Pin: {'✅' if st.get('pin',True) else '❌'} | 🗑 Del: {'✅' if st.get('del',True) else '❌'}"
    
    txt = (f"✅ **Summary**\n\n"
           f"📢 Content: {type_str}\n"
           f"📅 Time: `{t_str}`\n"
           f"🔁 Repeat: `{r_str}`\n"
           f"{settings_str}")
    
    kb = [[InlineKeyboardButton("✅ Schedule It", callback_data="save_task")],
          [InlineKeyboardButton("🔙 Back", callback_data="step_settings")]]
    
    await update_menu(m, txt, kb, uid, force_new)

async def list_active_tasks(uid, m, cid, force_new=False):
    tasks = await get_user_tasks(uid, cid)
    if not tasks:
        await update_menu(m, "✅ No active tasks.", [[InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")]], uid, force_new)
        return
    
    tasks.sort(key=lambda x: x['start_time'])
    txt = "**📅 Scheduled Tasks:**\nSelect one to manage:"
    kb = []
    
    type_icons = {"text": "📝", "photo": "📷", "video": "📹", "audio": "🎵", "poll": "📊"}
    
    for t in tasks:
        snippet = (t['content_text'] or "Media")[:15] + "..."
        icon = type_icons.get(t['content_type'], "📁")
        try:
            dt = datetime.datetime.fromisoformat(t["start_time"])
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            time_str = dt.strftime('%I:%M %p') 
        except: time_str = "?"
        
        btn_text = f"{icon} {snippet} | ⏰ {time_str}"
        kb.append([InlineKeyboardButton(btn_text, callback_data=f"view_{t['task_id']}")])
        
    kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"ch_{cid}")])
    await update_menu(m, txt, kb, uid, force_new)

async def show_task_details(uid, m, tid):
    t = await get_single_task(tid)
    if not t:
        await update_menu(m, "❌ Task not found.", [[InlineKeyboardButton("🏠 Home", callback_data="menu_home")]], uid)
        return

    dt = datetime.datetime.fromisoformat(t["start_time"])
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    time_str = dt.strftime('%d-%b %I:%M %p')
    type_map = {"text": "📝 Text", "photo": "📷 Photo", "video": "📹 Video", "audio": "🎵 Audio", "poll": "📊 Poll"}
    type_str = type_map.get(t['content_type'], "📁 File")
    
    txt = (f"⚙️ **Managing Task**\n\n"
           f"📝 **Snippet:** `{t['content_text'][:50] if t['content_text'] else 'Media'}...`\n"
           f"📂 **Type:** {type_str}\n"
           f"📅 **Time:** `{time_str}`\n"
           f"🔁 **Repeat:** `{t['repeat_interval'] or 'No'}`\n\n"
           f"👇 **Select Action:**")

    kb = [
        [InlineKeyboardButton("🗑 Delete Task", callback_data=f"del_task_{tid}")],
        [InlineKeyboardButton("🔙 Back to List", callback_data=f"back_list_{t['chat_id']}")]
    ]
    await update_menu(m, txt, kb, uid)

# --- SERIALIZATION ---
def serialize_entities(entities_list):
    if not entities_list: return None
    data = []
    for e in entities_list:
        data.append({
            "type": str(e.type), "offset": e.offset, "length": e.length,
            "url": e.url, "language": e.language, "custom_emoji_id": e.custom_emoji_id
        })
    return json.dumps(data)

def deserialize_entities(json_str):
    if not json_str: return None
    try:
        data = json.loads(json_str)
        entities = []
        for item in data:
            type_str = item["type"].split(".")[-1] 
            e_type = getattr(enums.MessageEntityType, type_str)
            entity = MessageEntity(
                type=e_type, offset=item["offset"], length=item["length"],
                url=item["url"], language=item["language"], custom_emoji_id=item["custom_emoji_id"]
            )
            entities.append(entity)
        return entities
    except: return None

# FIX #4: get_delete_before_kb was called but never defined — now implemented
async def get_delete_before_kb(temp_task_id, repeat_mins):
    all_options = [
        (0,    "🚫 Disable"),
        (5,    "5 min"),
        (10,   "10 min"),
        (15,   "15 min"),
        (30,   "30 min"),
        (60,   "1 hr"),
        (120,  "2 hrs"),
        (180,  "3 hrs"),
        (360,  "6 hrs"),
        (720,  "12 hrs"),
        (1440, "24 hrs"),
    ]
    kb = []
    row = []
    for mins, label in all_options:
        cb = f"set_del_off_{temp_task_id}_{mins}"
        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 3:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="step_settings")])
    return InlineKeyboardMarkup(kb)

# --- UI HELPER: HYBRID FLOW ---
async def update_menu(m, text, kb, uid, force_new=False):
    markup = InlineKeyboardMarkup(kb) if kb else None
    
    if force_new:
        sent = await app.send_message(m.chat.id, text, reply_markup=markup)
        if uid in user_state:
            user_state[uid]["menu_msg_id"] = sent.id
        return

    st = user_state.get(uid, {})
    menu_id = st.get("menu_msg_id")
    
    if menu_id:
        try:
            await app.edit_message_text(m.chat.id, menu_id, text, reply_markup=markup)
            return
        except: pass 

    sent = await app.send_message(m.chat.id, text, reply_markup=markup)
    if uid in user_state:
        user_state[uid]["menu_msg_id"] = sent.id

# --- BOT INTERFACE ---
@app.on_message(filters.command("manage") | filters.command("start"))
async def start_cmd(c, m):
    uid = m.from_user.id
    if uid not in user_state: user_state[uid] = {}
    
    if await get_session(uid):
        kb = [
            [InlineKeyboardButton("📢 Broadcast (Post to All)", callback_data="broadcast_start")],
            [InlineKeyboardButton("📢 My Channels", callback_data="list_channels")],
            [InlineKeyboardButton("➕ Add Channel (Forward Msg)", callback_data="add_channel_forward")],
            [InlineKeyboardButton("➕ Add Channel (By ID)", callback_data="add_channel_id")],
            [InlineKeyboardButton("🚪 Logout", callback_data="logout")]
        ]
        sent = await m.reply("👋 **Welcome to AutoCast | Channel Manager!**\n\nYour central hub for managing scheduled posts across your Telegram channels. Select an option below to get started.", reply_markup=InlineKeyboardMarkup(kb))
        user_state[uid]["menu_msg_id"] = sent.id
    else:
        await m.reply_text(
            "👋 **Welcome to AutoCast | Channel Manager!**\n\n"\
            "Your ultimate tool for scheduling and managing content across your Telegram channels. Here's how to begin:\n\n"\
            "**🚀 Getting Started:**\n"\
            "1️⃣ **Secure Login:** Connect your Telegram account to enable bot functionality.\n"\
            "2️⃣ **Add Channels:** Link the channels where you want to post content.\n"\
            "3️⃣ **Schedule Posts:** Create and schedule messages, photos, videos, and more!\n\n"\
            "👇 **Ready to elevate your channel management? Click 'Login' below!**",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Login Account", callback_data="login_start")]])
        )

async def show_broadcast_selection(uid, m):
    chs = await get_channels(uid)
    if not chs:
        await update_menu(m, "❌ No channels found.", [[InlineKeyboardButton("🔙 Back", callback_data="menu_home")]], uid)
        return

    targets = user_state[uid].get("broadcast_targets", [])
    kb = []
    
    for c in chs:
        is_selected = c['channel_id'] in targets
        icon = "✅" if is_selected else "⬜"
        kb.append([InlineKeyboardButton(f"{icon} {c['title']}", callback_data=f"toggle_bc_{c['channel_id']}")])
    
    kb.append([InlineKeyboardButton(f"➡️ Done ({len(targets)} Selected)", callback_data="broadcast_confirm")])
    kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")])
    
    await update_menu(m, "📢 **Broadcast Mode**\n\nSelect channels to post to:", kb, uid)

@app.on_callback_query()
async def callback_router(c, q):
    uid = q.from_user.id
    d = q.data

    if uid not in user_state: user_state[uid] = {}
    user_state[uid]["menu_msg_id"] = q.message.id

    if d == "menu_home":
        user_state[uid]["step"] = None
        await show_main_menu(q.message, uid)
    
    elif d == "login_start":
        login_state[uid] = {"step": "waiting_phone"}
        await update_menu(q.message, "📱 **Step 1: Phone Number**\n\nPlease enter your Telegram phone number with country code.\n\nExample: `+919876543210`", [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)
    
    elif d == "logout":
        tasks = await get_all_tasks()
        user_tasks = [t for t in tasks if t['owner_id'] == uid]
        txt = (f"⚠️ **Wait! Are you sure?**\n\n"
               f"You have **{len(user_tasks)} active tasks** scheduled.\n"
               f"If you logout, the bot will stop working.")
        kb = [[InlineKeyboardButton("⚠️ Yes, I want to Logout", callback_data="logout_step_2")],
              [InlineKeyboardButton("🔙 No, Cancel", callback_data="menu_home")]]
        await update_menu(q.message, txt, kb, uid)

    elif d == "logout_step_2":
        txt = ("🛑 **FINAL WARNING** 🛑\n\n"
               "This will **PERMANENTLY DELETE** all your scheduled posts and settings.\n"
               "This action cannot be undone.\n\n"
               "Are you absolutely sure?")
        kb = [[InlineKeyboardButton("🗑️ Delete Everything & Logout", callback_data="logout_final")],
              [InlineKeyboardButton("🔙 No! Go Back", callback_data="menu_home")]]
        await update_menu(q.message, txt, kb, uid)

    elif d == "logout_final":
        try:
            await app.edit_message_text(uid, q.message.id, "⏳ **Logging out...**\nTerminating session and wiping data.")
        except: 
            await q.answer("⏳ Processing...", show_alert=False)
        await delete_all_user_data(uid) 
        try:
            await app.edit_message_text(
                chat_id=uid, 
                message_id=q.message.id, 
                text="👋 **Logged out successfully.**\n\nAll data has been wiped and your active session has been terminated."
            )
        except:
            await app.send_message(uid, "👋 **Logged out successfully.**")

    elif d.startswith("view_"):
        tid = d.split("view_")[1]
        await show_task_details(uid, q.message, tid)

    # FIX #5: back_list_ handler was missing entirely from callback_router
    elif d.startswith("back_list_"):
        cid = d.split("back_list_")[1]
        await list_active_tasks(uid, q.message, cid)

    elif d.startswith("prev_"):
        tid = d.split("prev_")[1]
        task = await get_single_task(tid)
        if task and task['last_msg_id']:
            try:
                await app.copy_message(chat_id=uid, from_chat_id=int(task['chat_id']), message_id=task['last_msg_id'])
                await q.answer("✅ Preview sent!")
            except: await q.answer("❌ Cannot preview (Message not posted yet or deleted)")
        else:
            await q.answer("❌ Task hasn't run yet.")

    elif d.startswith("del_task_"):
        tid = d.split("del_task_")[1]
        try:
            if scheduler: scheduler.remove_job(tid)
            cid = await delete_task(tid)
            await q.answer("✅ Task deleted!")
            if cid: await list_active_tasks(uid, q.message, cid)
            else: await show_main_menu(q.message, uid)
        except Exception as e:
            logger.error(f"Error deleting task {tid}: {e}")
            await q.answer("❌ Failed to delete task.")

    elif d == "broadcast_start":
        user_state[uid]["step"] = "broadcast_select_channels"
        user_state[uid]["broadcast_targets"] = []
        await show_broadcast_selection(uid, q.message)

    elif d.startswith("toggle_bc_"):
        cid = d.split("toggle_bc_")[1]
        targets = user_state[uid].get("broadcast_targets", [])
        if cid in targets: targets.remove(cid)
        else: targets.append(cid)
        user_state[uid]["broadcast_targets"] = targets
        await show_broadcast_selection(uid, q.message)

    elif d == "broadcast_confirm":
        targets = user_state[uid].get("broadcast_targets", [])
        if not targets:
            await q.answer("❌ Select at least one channel!", show_alert=True)
            return
        user_state[uid]["broadcast_queue"] = [] 
        user_state[uid]["step"] = "waiting_broadcast_content"
        markup = ReplyKeyboardMarkup(
            [[KeyboardButton("✅ Done Adding Posts")], [KeyboardButton("❌ Cancel")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        guide_text = (
            f"📢 **Multi-Post Mode Active**\n"
            f"Selected: **{len(targets)} Channels**\n\n"
            f"👇 **How to Use:**\n"
            f"1️⃣ **Send Posts:** Send text, photos, or videos one by one.\n"
            f"2️⃣ **Create Threads:** If you want Post B to reply to Post A, simply **reply to Post A** right here!\n"
            f"3️⃣ **Finish:** Click **✅ Done** when finished.\n\n"
            f"⚙️ *You can configure Pin/Delete settings for each post individually after adding them.*"
        )
        await app.send_message(q.message.chat.id, guide_text, reply_markup=markup)

    elif d == "list_channels":
        await show_channels(uid, q.message)
    
    elif d == "add_channel_forward":
        user_state[uid]["step"] = "waiting_forward"
        await update_menu(q.message, 
                          "📝 **Step 2: Add Channel**\n\nForward a message from your channel to this chat now.\nI will detect the ID automatically.", 
                          None, uid)

    elif d == "add_channel_id":
        user_state[uid]["step"] = "waiting_channel_id"
        await update_menu(q.message, 
                          "📝 **Step 2: Add Channel by ID**\n\nPlease send the **Channel ID** now.\n\n"\
                          "**How to find Channel ID:**\n"\
                          "1. Add your bot as an administrator to your channel.\n"\
                          "2. Forward any message from your channel to @JsonDumpBot.\n"\
                          "3. Copy the `forward_from_chat.id` (it will be a negative number, e.g., `-1001234567890`).", 
                          [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)

    elif d.startswith("ch_"):
        cid = d.split("ch_")[1]
        await show_channel_options(uid, q.message, cid)
    
    elif d.startswith("rem_"):
        cid = d.split("rem_")[1]
        await del_channel(uid, cid)
        await q.answer("Channel Unlinked!")
        await show_channels(uid, q.message)

    elif d.startswith("new_"):
        cid = d.split("new_")[1]
        user_state[uid].update({"step": "waiting_content", "target": cid})
        await update_menu(q.message, "1️⃣ **Create Post**\n\nSend me the content you want to schedule:\n• Text / Photo / Video\n• Audio / Voice Note\n• Poll", 
                        [[InlineKeyboardButton("🔙 Cancel", callback_data="menu_home")]], uid)

    elif d == "step_time":
        await show_time_menu(q.message, uid)
    elif d == "step_rep":
        await ask_repetition(q.message, uid)
    elif d == "step_settings":
        await ask_settings(q.message, uid)

    elif d.startswith("wizard_ask_offset"):
        interval = user_state[uid].get("interval")
        if interval and "=" in interval:
            repeat_mins = int(interval.split("=")[1])
        else:
            repeat_mins = 999999 

        parts = d.split("_")
        is_batch = len(parts) > 3 
        temp_task_id = f"WIZARD_{parts[3]}" if is_batch else "WIZARD"
        
        markup = await get_delete_before_kb(temp_task_id, repeat_mins)
        
        await update_menu(
            q.message, 
            "⏳ **Select Auto-Delete Time**\n\n"
            "How long should the message stay in the channel **after** being posted before it is automatically deleted?", 
            markup.inline_keyboard, 
            uid
        )

    elif d.startswith("set_del_off_WIZARD"):
        parts = d.split("_")
        
        if len(parts) == 5:
            offset = int(parts[4])
            user_state[uid]["auto_delete_offset"] = offset
            time_str = f"{offset}m" if offset > 0 else "Disabled"
            await q.answer(f"✅ Auto-Delete set to {time_str} after post!")
            
        elif len(parts) == 6:
            idx = int(parts[4])
            offset = int(parts[5])
            if "broadcast_queue" in user_state[uid]:
                user_state[uid]["broadcast_queue"][idx]["auto_delete_offset"] = offset
            time_str = f"{offset}m" if offset > 0 else "Disabled"
            await q.answer(f"✅ Post #{idx+1} auto-delete: {time_str}!")

        await ask_settings(q.message, uid)

    elif d.startswith("time_"):
        offset = d.split("time_")[1] 
        if offset == "custom":
            user_state[uid]["step"] = "waiting_custom_date"
            cur_time = datetime.datetime.now(IST).strftime("%d-%b %I:%M %p")
            msg_txt = (f"📅 **Select Custom Date**\n\n"
                       f"Current Time: `{cur_time}`\n"
                       f"(Tap to copy)\n\n"
                       f"Please type the date and time in this format:\n"
                       f"`{cur_time}`")
            await update_menu(q.message, msg_txt, [[InlineKeyboardButton("🔙 Back", callback_data="step_time")]], uid)
            return

        now = datetime.datetime.now(IST)
        if offset == "0":
            run_time = now + datetime.timedelta(seconds=5)
        else:
            run_time = now + datetime.timedelta(minutes=int(offset))
            run_time = run_time.replace(second=0, microsecond=0)
        
        user_state[uid]["start_time"] = run_time
        await ask_repetition(q.message, uid)

    elif d.startswith("rep_"):
        val = d.split("rep_")[1]
        interval = None
        if val != "0": interval = f"minutes={val}"
        user_state[uid]["interval"] = interval
        await ask_settings(q.message, uid)

    elif d.startswith("cfg_q_"):
        idx = int(d.split("cfg_q_")[1])
        post = user_state[uid]["broadcast_queue"][idx]
        p_stat = "Enabled ✅" if post["pin"] else "Disabled ❌"
        d_stat = "Enabled ✅" if post["delete_old"] else "Disabled ❌"
        txt = (f"⚙️ **Configuring Post #{idx+1}**\n\n"
               f"📂 Type: **{post['content_type']}**\n"
               f"📌 Pin this post? **{p_stat}**\n"
               f"🗑 Delete previous? **{d_stat}**")
        kb = [
            [InlineKeyboardButton(f"📌 Toggle Pin", callback_data=f"t_q_pin_{idx}")],
            [InlineKeyboardButton(f"🗑 Toggle Delete", callback_data=f"t_q_del_{idx}")],
            [InlineKeyboardButton("⏰ Set Delete Before", callback_data=f"wizard_ask_offset_{idx}")],
            [InlineKeyboardButton("🔙 Back to List", callback_data="step_settings")]
        ]
        await update_menu(q.message, txt, kb, uid)

    elif d.startswith("t_q_"):
        parts = d.split("_")
        action = parts[2]
        idx = int(parts[3])
        post = user_state[uid]["broadcast_queue"][idx]
        if action == "pin": post["pin"] = not post["pin"]
        if action == "del": post["delete_old"] = not post["delete_old"]
        q.data = f"cfg_q_{idx}"
        await callback_router(c, q)

    elif d in ["toggle_pin", "toggle_del"]:
        st = user_state[uid]
        st.setdefault("pin", True)
        st.setdefault("del", True)
        if d == "toggle_pin": st["pin"] = not st["pin"]
        if d == "toggle_del": st["del"] = not st["del"]
        await ask_settings(q.message, uid)

    elif d == "goto_confirm":
        await confirm_task(q.message, uid)

    elif d == "save_task":
        await create_task_logic(uid, q)

    elif d.startswith("tasks_"):
        cid = d.split("tasks_")[1]
        await list_active_tasks(uid, q.message, cid)

# --- INPUTS ---
@app.on_message(filters.private & ~filters.command("manage") & ~filters.command("start"))
async def handle_inputs(c, m):
    uid = m.from_user.id
    text = m.text.strip() if m.text else ""

    if uid in login_state:
        st = login_state[uid]
        if st["step"] == "waiting_phone":
            wait_msg = await m.reply("⏳ **Trying to connect!**\nThis can take up to 2 minutes.\n\nPlease wait...")
            try:
                temp = Client(":memory:", api_id=API_ID, api_hash=API_HASH, 
                              device_model="AutoCast Client",
                              system_version="PC", 
                              app_version="AutoCast Version")
                await temp.connect()
                sent = await temp.send_code(text)
                st.update({"client": temp, "phone": text, "hash": sent.phone_code_hash, "step": "waiting_code"})
                await wait_msg.delete()
                await update_menu(m, "📩 **Step 2: Enter Verification Code**\n\nTelegram has sent a verification code to your account. Please enter it below.\n\n⚠️ **Important:** To ensure the code is processed correctly and prevent expiry, please prefix the code with `aa`. For example, if your code is `12345`, send `aa12345`.", None, uid, force_new=True)
            except Exception as e: 
                await wait_msg.delete()
                await m.reply(f"❌ **Login Failed:** {e}\n\nIt seems there was an issue connecting to Telegram. Please ensure your phone number is correct and try /start again.")
        
        elif st["step"] == "waiting_code":
            try:
                real_code = text.lower().replace("aa", "").strip()
                await st["client"].sign_in(st["phone"], st["hash"], real_code)
                session_string = await st["client"].export_session_string()
                await save_session(uid, session_string)
                await st["client"].disconnect()
                del login_state[uid]
                await m.reply("✅ **Login Successful!**\n\nYou can now manage your channels. Click /manage to start.", reply_markup=ReplyKeyboardRemove())
            except errors.PhoneCodeInvalid:
                await m.reply("❌ **Invalid Verification Code.**\n\nThe code you entered was incorrect. Please try again, ensuring you prefix it with `aa`.")
            except errors.SessionPasswordNeeded:
                st["step"] = "waiting_password"
                await update_menu(m, "🔒 **Two-Step Verification Required**\n\nYour Telegram account has Two-Step Verification enabled. Please enter your cloud password to proceed.", None, uid, force_new=True)
            except Exception as e:
                logger.error(f"Login Error: {e}")
                await m.reply(f"❌ **Login Error:** An unexpected error occurred during the login process: {e}\n\nPlease try /start again. If the issue persists, contact support.")

        elif st["step"] == "waiting_password":
            try:
                await st["client"].check_password(text)
                session_string = await st["client"].export_session_string()
                await save_session(uid, session_string)
                await st["client"].disconnect()
                del login_state[uid]
                await m.reply("✅ **Login Successful!**\n\nYou can now manage your channels. Click /manage to start.", reply_markup=ReplyKeyboardRemove())
            except errors.PasswordHashInvalid:
                await m.reply("❌ **Invalid Password.**\n\nThe password you entered for Two-Step Verification was incorrect. Please try again.")
            except Exception as e:
                logger.error(f"Login Error: {e}")
                await m.reply(f"❌ **Password Verification Error:** An unexpected error occurred during password verification: {e}\n\nPlease try /start again. If the issue persists, contact support.")
        return

    if user_state.get(uid, {}).get("step") == "waiting_channel_id":
        try:
            channel_id = int(text)
            if not str(channel_id).startswith("-100"):
                await m.reply("❌ Invalid Channel ID format. Channel IDs usually start with `-100` followed by 10-12 digits. Please send a valid ID or click '🔙 Cancel'.")
                return
            
            session_string = await get_session(uid)
            if not session_string:
                await m.reply("❌ User session not found. Please login again using /start.")
                user_state[uid]["step"] = "menu_home"
                return

            async with Client(
                ":memory:",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session_string,
                device_model="AutoCast Client",
                system_version="PC",
                app_version="AutoCast Version"
            ) as user_client:
                try:
                    chat = await user_client.get_chat(channel_id)
                    if chat.type != enums.ChatType.CHANNEL:
                        await m.reply("❌ The provided ID does not belong to a channel. Please provide a valid channel ID.")
                        return
                    try:
                        member = await user_client.get_chat_member(channel_id, app.me.id)
                        if not member.can_post_messages:
                            await m.reply("⚠️ Warning: The bot does not have 'Post Messages' permission in this channel. Please grant it to allow scheduling.")
                        if not member.can_delete_messages:
                            await m.reply("⚠️ Warning: The bot does not have 'Delete Messages' permission in this channel. Auto-deletion of posts will not work.")
                    except errors.ChatAdminRequired:
                        await m.reply("❌ The bot is not an administrator in this channel. Please add the bot as an administrator and grant it 'Post Messages' and 'Delete Messages' permissions.")
                        return
                    except Exception as e:
                        logger.warning(f"Could not check bot permissions in channel {channel_id}: {e}")
                        await m.reply("⚠️ Could not verify bot permissions in the channel. Please ensure the bot is an administrator with necessary permissions.")

                    await add_channel(uid, str(channel_id), chat.title)
                    await m.reply(f"✅ Channel **{chat.title}** (`{channel_id}`) added successfully!", reply_markup=ReplyKeyboardRemove())
                    user_state[uid]["step"] = "menu_home"
                    await start_cmd(c, m)
                except errors.ChatIdInvalid:
                    await m.reply("❌ Invalid Channel ID. Please ensure it's correct and the bot has access to the channel.")
                except errors.ChatAdminRequired:
                    await m.reply("❌ The bot is not an administrator in this channel. Please add the bot as an administrator.")
                except errors.ChatForwardsRestricted:
                    await m.reply("❌ Cannot access channel information. Forwards are restricted. Please ensure the bot is an admin or try another method.")
                except Exception as e:
                    logger.error(f"Error adding channel by ID: {e}")
                    await m.reply(f"❌ An unexpected error occurred: {e}\nPlease try again.")
        except ValueError:
            await m.reply("❌ Invalid input. Please send a numeric Channel ID (e.g., `-1001234567890`).")
        return

    if m.forward_from_chat and user_state.get(uid, {}).get("step") == "waiting_forward":
        cid = m.forward_from_chat.id
        title = m.forward_from_chat.title
        if not title: title = "Private Channel"

        session_string = await get_session(uid)
        if not session_string:
            await m.reply("❌ User session not found. Please login again using /start.")
            user_state[uid]["step"] = "menu_home"
            return

        async with Client(
            ":memory:",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session_string,
            device_model="AutoCast Client",
            system_version="PC",
            app_version="AutoCast Version"
        ) as user_client:
            try:
                member = await user_client.get_chat_member(cid, app.me.id)
                if not member.can_post_messages:
                    await m.reply("⚠️ Warning: The bot does not have 'Post Messages' permission in this channel. Please grant it to allow scheduling.")
                if not member.can_delete_messages:
                    await m.reply("⚠️ Warning: The bot does not have 'Delete Messages' permission in this channel. Auto-deletion of posts will not work.")
            except errors.ChatAdminRequired:
                await m.reply("❌ The bot is not an administrator in this channel. Please add the bot as an administrator and grant it 'Post Messages' and 'Delete Messages' permissions.")
                return
            except Exception as e:
                logger.warning(f"Could not check bot permissions in channel {cid}: {e}")
                await m.reply("⚠️ Could not verify bot permissions in the channel. Please ensure the bot is an administrator with necessary permissions.")

        await add_channel(uid, str(cid), title)
        await m.reply(f"✅ Channel **{title}** (`{cid}`) added successfully!", reply_markup=ReplyKeyboardRemove())
        user_state[uid]["step"] = "menu_home"
        await start_cmd(c, m)
        return

    st = user_state.get(uid, {})
    step = st.get("step")

    if step == "waiting_content":
        await process_content_message(c, m, uid)
    elif step == "waiting_custom_date":
        await process_custom_date(c, m, uid)
    elif step == "waiting_broadcast_content":
        await process_broadcast_content_message(c, m, uid)
    elif text == "✅ Done Adding Posts":
        await confirm_task(m, uid, force_new=True)
    elif text == "❌ Cancel":
        if uid in user_state: del user_state[uid]
        await m.reply("Operation cancelled.", reply_markup=ReplyKeyboardRemove())
        await start_cmd(c, m)
    else:
        if not m.text.startswith("/"):
            await m.reply("I'm not sure how to handle that. Please use the menu or /start to begin.")

async def process_content_message(c, m, uid):
    st = user_state[uid]
    st["content_type"] = m.media.value if m.media else "text"
    st["content_text"] = m.caption or m.text
    st["file_id"] = m.photo.file_id if m.photo else \
                    m.video.file_id if m.video else \
                    m.audio.file_id if m.audio else \
                    m.voice.file_id if m.voice else \
                    m.document.file_id if m.document else \
                    m.animation.file_id if m.animation else \
                    m.sticker.file_id if m.sticker else None
    st["entities"] = serialize_entities(m.caption_entities or m.entities)
    st["input_msg_id"] = m.id

    if m.reply_to_message:
        st["reply_ref_id"] = m.reply_to_message.id

    await show_time_menu(m, uid)

async def process_broadcast_content_message(c, m, uid):
    st = user_state[uid]
    queue = st.get("broadcast_queue", [])

    post_data = {
        "content_type": m.media.value if m.media else "text",
        "content_text": m.caption or m.text,
        "file_id": m.photo.file_id if m.photo else \
                    m.video.file_id if m.video else \
                    m.audio.file_id if m.audio else \
                    m.voice.file_id if m.voice else \
                    m.document.file_id if m.document else \
                    m.animation.file_id if m.animation else \
                    m.sticker.file_id if m.sticker else None,
        "entities": serialize_entities(m.caption_entities or m.entities),
        "input_msg_id": m.id,
        "pin": True,
        "delete_old": True,
        "auto_delete_offset": 0,
    }

    if m.reply_to_message:
        post_data["reply_ref_id"] = m.reply_to_message.id

    queue.append(post_data)
    st["broadcast_queue"] = queue
    await m.reply(f"✅ Post #{len(queue)} added to queue. Send next post or click '✅ Done'.")

async def process_custom_date(c, m, uid):
    st = user_state[uid]
    try:
        dt = datetime.datetime.strptime(m.text, "%d-%b %I:%M %p")
        dt = IST.localize(dt)
        
        if dt < datetime.datetime.now(IST):
            await m.reply("❌ The date and time you entered is in the past. Please provide a future date and time.")
            return

        st["start_time"] = dt
        await ask_repetition(m, uid)
    except ValueError:
        await m.reply("❌ Invalid date/time format. Please use the format `DD-Mon HH:MM AM/PM` (e.g., `25-Dec 03:30 PM`).")

# --- WORKER ---
async def create_task_logic(uid, q):
    st = user_state[uid]
    targets = st.get("broadcast_targets", [st.get("target")])
    queue = st.get("broadcast_queue")

    if not queue:
        queue = [{
            "content_type": st["content_type"],
            "content_text": st["content_text"],
            "file_id": st["file_id"],
            "entities": st.get("entities"),
            "input_msg_id": 0, "reply_ref_id": None,
            "pin": st.get("pin", True),
            "delete_old": st.get("del", True),
            "auto_delete_offset": st.get("auto_delete_offset", 0) 
        }]

    base_tid = int(datetime.datetime.now().timestamp())
    t_str = st["start_time"].strftime("%d-%b %I:%M %p")
    total_tasks = 0

    for ch_idx, cid in enumerate(targets):
        batch_map = {} 
        for post_idx, post in enumerate(queue):
            tid = f"task_{base_tid}_{ch_idx}_{post_idx}"
            if "input_msg_id" in post:
                batch_map[post["input_msg_id"]] = tid

        for post_idx, post in enumerate(queue):
            tid = f"task_{base_tid}_{ch_idx}_{post_idx}"
            run_time = st["start_time"] + datetime.timedelta(seconds=post_idx * 10)
            
            target_tid = None
            if post.get("reply_ref_id") and post["reply_ref_id"] in batch_map:
                target_tid = batch_map[post["reply_ref_id"]]
            elif post.get("reply_to_old") and post_idx > 0:
                target_tid = f"task_{base_tid}_{ch_idx}_{post_idx-1}"

            task_data = {
                "task_id": tid,
                "owner_id": uid,
                "chat_id": cid,
                "content_type": post["content_type"],
                "content_text": post["content_text"],
                "file_id": post["file_id"],
                "entities": post["entities"],
                "pin": post.get("pin", st.get("pin", True)),
                "delete_old": post.get("delete_old", st.get("del", True)),
                "auto_delete_offset": post.get("auto_delete_offset", st.get("auto_delete_offset", 0)),
                "repeat_interval": st["interval"],
                "start_time": run_time.isoformat(),
                "last_msg_id": None,
                "reply_target": target_tid
            }
            
            try:
                await save_task(task_data)
                add_scheduler_job(task_data) 
                total_tasks += 1
            except Exception as e:
                logger.error(f"Task Fail: {e}")

    if "broadcast_targets" in user_state[uid]: del user_state[uid]["broadcast_targets"]
    if "broadcast_queue" in user_state[uid]: del user_state[uid]["broadcast_queue"]
    if "auto_delete_offset" in user_state[uid]: del user_state[uid]["auto_delete_offset"]

    final_txt = (f"🎉 **Broadcast Scheduled!**\n\n"
                 f"📢 **Channels:** `{len(targets)}`\n"
                 f"📬 **Posts per Channel:** `{len(queue)}`\n"
                 f"⏱️ **Post Gap:** `10 seconds` (Safe Mode)\n"
                 f"📅 **Start Time:** `{t_str}`\n\n"
                 f"👉 Click /manage to schedule more.")

    await update_menu(q.message, final_txt, None, uid, force_new=False)

def add_scheduler_job(t):
    if scheduler is None:
        return

    tid = t["task_id"]
    dt = datetime.datetime.fromisoformat(t["start_time"])
    if dt.tzinfo is None:
        dt = IST.localize(dt)

    async def job_func():
        async with queue_lock:
            logger.info(f"🚀 JOB {tid} TRIGGERED")

            # FIX #6: Always fetch fresh task data from DB so last_msg_id is current
            # The original closure captured t at creation time and was always stale
            fresh_t = await get_single_task(tid)
            if not fresh_t:
                logger.warning(f"⚠️ Job {tid}: Task not found in DB (likely deleted). Skipping.")
                return

            next_run_iso = None
            if fresh_t["repeat_interval"]:
                try:
                    last_start = datetime.datetime.fromisoformat(fresh_t["start_time"])
                    if last_start.tzinfo is None:
                        last_start = IST.localize(last_start)
                    mins = int(fresh_t["repeat_interval"].split("=")[1])
                    next_run = last_start + datetime.timedelta(minutes=mins)
                    next_run_iso = next_run.isoformat()
                except Exception as e:
                    logger.error(f"Error calculating next run for {tid}: {e}")

            session = await get_session(fresh_t['owner_id'])
            if not session:
                logger.warning(f"⚠️ Job {tid}: No session found for user {fresh_t['owner_id']}. Skipping.")
                if fresh_t["repeat_interval"] and scheduler: 
                    try: scheduler.remove_job(tid)
                    except: pass
                    await delete_task(tid)
                return

            sent = None
            # FIX #3: try now wraps the entire async with block so except/finally
            # are correctly scoped — previously they were misaligned causing
            # unhandled exceptions to propagate silently
            try:
                async with Client(
                    ":memory:",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=session,
                    device_model="AutoCast Client",
                    system_version="PC",
                    app_version="AutoCast Version"
                ) as user:

                    # FIX #2: delete_old was never executed — now actually deletes
                    # the previous message before sending the new one
                    if fresh_t["delete_old"] and fresh_t.get("last_msg_id"):
                        try:
                            await user.delete_messages(int(fresh_t['chat_id']), fresh_t['last_msg_id'])
                            logger.info(f"🗑️ Job {tid}: Deleted old msg {fresh_t['last_msg_id']}")
                        except errors.MessageDeleteForbidden:
                            logger.warning(f"⚠️ Job {tid}: Cannot delete old msg — no permission.")
                        except errors.MessageIdInvalid:
                            logger.warning(f"⚠️ Job {tid}: Old message already deleted.")
                        except Exception as e:
                            logger.warning(f"⚠️ Job {tid}: Failed to delete old msg: {e}")

                    target = int(fresh_t['chat_id'])
                    caption = fresh_t['content_text']
                    entities_objs = deserialize_entities(fresh_t['entities'])
                    reply_id = None

                    if fresh_t.get("reply_target"):
                        target_task = await get_single_task(fresh_t["reply_target"])
                        if target_task and target_task.get("last_msg_id"):
                            reply_id = target_task["last_msg_id"]
                            logger.info(f"🔗 Job {tid}: Replying to msg {reply_id} from task {fresh_t['reply_target']}")
                        else:
                            logger.warning(f"⚠️ Job {tid}: Reply target task {fresh_t['reply_target']} not found or has no last_msg_id.")

                    if fresh_t["content_type"] == "text":
                        sent = await user.send_message(target, caption, entities=entities_objs, reply_to_message_id=reply_id)
                    elif fresh_t["content_type"] == "poll":
                        poll_data = json.loads(caption)
                        sent = await user.send_poll(target, poll_data["question"], poll_data["options"], reply_to_message_id=reply_id)
                    else:
                        try:
                            if fresh_t["content_type"] == "photo":
                                sent = await user.send_photo(target, fresh_t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "video":
                                sent = await user.send_video(target, fresh_t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "animation":
                                sent = await user.send_animation(target, fresh_t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "document":
                                sent = await user.send_document(target, fresh_t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "sticker":
                                sent = await user.send_sticker(target, fresh_t["file_id"], reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "audio":
                                sent = await user.send_audio(target, fresh_t["file_id"], caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "voice":
                                sent = await user.send_voice(target, fresh_t["file_id"], caption=caption, reply_to_message_id=reply_id)
                        except errors.FileIdInvalid as f_e:
                            logger.warning(f"⚠️ Job {tid}: File ID invalid for {fresh_t['content_type']} {fresh_t['file_id']}. Attempting re-upload. Error: {f_e}")
                            media_file = await app.download_media(fresh_t["file_id"], in_memory=True)
                            if fresh_t["content_type"] == "photo":
                                sent = await user.send_photo(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "video":
                                sent = await user.send_video(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "animation":
                                sent = await user.send_animation(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "document":
                                sent = await user.send_document(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "audio":
                                media_file.name = "audio.mp3"
                                sent = await user.send_audio(target, media_file, caption=caption, caption_entities=entities_objs, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "voice":
                                media_file.name = "voice.ogg"
                                sent = await user.send_voice(target, media_file, caption=caption, reply_to_message_id=reply_id)
                            elif fresh_t["content_type"] == "sticker":
                                logger.error(f"❌ Job {tid}: Sticker re-upload from BytesIO not supported. File ID {fresh_t['file_id']} failed.")
                                sent = None

                    if sent:
                        logger.info(f"✅ Job {tid}: Message Sent! ID: {sent.id}")
                        
                        if fresh_t["pin"]:
                            try: 
                                pinned = await sent.pin()
                                if isinstance(pinned, Message): await pinned.delete()
                            except Exception as e:
                                logger.warning(f"⚠️ Job {tid}: Pin failed: {e}")
                        
                        await update_last_msg(tid, sent.id)

                        offset_mins = fresh_t.get("auto_delete_offset", 0)
                        if offset_mins and offset_mins > 0:
                            try:
                                run_at = datetime.datetime.now(IST) + datetime.timedelta(minutes=offset_mins)
                                scheduler.add_job(
                                    delete_sent_message,
                                    'date',
                                    run_date=run_at,
                                    args=[fresh_t['owner_id'], fresh_t['chat_id'], sent.id],
                                    id=f"del_{tid}_{sent.id}",
                                    misfire_grace_time=60
                                )
                                logger.info(f"⏳ Scheduled delete for Job {tid} at {run_at} ({offset_mins}m after sending)")
                            except Exception as e:
                                logger.error(f"❌ Deletion Scheduling Error: {e}")

                        if not fresh_t["repeat_interval"]:
                            await delete_task(tid)
                            logger.info(f"🗑️ One-time task {tid} deleted from DB.")

            except errors.MessageTooLong:
                logger.error(f"🔥 Job {tid} Critical: Message content too long.")
            except errors.MessageEmpty:
                logger.error(f"🔥 Job {tid} Critical: Message content is empty.")
            except errors.FloodWait as e:
                logger.warning(f"⚠️ Job {tid} FloodWait: Sleeping for {e.value} seconds.")
                await asyncio.sleep(e.value)
            except errors.ChatWriteForbidden:
                logger.error(f"🔥 Job {tid} Critical: Bot cannot write in chat {fresh_t['chat_id']}.")
            except errors.ChatAdminRequired:
                logger.error(f"🔥 Job {tid} Critical: Bot is not an admin in chat {fresh_t['chat_id']}.")
            except errors.PeerIdInvalid:
                logger.error(f"🔥 Job {tid} Critical: Invalid chat ID {fresh_t['chat_id']}.")
            except Exception as e:
                logger.error(f"🔥 Job {tid} Critical: An unexpected error occurred: {e}")
            finally:
                if next_run_iso and fresh_t["repeat_interval"]:
                    try: await update_next_run(tid, next_run_iso)
                    except: pass

    if t["repeat_interval"]:
        mins = int(t["repeat_interval"].split("=")[1])
        trigger = IntervalTrigger(start_date=dt, timezone=IST, minutes=mins)
        scheduler.add_job(job_func, trigger=trigger, id=tid, replace_existing=True, misfire_grace_time=3600, coalesce=True, max_instances=1)
    else:
        trigger = DateTrigger(run_date=dt, timezone=IST)
        scheduler.add_job(job_func, trigger=trigger, id=tid, replace_existing=True, misfire_grace_time=3600)
    
# --- STARTUP ---
async def main():
    check_env_vars()
    global queue_lock
    queue_lock = asyncio.Lock()
    
    await init_db()
    await migrate_to_v11()
    
    executors = { 'default': AsyncIOExecutor() }
    global scheduler
    scheduler = AsyncIOScheduler(timezone=IST, event_loop=asyncio.get_running_loop(), executors=executors)
    scheduler.start()
    try:
        tasks = await get_all_tasks()
        logger.info(f"📂 Loaded {len(tasks)} tasks")
        for t in tasks: add_scheduler_job(t)
    except Exception as e:
        logger.error(f"Failed to load tasks on startup: {e}")
    await app.start()
    await idle()
    await app.stop()

if __name__ == "__main__":
    app.run(main())
