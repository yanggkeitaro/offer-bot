import asyncio
import logging
import sqlite3
import os
import time
import uuid
import pandas as pd
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile, Message, BotCommand, BotCommandScopeChat, TelegramObject
from typing import Callable, Dict, Any, Awaitable
from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
SUPERADMIN_ID = int(os.getenv('SUPERADMIN_ID'))
DB_NAME = 'arbitrage_base.db'

BOT_CONFIG = {
    "log_chat_id": 0
}

ROLE_USER = 'user'
ROLE_MANAGER = 'manager'
ROLE_ADMIN = 'admin'
ROLE_SUPERADMIN = 'superadmin'
ROLE_BANNED = 'banned'

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

GEO_MAPPING = {
    'RO': 'Romania (Румыния)', 'ROMANIA': 'Romania (Румыния)', 'РУМЫНИЯ': 'Romania (Румыния)',
    'RU': 'Russia (Россия)', 'KZ': 'Kazakhstan (Казахстан)', 'UZ': 'Uzbekistan (Узбекистан)',
    'UA': 'Ukraine (Украина)', 'BY': 'Belarus (Беларусь)', 'AZ': 'Azerbaijan (Азербайджан)',
    'BR': 'Brazil (Бразилия)', 'IN': 'India (Индия)', 'TR': 'Turkey (Турция)',
    'PT': 'Portugal (Португалия)', 'ES': 'Spain (Испания)', 'PL': 'Poland (Польша)',
    'GLOBAL': 'Global (WW)', 'WW': 'Global (WW)'
}

GEO_SYNONYMS = [
    {'ro', 'romania', 'румыния'}, {'br', 'brazil', 'бразилия'}, {'ru', 'russia', 'россия'},
    {'kz', 'kazakhstan', 'казахстан'}, {'uz', 'uzbekistan', 'узбекистан'}, {'ua', 'ukraine', 'украина'},
    {'by', 'belarus', 'беларусь'}, {'az', 'azerbaijan', 'азербайджан'}, {'tr', 'turkey', 'турция'},
    {'pt', 'portugal', 'португалия'}, {'es', 'spain', 'испания'}, {'pl', 'poland', 'польша'},
    {'in', 'india', 'индия'}, {'global', 'ww', 'мир', 'весь мир'}
]


def normalize_geo(geo_input: str) -> str:
    key = geo_input.strip().upper()
    return GEO_MAPPING.get(key, geo_input.strip())


def get_search_variations(word: str) -> list:
    word_lower = word.lower()
    for group in GEO_SYNONYMS:
        if word_lower in group: return list(group)
    return [word]


def init_db():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pp_name TEXT, 
            offer_name TEXT, 
            geo TEXT,
            rate TEXT, 
            details TEXT,
            is_active BOOLEAN DEFAULT 1, 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            added_by INTEGER DEFAULT NULL
        )''')
        try:
            cursor.execute("ALTER TABLE offers ADD COLUMN added_by INTEGER DEFAULT NULL")
        except:
            pass

        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            role TEXT DEFAULT 'user', 
            username TEXT,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )''')

        cursor.execute('''CREATE TABLE IF NOT EXISTS invites (
            code TEXT PRIMARY KEY,
            role TEXT,
            uses_left INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        defaults = [('log_chat_id', '0')]
        for key, val in defaults:
            cursor.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (key, val))

        conn.commit()
        conn.close()
        load_config_from_db()
    except Exception as e:
        logging.error(f"DB Error: {e}")


def load_config_from_db():
    global BOT_CONFIG
    try:
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute('SELECT key, value FROM settings').fetchall()
        conn.close()
        for key, value in rows:
            if key in ['log_chat_id']:
                BOT_CONFIG[key] = int(value)
            else:
                BOT_CONFIG[key] = value
    except Exception as e:
        logging.error(f"Config Error: {e}")


def update_setting_db(key, value):
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, str(value)))
        conn.commit()
        conn.close()
        load_config_from_db()
    except Exception as e:
        logging.error(f"Setting Error: {e}")


def create_invite_db(role, uses):
    code = uuid.uuid4().hex[:8]
    conn = sqlite3.connect(DB_NAME)
    conn.execute('INSERT INTO invites (code, role, uses_left) VALUES (?, ?, ?)', (code, role, uses))
    conn.commit()
    conn.close()
    return code


def check_and_use_invite(code):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    row = cursor.execute('SELECT role, uses_left FROM invites WHERE code = ?', (code,)).fetchone()

    if not row:
        conn.close()
        return None

    role, uses = row
    if uses <= 0:
        conn.close()
        return None

    new_uses = uses - 1
    if new_uses == 0:
        cursor.execute('DELETE FROM invites WHERE code = ?', (code,))
    else:
        cursor.execute('UPDATE invites SET uses_left = ? WHERE code = ?', (new_uses, code))

    conn.commit()
    conn.close()
    return role


def get_user_role(user_id):
    if user_id == SUPERADMIN_ID: return ROLE_SUPERADMIN
    conn = sqlite3.connect(DB_NAME)
    res = conn.execute('SELECT role FROM users WHERE user_id = ?', (user_id,)).fetchone()
    conn.close()
    return res[0] if res else None


def add_user(user_id, username, role=ROLE_USER):
    conn = sqlite3.connect(DB_NAME)
    conn.execute('INSERT OR IGNORE INTO users (user_id, username, role) VALUES (?, ?, ?)', (user_id, username, role))
    conn.commit()
    conn.close()


def update_user_role(target_id, new_role):
    conn = sqlite3.connect(DB_NAME)
    conn.execute('UPDATE users SET role = ? WHERE user_id = ?', (new_role, target_id))
    conn.commit()
    conn.close()


def add_offer_db(data, user_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute(
        'INSERT INTO offers (pp_name, offer_name, geo, rate, details, added_by) VALUES (?, ?, ?, ?, ?, ?)',
        (data['pp_name'], data['offer_name'], data.get('geo', 'Global'), data['rate'], data.get('details', '-'),
         user_id)
    )

    conn.commit()

    new_id = cursor.lastrowid

    conn.close()
    return new_id


def update_offer_db(offer_id, data, user_id, role):
    conn = sqlite3.connect(DB_NAME)
    if role == ROLE_MANAGER:
        check = conn.execute("SELECT added_by FROM offers WHERE id = ?", (offer_id,)).fetchone()
        if not check:
            conn.close()
            return False
        if check[0] != user_id:
            conn.close()
            return "not_owner"

    sql = 'UPDATE offers SET pp_name=?, offer_name=?, geo=?, rate=?, details=? WHERE id=?'
    conn.execute(sql,
                 (data['pp_name'], data['offer_name'], data.get('geo'), data['rate'], data.get('details'), offer_id))
    conn.commit()
    conn.close()
    return True


def get_offer_by_id(offer_id):
    conn = sqlite3.connect(DB_NAME)
    row = conn.execute('SELECT pp_name, offer_name, geo, rate, details FROM offers WHERE id = ?',
                       (offer_id,)).fetchone()
    conn.close()
    return row


def check_offer_ownership_db(offer_id, user_id, role):
    if role in [ROLE_ADMIN, ROLE_SUPERADMIN]:
        return True

    conn = sqlite3.connect(DB_NAME)
    row = conn.execute("SELECT added_by FROM offers WHERE id = ?", (offer_id,)).fetchone()
    conn.close()

    if not row:
        return False

    owner_id = row[0]
    if owner_id != user_id:
        return False

    return True


def search_offers_db(query=None, show_all=False, restrict_to_user_id=None):
    conn = sqlite3.connect(DB_NAME)
    sql = 'SELECT id, pp_name, offer_name, geo, rate, details, is_active FROM offers'
    conditions = []
    params = []

    if not show_all:
        conditions.append("is_active = 1")

    if restrict_to_user_id:
        conditions.append("added_by = ?")
        params.append(restrict_to_user_id)

    if query:
        keywords = query.split()
        for word in keywords:
            variations = get_search_variations(word)
            var_conditions = []
            for var in variations:
                var_conditions.append("(pp_name LIKE ? OR offer_name LIKE ? OR geo LIKE ?)")
                params.extend([f"%{var}%", f"%{var}%", f"%{var}%"])
            if var_conditions:
                conditions.append(f"({' OR '.join(var_conditions)})")

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += ' ORDER BY id DESC'

    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        logging.error(f"Search Error: {e}")
        rows = []

    conn.close()
    return rows


def get_my_offers_db(user_id):
    conn = sqlite3.connect(DB_NAME)
    sql = 'SELECT id, pp_name, offer_name, geo, rate, details FROM offers WHERE added_by = ? AND is_active = 1 ORDER BY id DESC'
    try:
        rows = conn.execute(sql, (user_id,)).fetchall()
    except Exception as e:
        logging.error(f"My Offers Error: {e}")
        rows = []
    conn.close()
    return rows


def delete_offer_db(offer_id, user_id, role):
    conn = sqlite3.connect(DB_NAME)
    row = conn.execute(
        "SELECT pp_name, offer_name, geo, rate, details, added_by FROM offers WHERE id = ?",
        (offer_id,)
    ).fetchone()

    if not row:
        conn.close()
        return False

    pp_name, offer_name, geo, rate, details, owner_id = row

    offer_data = {
        'pp_name': pp_name,
        'offer_name': offer_name,
        'geo': geo,
        'rate': rate,
        'details': details
    }

    if role == ROLE_MANAGER:
        if owner_id != user_id:
            conn.close()
            return "not_owner"

    conn.execute('UPDATE offers SET is_active = 0 WHERE id = ?', (offer_id,))
    conn.commit()
    conn.close()

    return offer_data


def get_all_users():
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query("SELECT user_id, username, role FROM users", conn)
    conn.close()
    return df


async def update_command_menu(bot: Bot, user_id: int, role: str):
    commands_user = [
        BotCommand(command="check", description="🔎 Поиск"),
        BotCommand(command="export", description="📊 Excel"),
        BotCommand(command="help", description="ℹ️ Помощь"),
    ]

    commands_manager = [
        BotCommand(command="check", description="🔎 Поиск (Мои)"),
        BotCommand(command="my_offers", description="📋 Список (Мои)"),
        BotCommand(command="add", description="➕ Добавить"),
        BotCommand(command="edit", description="✏️ Изменить"),
        BotCommand(command="del", description="🗑 Удалить"),
        BotCommand(command="export", description="📊 Excel (Мои)"),
        BotCommand(command="help", description="ℹ️ Помощь"),
    ]

    commands_admin = [
        BotCommand(command="check", description="🔎 Поиск (Актив)"),
        BotCommand(command="check_archive", description="🗄 Поиск (Все)"),
        BotCommand(command="add", description="➕ Добавить"),
        BotCommand(command="edit", description="✏️ Изменить"),
        BotCommand(command="del", description="🗑 Удалить"),
        BotCommand(command="invite", description="🎫 Создать ссылку"),
        BotCommand(command="export", description="📊 Excel"),
        BotCommand(command="export_archive", description="🗄 Excel (Архив)"),
        BotCommand(command="help", description="ℹ️ Помощь"),
    ]

    commands_super = commands_admin + [
        BotCommand(command="users", description="👥 Люди"),
        BotCommand(command="setmanager", description="👔 Менеджер"),
        BotCommand(command="setadmin", description="👮‍♂️ Админ"),
        BotCommand(command="setuser", description="⬇️ Юзер"),
        BotCommand(command="setlog", description="📢 Лог-чат"),
        BotCommand(command="fire", description="☠️ Бан"),
        BotCommand(command="config", description="⚙️ Настр"),
    ]

    selected = commands_user
    if role == ROLE_MANAGER:
        selected = commands_manager
    elif role == ROLE_ADMIN:
        selected = commands_admin
    elif role == ROLE_SUPERADMIN:
        selected = commands_super
    elif role == ROLE_BANNED:
        selected = []

    try:
        await bot.set_my_commands(selected, scope=BotCommandScopeChat(chat_id=user_id))
    except Exception as e:
        logging.error(f"Menu Error: {e}")


async def send_log_to_chat(text: str):
    log_chat_id = BOT_CONFIG.get('log_chat_id', 0)
    if log_chat_id != 0:
        try:
            await bot.send_message(log_chat_id, text, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Failed to send log: {e}")


async def perform_search(message: Message, query: str, show_all: bool, restrict_user_id=None):
    try:
        rows = search_offers_db(query, show_all=show_all, restrict_to_user_id=restrict_user_id)

        if not rows:
            return await message.answer(f"📭 Ничего не найдено.")

        total_found = len(rows)
        LIMIT_VIEW = 20

        if total_found > LIMIT_VIEW:
            rows = rows[:LIMIT_VIEW]
            await message.answer(f"⚠️ <b>Найдено: {total_found}.</b> Первые {LIMIT_VIEW}.", parse_mode="HTML")

        res = []
        for r in rows:
            oid = r[0]
            pp_name = str(r[1] or "—")
            offer_name = str(r[2] or "—")
            geo = str(r[3] or "Global")
            rate = str(r[4] or "—")
            raw_details_db = str(r[5] or "")
            is_active = r[6]

            raw_details = raw_details_db.replace("Аппрув:", "Гарант:")

            formatted_details = ""
            if " | " in raw_details:
                try:
                    part_garant, part_info = raw_details.split(" | ", 1)
                    formatted_details = f"✅ {part_garant}\n📝 {part_info}"
                except:
                    formatted_details = f"📝 {raw_details}"
            else:
                formatted_details = f"📝 {raw_details}"

            prefix = "🗑 " if is_active == 0 else "✅ " if show_all else ""

            item_text = (
                f"{prefix}🆔 <code>{oid}</code>\n"
                f"🏢 <b>{pp_name}</b>\n"
                f"🏷 {offer_name}\n"
                f"🌍 {geo}\n"
                f"💰 {rate}\n"
                f"{formatted_details}"
            )
            res.append(item_text)

        chunk_size = 5
        for i in range(0, len(res), chunk_size):
            chunk = res[i:i + chunk_size]
            text = "\n\n➖➖➖➖➖➖➖\n\n".join(chunk)
            await message.answer(text, parse_mode="HTML")
            await asyncio.sleep(0.3)

    except Exception as e:
        logging.error(f"Search Loop Error: {e}")
        await message.answer(f"⚠️ Ошибка при отображении списка: {e}")


async def create_and_send_excel(message: Message, query: str, is_archive_mode: bool, restrict_user_id=None):
    conn = sqlite3.connect(DB_NAME)

    sql = """
    SELECT 
        t1.id, 
        t1.pp_name, 
        t1.offer_name, 
        t1.geo, 
        t1.rate, 
        t1.details, 
        t1.is_active, 
        t1.added_by,
        t2.username
    FROM offers t1
    LEFT JOIN users t2 ON t1.added_by = t2.user_id
    """

    conditions = []
    params = []

    if not is_archive_mode:
        conditions.append("t1.is_active = 1")

    if restrict_user_id:
        conditions.append("t1.added_by = ?")
        params.append(restrict_user_id)

    if query:
        keywords = query.split()
        for word in keywords:
            variations = get_search_variations(word)
            var_conditions = []
            for var in variations:
                var_conditions.append("(t1.pp_name LIKE ? OR t1.offer_name LIKE ? OR t1.geo LIKE ?)")
                params.extend([f"%{var}%", f"%{var}%", f"%{var}%"])
            if var_conditions:
                conditions.append(f"({' OR '.join(var_conditions)})")

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY t1.id DESC"

    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()

    if df.empty:
        return await message.answer(f"📭 Данных не найдено.")

    def format_user(row):
        uid = row['added_by']
        uname = row['username']

        if pd.isna(uid) or uid == 0:
            return "-"

        uid_str = str(int(uid))

        if pd.isna(uname):
            return uid_str

        return f"{uid_str} / @{uname}"

    df['added_by'] = df.apply(format_user, axis=1)

    df = df.drop(columns=['username'])

    wait_msg = await message.answer("⏳ Генерация файла...")
    fname = f"export_{int(time.time())}.xlsx"
    sheet_name = 'Offers'

    try:
        with pd.ExcelWriter(fname, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            (max_row, max_col) = df.shape

            if max_row > 0:
                worksheet.autofilter(0, 0, max_row, max_col - 1)

            worksheet.set_column(0, 0, 5)
            worksheet.set_column(1, 2, 20)
            worksheet.set_column(3, 3, 15)
            worksheet.set_column(7, 7, 25)

        mode_text = "🗄 АРХИВ" if is_archive_mode else "📊 АКТИВНЫЕ"
        if restrict_user_id: mode_text += " (МОИ)"

        caption = f"{mode_text} | Фильтр: '{query}'" if query else f"{mode_text} | Полная база"
        await message.answer_document(FSInputFile(fname), caption=caption)
    except Exception as e:
        await message.answer(f"⚠️ Ошибка экспорта: {e}")
    finally:
        await wait_msg.delete()
        if os.path.exists(fname): os.remove(fname)


class AuthMiddleware(BaseMiddleware):
    async def __call__(
            self,
            handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
            event: TelegramObject,
            data: Dict[str, Any]
    ) -> Any:
        if not isinstance(event, Message): return await handler(event, data)

        user_id = event.from_user.id

        if user_id == SUPERADMIN_ID:
            data['role'] = ROLE_SUPERADMIN
            return await handler(event, data)

        role = get_user_role(user_id)

        if role:
            if role == ROLE_BANNED:
                if event.chat.type == 'private': await event.answer("⛔️ You are Banned.")
                return
            data['role'] = role
            return await handler(event, data)

        text = event.text or ""

        if text.startswith("/start"):
            args = text.split()
            if len(args) > 1:
                invite_code = args[1]
                new_role = check_and_use_invite(invite_code)

                if new_role:
                    add_user(user_id, event.from_user.username, new_role)
                    await update_command_menu(bot, user_id, new_role)

                    icon = "👑" if new_role == ROLE_SUPERADMIN else "👮‍♂️" if new_role == ROLE_ADMIN else "💼" if new_role == ROLE_MANAGER else "👤"
                    await event.answer(
                        f"🎉 <b>Добро пожаловать!</b>\n"
                        f"Инвайт активирован. Ваша роль: {icon} <b>{new_role.upper()}</b>.\n\n"
                        f"👇 <i>Нажмите /start для начала работы.</i>",
                        parse_mode="HTML"
                    )

                    user_link = f"<a href='tg://user?id={user_id}'>{event.from_user.full_name}</a>"
                    await send_log_to_chat(f"🎫 <b>Активация инвайта!</b>\n👤 {user_link} зашел как <b>{new_role}</b>.")

                    data['role'] = new_role
                    return await handler(event, data)
                else:
                    await event.answer("⛔️ Неверная или устаревшая ссылка приглашения.")
                    return

        if event.chat.type == 'private':
            await event.answer("⛔️ Доступ запрещен. Обратитесь к администратору за ссылкой.")
            return

        return


@dp.message(Command("start"))
async def cmd_start(message: Message, role: str):
    await update_command_menu(bot, message.from_user.id, role)

    role_settings = {
        ROLE_SUPERADMIN: ("👑", "SUPERADMIN", "Полный доступ к системе и людям."),
        ROLE_ADMIN: ("👮‍♂️", "ADMIN", "Управление всей базой и архивом."),
        ROLE_MANAGER: ("💼", "MANAGER", "Управление своими офферами."),
        ROLE_USER: ("👤", "USER", "Просмотр активной базы."),
        ROLE_BANNED: ("💀", "BANNED", "Доступ ограничен.")
    }

    icon, title, desc = role_settings.get(role, ("❓", role.upper(), "-"))
    name = message.from_user.first_name

    text = (
        f"👋 <b>Привет, {name}!</b>\n\n"
        f"Ваш статус: {icon} <b>{title}</b>\n"
        f"<i>{desc}</i>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
    )

    if role == ROLE_USER:
        text += (
            "🔎 <b>Поиск:</b> <code>/check -</code>\n"
            "📊 <b>Выгрузка:</b> <code>/export -</code>"
        )
    elif role in [ROLE_MANAGER, ROLE_ADMIN, ROLE_SUPERADMIN]:
        text += (
            "➕ <b>Добавить:</b> <code>/add</code>\n"
            "🔎 <b>Поиск:</b> <code>/check -</code>\n"
            "📊 <b>Отчет:</b> <code>/export -</code>\n"
        )
        if role != ROLE_MANAGER:
            text += "🗄 <b>Архив:</b> <code>/check_archive -</code>"

    text += "\n\nℹ️ <i>Используйте меню для всех команд.</i>"

    await message.answer(text, parse_mode="HTML")


@dp.message(Command("invite"))
async def cmd_invite(message: Message, role: str):
    if role not in [ROLE_SUPERADMIN, ROLE_ADMIN]:
        return await message.answer("⛔️ У вас нет прав создавать инвайты.")

    args = message.text.split()
    if len(args) < 2:
        return await message.answer(
            "🎫 <b>Генерация ссылок:</b>\n"
            "<code>/invite manager</code> (создать 1 ссылку)\n"
            "<code>/invite user 10</code> (создать 10 разных ссылок)\n\n"
            "<i>* Каждая ссылка всегда одноразовая.</i>",
            parse_mode="HTML"
        )

    target_role = args[1].lower()
    if target_role not in [ROLE_MANAGER, ROLE_USER, ROLE_ADMIN]:
        return await message.answer("⚠️ Роли: manager, user, admin")

    count = 1
    if len(args) > 2:
        try:
            count = int(args[2])
        except:
            pass

    if count > 50:
        count = 50
        await message.answer("⚠️ Ограничение: максимум 50 штук за раз.")

    bot_info = await bot.get_me()
    base_url = f"https://t.me/{bot_info.username}?start="

    links = []

    for _ in range(count):
        code = create_invite_db(target_role, 1)
        links.append(f"{base_url}{code}")

    if count == 1:
        await message.answer(
            f"✅ <b>Ссылка создана!</b>\n"
            f"Роль: {target_role.upper()}\n"
            f"Тип: Одноразовая\n\n"
            f"{links[0]}",
            parse_mode="HTML"
        )
    else:
        links_text = "\n".join(links)
        header = (
            f"✅ <b>Сгенерировано ссылок: {count}</b>\n"
            f"Роль: {target_role.upper()}\n"
            f"Каждая ссылка действует 1 раз.\n"
            f"➖➖➖➖➖➖➖➖➖➖"
        )
        await message.answer(f"{header}\n{links_text}", parse_mode="HTML")


@dp.message(Command("help"))
async def cmd_help(message: Message, role: str):
    header = (
        f"🤖 <b>Система Управления Офферами</b>\n"
        f"👋 Ваша роль: <b>{role.upper()}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
    )

    section_search = (
        "🔎 <b>Поиск и Просмотр:</b>\n"
        "• <code>/check 1win</code> — Найти офферы по слову\n"
        "• <code>/check -</code> — Показать последние активные\n"
    )
    if role == ROLE_MANAGER:
        section_search += "<i>(Поиск ищет только по вашим личным офферам)</i>\n"
    elif role == ROLE_USER:
        section_search += "<i>(Поиск по всей активной базе)</i>\n"

    section_search += "\n"

    section_manager = ""
    if role in [ROLE_MANAGER, ROLE_ADMIN, ROLE_SUPERADMIN]:
        access_note = "<i>(Только свои)</i>" if role == ROLE_MANAGER else "<i>(Любые)</i>"

        section_manager = (
            f"💼 <b>Управление {access_note}:</b>\n"
            "• <code>/add ...</code> — Добавить оффер\n"
            "• <code>/edit ID</code> — Изменить (получить строку)\n"
            "• <code>/del ID</code> — Удалить в архив\n"
            "• <code>/my_offers</code> — Список моих активных\n"
            "• <code>/export -</code> — Скачать Excel-отчет\n\n"
            "📝 <b>Формат добавления:</b>\n"
            "<code>/add ПП - Оффер - Гео - Ставка - Гарант (0 если нет) - Инфо</code>\n"
            "<i>Пример:</i> <code>/add 1win - Aviator - RO - 45$ - 5 cap - Тест</code>\n\n"
        )

    section_admin = ""
    if role in [ROLE_ADMIN, ROLE_SUPERADMIN]:
        section_admin = (
            "👑 <b>Администрирование:</b>\n"
            "• <code>/check_archive -</code> — Поиск по Архиву\n"
            "• <code>/export_archive -</code> — Скачать Архив (Excel)\n"
            "• <code>/del ID</code> — Удаление любого оффера\n\n"
            "• <code>/invite manager</code> — Создать инвайт (1 вход)\n"
            "• <code>/invite user 10</code> — Инвайт на 10 входов\n"
        )

    section_super = ""
    if role == ROLE_SUPERADMIN:
        section_super = (
            "⚙️ <b>Системное управление:</b>\n"
            "• <code>/users</code> — Список всех пользователей\n"
            "• <code>/fire ID</code> — Забанить/Разбанить\n"
            "• <code>/setmanager ID</code> — Назначить Менеджером\n"
            "• <code>/setadmin ID</code> — Назначить Админом\n"
            "• <code>/setlog</code> — Назначить этот чат для Логов\n"
        )

    text = header + section_search + section_manager + section_admin + section_super

    await message.answer(text, parse_mode="HTML")


@dp.message(Command("add"))
async def cmd_add(message: Message, role: str):
    if role not in [ROLE_ADMIN, ROLE_SUPERADMIN, ROLE_MANAGER]:
        return await message.answer("⛔️ У вас нет прав на добавление.")

    try:
        args = message.text.split(maxsplit=1)
        if len(args) == 1:
            return await message.answer(
                "➕ <b>Добавление оффера</b>\n\n"
                "Формат (разделитель « - » или длинное тире «—»):\n"
                "<code>/add ПП - Оффер - Гео - Ставка - Гарант (0 если нет) - Инфо</code>\n"
                "или\n"
                "<code>/add ПП—Оффер—Гео—Ставка—0—Инфо</code>",
                parse_mode="HTML"
            )

        raw_text = args[1]
        raw_text = raw_text.replace('—', ' - ')
        parts = [p.strip() for p in raw_text.split(' - ')]

        if len(parts) < 6:
            return await message.answer(
                "⚠️ <b>Ошибка формата!</b>\n"
                "Используйте разделитель « - ».\n"
                f"Я нашел частей: {len(parts)} из 6."
            )

        if len(parts) > 6:
            parts[5] = " - ".join(parts[5:])
            parts = parts[:6]

        pp, off, geo, rate, gar, com = parts

        details_db = f"Гарант: {gar} | {com}" if gar not in ['0', '-', '', 'нет'] else com

        data = {
            'pp_name': pp,
            'offer_name': off,
            'geo': normalize_geo(geo),
            'rate': rate,
            'details': details_db
        }

        new_id = add_offer_db(data, message.from_user.id)

        await message.answer(f"✅ <b>OK!</b> {pp} | {off} (ID: {new_id})", parse_mode="HTML")

        if message.chat.type == 'private':
            user_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"

            if gar not in ['0', '-', '', 'нет']:
                details_log = f"✅ Гарант: {gar}\n📝 {com}"
            else:
                details_log = f"📝 {com}"

            log_text = (
                f"🆕 <b>Новый оффер!</b>\n"
                f"👤 {user_link} (ID {message.from_user.id})\n\n"
                f"🆔 <code>{new_id}</code>\n"
                f"🏢 <b>{pp}</b>\n"
                f"🏷 {off}\n"
                f"🌍 {normalize_geo(geo)}\n"
                f"💰 {rate}\n"
                f"{details_log}"
            )
            await send_log_to_chat(log_text)

        try:
            safe_log = f"ADD OFFER: {pp} - {off}".encode('utf-8', 'ignore').decode('utf-8')
            print(f"INFO: {safe_log}")
        except:
            print("INFO: New offer added")

    except Exception as e:
        logging.error(f"Add Error: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("edit"))
async def cmd_edit(message: Message, role: str):
    if role not in [ROLE_ADMIN, ROLE_SUPERADMIN, ROLE_MANAGER]:
        return await message.answer("⛔️ У вас нет прав на редактирование.")

    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        return await message.answer("⚠️ Пример: <code>/edit 123</code>", parse_mode="HTML")

    try:
        offer_id = int(args[1])
    except:
        return await message.answer("⚠️ ID должен быть числом.")

    can_touch = check_offer_ownership_db(offer_id, message.from_user.id, role)
    if not can_touch:
        return await message.answer("⛔️ Вы можете редактировать только <b>свои</b> офферы.", parse_mode="HTML")

    if len(args) == 2:
        row = get_offer_by_id(offer_id)
        if not row: return await message.answer("❌ Оффер не найден.")

        details = row[4]
        garant = "0"
        comment = details

        if " | " in details:
            try:
                g_part, c_part = details.split(" | ", 1)
                garant = g_part.replace("Гарант:", "").replace("Аппрув:", "").strip()
                comment = c_part
            except:
                pass

        edit_string = f"{row[0]} - {row[1]} - {row[2]} - {row[3]} - {garant} - {comment}"

        await message.answer(
            f"✏️ <b>Редактирование {offer_id}:</b>\n\n"
            f"Скопируйте, измените и отправьте:\n"
            f"<code>/edit {offer_id} {edit_string}</code>",
            parse_mode="HTML"
        )
        return

    text_to_process = args[2]
    parts = [p.strip() for p in text_to_process.split(' - ')]
    if len(parts) < 6:
        return await message.answer(
            "⚠️ <b>Ошибка формата!</b>\n"
            "Используйте разделитель « - ».\n\n"
            "✅ <b>Пример:</b>\n"
            "<code>/edit 123 1win - Aviator - RO - 40$ - 0 - Тест</code>",
            parse_mode="HTML"
        )
    if len(parts) > 6:
        parts[5] = " - ".join(parts[5:])
        parts = parts[:6]

    pp, off, geo, rate, gar, com = parts
    details = ""
    if gar not in ['0', '-', '', 'нет']:
        details += f"Гарант: {gar} | "
    details += com

    data = {'pp_name': pp, 'offer_name': off, 'geo': normalize_geo(geo), 'rate': rate, 'details': details}

    result = update_offer_db(offer_id, data, message.from_user.id, role)

    if result == True:
        await message.answer(f"✅ Оффер {offer_id} обновлен!")
        if message.chat.type == 'private':
            user_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"

            if gar not in ['0', '-', '', 'нет']:
                details_log = f"✅ Гарант: {gar}\n📝 {com}"
            else:
                details_log = f"📝 {com}"

            log_text = (
                f"✏️ <b>Изменение оффера!</b>\n"
                f"👤 {user_link}\n\n"
                f"🆔 <code>{offer_id}</code>\n"
                f"🏢 <b>{pp}</b>\n"
                f"🏷 {off}\n"
                f"🌍 {normalize_geo(geo)}\n"
                f"💰 {rate}\n"
                f"{details_log}"
            )
            await send_log_to_chat(log_text)

    elif result == "not_owner":
        await message.answer("⛔️ Вы можете менять только свои офферы.")
    else:
        await message.answer("❌ Оффер не найден.")


@dp.message(Command("my_offers"))
async def cmd_my_offers(message: Message, role: str):
    if role not in [ROLE_MANAGER, ROLE_ADMIN, ROLE_SUPERADMIN]: return

    rows = get_my_offers_db(message.from_user.id)

    if not rows:
        return await message.answer("📭 Вы еще ничего не добавили.")

    res = []
    for r in rows:
        res.append(f"🆔<code>{r[0]}</code> <b>{r[1]}</b>: {r[2]} (🌍 {r[3]}) — <b>{r[4]}</b> | {r[5]}")

    header = f"📋 <b>Ваши активные офферы ({len(rows)}):</b>\n\n"
    text = header + "\n\n".join(res)

    if len(text) > 4000:
        await message.answer(text[:4000] + "...\n(Список обрезан)", parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")


@dp.message(Command("del"))
async def cmd_del(message: Message, role: str):
    if role not in [ROLE_ADMIN, ROLE_SUPERADMIN, ROLE_MANAGER]:
        return await message.answer("⛔️ У вас нет прав на удаление.")

    try:
        args = message.text.split()
        if len(args) < 2:
            return await message.answer("⚠️ Пример: <code>/del 123</code>", parse_mode="HTML")

        oid = int(args[1])
        res = delete_offer_db(oid, message.from_user.id, role)

        if res == False:
            await message.answer(f"⚠️ Оффер <code>{oid}</code> не найден.", parse_mode="HTML")
        elif res == "not_owner":
            await message.answer("⛔️ Вы не можете удалять чужие офферы.")
        else:
            info_text = (
                f"🗑 <b>Оффер удален в архив:</b>\n\n"
                f"🆔 <code>{oid}</code>\n"
                f"🏷 <b>{res['pp_name']}</b> — {res['offer_name']}\n"
                f"🌍 {res['geo']}\n"
                f"💰 {res['rate']}\n"
                f"📝 {res['details']}"
            )
            await message.answer(info_text, parse_mode="HTML")

            if message.chat.type == 'private':
                user_link = f"<a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>"
                log_text = (
                    f"🗑 <b>Удаление оффера!</b>\n"
                    f"👤 {user_link}\n\n"
                    f"🆔 <code>{oid}</code>\n"
                    f"🏷 {res['pp_name']} | {res['offer_name']}\n"
                    f"🌍 {res['geo']} | 💰 {res['rate']}\n"
                    f"📝 {res['details']}"
                )
                await send_log_to_chat(log_text)

    except ValueError:
        await message.answer("⚠️ ID должен быть числом.")
    except Exception as e:
        logging.error(f"Del Error: {e}")
        await message.answer("⚠️ Ошибка при удалении.")


@dp.message(Command("check", "search", "check_archive"))
async def cmd_check(message: Message, role: str):
    full_text = message.text
    if '@' in full_text.split()[0]:
        command_part = full_text.split()[0].split('@')[0]
        args_part = full_text.split(maxsplit=1)[1] if len(full_text.split()) > 1 else ""
        full_text = f"{command_part} {args_part}".strip()

    parts = full_text.split(maxsplit=1)
    is_archive = "archive" in parts[0]

    if len(parts) == 1:
        cmd = "/check_archive" if is_archive else "/check"
        return await message.reply(f"⚠️ Формат: <code>{cmd} текст</code>", parse_mode="HTML")

    q = parts[1].strip()
    if q in ['-', '.', 'все', 'all']: q = None

    restrict_uid = None
    if role == ROLE_MANAGER:
        restrict_uid = message.from_user.id

    await perform_search(message, q, show_all=is_archive, restrict_user_id=restrict_uid)


@dp.message(Command("export", "export_archive"))
async def cmd_export(message: Message, role: str):
    parts = message.text.split(maxsplit=1)
    is_archive = "archive" in parts[0]

    if role == ROLE_USER and is_archive:
        return await message.answer("⛔️ Архив доступен только администраторам.")

    if len(parts) == 1:
        cmd = "/export_archive" if is_archive else "/export"
        return await message.reply(f"⚠️ Формат: <code>{cmd} -</code>", parse_mode="HTML")

    q = parts[1].strip()
    if q in ['-', '.', 'все', 'all']: q = None

    restrict_uid = None
    if role == ROLE_MANAGER:
        restrict_uid = message.from_user.id

    await create_and_send_excel(message, query=q, is_archive_mode=is_archive, restrict_user_id=restrict_uid)


@dp.message(Command("config"))
async def cmd_config(message: Message, role: str):
    if role != ROLE_SUPERADMIN: return
    await message.answer(f"⚙️ LogChat: {BOT_CONFIG['log_chat_id']}", parse_mode="HTML")


@dp.message(Command("setlog"))
async def cmd_setlog(message: Message, role: str):
    if role != ROLE_SUPERADMIN: return
    chat_id = message.chat.id
    update_setting_db('log_chat_id', chat_id)
    await message.answer(f"✅ Логи будут приходить сюда (ID: {chat_id}).")


@dp.message(Command("users"))
async def cmd_users(message: Message, role: str):
    if role != ROLE_SUPERADMIN: return
    df = get_all_users()
    if df.empty: return await message.answer("Пусто.")
    res = [f"🆔{r['user_id']} | {ROLE_SUPERADMIN if r['user_id'] == SUPERADMIN_ID else r['role']} | @{r['username']}" for
           _, r in df.iterrows()]
    await message.answer("\n".join(res))


@dp.message(Command("setmanager"))
async def cmd_setmanager(message: Message, role: str):
    if role != ROLE_SUPERADMIN: return
    try:
        uid = int(message.text.split()[1])
        update_user_role(uid, ROLE_MANAGER)
        await update_command_menu(bot, uid, ROLE_MANAGER)
        await message.answer(f"✅ {uid} -> MANAGER.")
    except:
        await message.answer("Пример: /setmanager 12345")


@dp.message(Command("setadmin"))
async def cmd_setadmin(message: Message, role: str):
    if role != ROLE_SUPERADMIN: return
    try:
        uid = int(message.text.split()[1])
        update_user_role(uid, ROLE_ADMIN)
        await update_command_menu(bot, uid, ROLE_ADMIN)
        await message.answer(f"✅ {uid} -> ADMIN.")
    except:
        await message.answer("Пример: /setadmin 12345")


@dp.message(Command("setuser"))
async def cmd_setuser(message: Message, role: str):
    if role != ROLE_SUPERADMIN: return
    try:
        uid = int(message.text.split()[1])
        if uid == SUPERADMIN_ID: return
        update_user_role(uid, ROLE_USER)
        await update_command_menu(bot, uid, ROLE_USER)
        await message.answer(f"⬇️ {uid} -> USER (Общий поиск).")
    except:
        await message.answer("Пример: /setuser 12345")


@dp.message(Command("fire"))
async def cmd_fire(message: Message, role: str):
    if role != ROLE_SUPERADMIN: return
    try:
        uid = int(message.text.split()[1])
        if uid == SUPERADMIN_ID: return await message.answer("🗿 Себя нельзя.")

        cur = get_user_role(uid) or ROLE_USER
        if cur == ROLE_BANNED:
            update_user_role(uid, ROLE_USER)
            await update_command_menu(bot, uid, ROLE_USER)
            await message.answer(f"😇 {uid} Разбанен.")
            try:
                await bot.send_message(uid, "✅ Бан снят.")
            except:
                pass
        else:
            update_user_role(uid, ROLE_BANNED)
            try:
                await bot.set_my_commands([], scope=BotCommandScopeChat(chat_id=uid))
            except:
                pass
            await message.answer(f"💀 {uid} Забанен.")
            try:
                await bot.send_message(uid, "⛔️ Вы забанены.")
            except:
                pass
    except:
        await message.answer("Ошибка.")


async def main():
    print("🚀 Bot started (v4 with Invites & Logs).")
    init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    dp.message.outer_middleware(AuthMiddleware())
    try:
        await update_command_menu(bot, SUPERADMIN_ID, ROLE_SUPERADMIN)
    except:
        pass
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")
