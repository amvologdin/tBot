import os
import json
import time
import threading
from datetime import datetime, timedelta

import asyncio
import crcmod.predefined
import gspread
import numpy as np
import pandas as pd
import pytz
import telebot
from oauth2client.service_account import ServiceAccountCredentials
from telebot import TeleBot
from telebot.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

from flask import Flask
app = Flask(__name__)

@app.route("/", methods=["GET", "HEAD"])
def index():
    return "OK", 200

# ==============================
#  Общие утилиты и настройки
# ==============================

def is_running_in_docker() -> bool:
    """Проверяет, запущен ли скрипт внутри Docker-контейнера."""
    if os.path.exists("/.dockerenv"):
        return True
    try:
        with open("/proc/self/cgroup", "r") as f:
            if "docker" in f.read():
                return True
    except FileNotFoundError:
        pass
    return False

MONTHS_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

# ---------- Загрузка сервисного ключа Google с вариативностью ----------

sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

if sa_json:
    # JSON сервисного аккаунта в переменной окружения
    creds_dict = json.loads(sa_json)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    spreadsheet_id = creds_dict["spreadsheet_id"]
    TOKEN = creds_dict["botTOKEN"]
    print("Используем ключ из переменной окружения GOOGLE_SERVICE_ACCOUNT_JSON")
else:
    # Локальный JSON-файл
    key_path = os.getenv("KEY_PATH")
    if key_path:
        key_file = key_path
        print(f"Используем ключ из KEY_PATH: {key_path}")
    else:
        key_file = "KEY/tb-fabric-dev.json"
        print(f"Используем ключ по умолчанию: {key_file}")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(key_file, scope)

    with open(key_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    spreadsheet_id = data["spreadsheet_id"]
    TOKEN = data["botTOKEN"]

welcome_message = (
    "Я твой личный помощник для ведения учета рабочего времени и отчетности.\n\n"
    "После каждого рабочего дня, я буду ждать тебя с отчетом о проделанной работе. "
    "Это поможет тебе не только точно отслеживать свой рабочий график, но и получать "
    "заработанные деньги вовремя.\n\n"
    "Но самое главное, не забывай отдыхать и заботиться о своем физическом и "
    "эмоциональном здоровье — это тоже очень важно!"
)


def get_anekdot() -> str:
    """Получение анекдота с сайта, при ошибке — fallback."""
    # Справочник запретных слов и тем (можно расширить)
    FORBIDDEN_WORDS = {
        # Политика
        'путин', 'байден', 'трамп', 'кириенко', 'медведев', 'навальный', 'зеленский',
        'президент', 'выборы', 'правительство', 'олигарх', 'депутат', 'министр',
        'москва', 'kremlin', 'украина', 'россия', 'russia', 'украин', 'русск',
        # Война и конфликты
        'война', 'войны', 'войну', 'военный', 'солдат', 'танки', 'бомба', 'бомбы',
        'снайпер', 'дрон', 'оружие', 'армия', 'front', 'war', 'nato', 'natoс',
        # Фашизм и экстремизм
        'фашист', 'фашизм', 'нацист', 'нацизм', 'гитлер', 'гитлера', 'сталин',
        'еврей', 'жид', 'негр', 'мусульманин', 'террорист',
        # Другие чувствительные темы
        'убийство', 'убийц', 'геноцид', 'хохол', 'укроп', 'ватник', 'либерал',
        'коммунизм', 'капитализм', 'империализм'
    }
    
    def is_clean(text: str) -> bool:
        """Проверяет, не содержит ли текст запретных слов (регистронезависимо)."""
        text_lower = text.lower()
        return not any(word in text_lower for word in FORBIDDEN_WORDS)
    
    try:
        import requests
        from bs4 import BeautifulSoup  # type: ignore

        url = "https://www.anekdot.ru/random/anekdot/"
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        topicboxes = soup.find_all("div", {"class": ["content content-min", "topicbox"]})

        anecdotes = []
        for topicbox in topicboxes:
            votingbox = topicbox.find("div", {"class": "votingbox"})
            if not votingbox:
                continue
            btn2 = votingbox.find("div", {"class": "btn2"})
            if not btn2:
                continue
            user_star = btn2.find("a", {"class": "user-star"})
            if not user_star:
                continue
            rate = user_star.text.count("★")
            text_div = topicbox.find("div", {"class": "text"})
            if not text_div:
                continue
            text = "".join(str(item) for item in text_div.contents)
            
            # Фильтрация по справочнику сразу при сборе
            if is_clean(text):
                anecdotes.append({"rate": rate, "text": text})

        if not anecdotes:
            return "Шутки нет 🤷‍♂️"

        # Берем лучший из отфильтрованных
        best_one = sorted(anecdotes, key=lambda x: x["rate"], reverse=True)[0]["text"]
        return "Держи случайную шутку с anekdot.ru:\n" + best_one.replace("<br/>", "\n") 
    
    except Exception:
        return "Шутки нет 🤷‍♂️"


# ==============================
#  Google Sheets: диапазоны
# ==============================

settings_sheet_name = "Настройки"
settings_range = "A2:H"

results_sheet_name = "Результативность"
results_range = "J2:O"

goals_sheet_name = "Целевые показатели"
goals_range = "A2:B"

questions_sheet_name = "Операции"
operations_detail_range = "A2:H"
models_range = "J2:J"
questions_range = "J2:K"
questions_detail_range = "U2:AB"

answers_sheet_name = "Отчет"
answer_range = "A2:H"

logs_sheet_name = "LOGS"
logs_range = "A2:H"

update_period = 300  # сек


gc = gspread.authorize(creds).open_by_key(spreadsheet_id)
settings_sheet = gc.worksheet(settings_sheet_name)
models_sheet = gc.worksheet(questions_sheet_name)
questions_sheet = gc.worksheet(questions_sheet_name)
questions_detail_sheet = gc.worksheet(questions_sheet_name)
answers_sheet = gc.worksheet(answers_sheet_name)
goals_sheet = gc.worksheet(goals_sheet_name)
logs_sheet = gc.worksheet(logs_sheet_name)
results_sheet = gc.worksheet(results_sheet_name)

# Кеши и время обновления
goals_last_update = datetime(2023, 1, 1, 23, 59, 59)
questions_detail_last_update = datetime(2023, 1, 1, 23, 59, 59)
questions_last_update = datetime(2023, 1, 1, 23, 59, 59)
models_last_update = datetime(2023, 1, 1, 23, 59, 59)
settings_last_update = datetime(2023, 1, 1, 23, 59, 59)
answers_last_update = datetime(2023, 1, 1, 23, 59, 59)
results_last_update = datetime(2023, 1, 1, 23, 59, 59)
operations_detail_last_update = datetime(2023, 1, 1, 23, 59, 59)

settings = []
models = []
questions = []
questions_detail = []
operations_detail = []
answers = []
results = []
goals = []


def reload_data(message=None, scope="m+q+qd+g+s", force=False, silent=False):
    """Универсальная функция подгрузки данных из таблиц."""
    global models_sheet, models
    global questions_sheet, questions
    global questions_detail_sheet, questions_detail
    global operations_detail
    global goals_sheet, goals
    global settings_sheet, settings
    global goals_last_update, questions_detail_last_update
    global questions_last_update, models_last_update, settings_last_update
    global answers_last_update, results_last_update, operations_detail_last_update
    global answers, results, results_sheet

    if message and not silent:
        info_msg = bot.send_message(message.chat.id, "Загружаю данные...")
    else:
        info_msg = None

    current_time = datetime.today()

    for s in scope.split("+"):
        if s == "s":
            if (current_time - settings_last_update).total_seconds() > update_period or force:
                settings = settings_sheet.get(settings_range)
                settings_last_update = current_time
        if s == "m":
            if (current_time - models_last_update).total_seconds() > update_period or force:
                models = models_sheet.get(models_range)
                models_last_update = current_time
        if s == "q":
            if (current_time - questions_last_update).total_seconds() > update_period or force:
                raw_questions = questions_sheet.get(questions_range)
                for row in raw_questions:
                    if len(row) < 2:
                        continue
                    groups = row[1].split(";")
                    row[1:] = groups
                questions[:] = raw_questions
                questions_last_update = current_time
        if s == "qd":
            if (current_time - questions_detail_last_update).total_seconds() > update_period or force:
                questions_detail = questions_detail_sheet.get(questions_detail_range)
                questions_detail_last_update = current_time
        if s == "od":
            operations_detail = questions_sheet.get(operations_detail_range)
            operations_detail_last_update = current_time
        if s == "g":
            if (current_time - goals_last_update).total_seconds() > update_period or force:
                goals = goals_sheet.get(goals_range)
                goals_last_update = current_time
        if s == "a":
            answers = answers_sheet.get(answer_range)
            answers_last_update = current_time
        if s == "r":
            if (current_time - results_last_update).total_seconds() > update_period or force:
                results = results_sheet.get(results_range)
                results_last_update = current_time

    if info_msg and not silent:
        bot.delete_message(info_msg.chat.id, info_msg.message_id)


def get_setting(message=None, name=None):
    """Получение значений настроек по имени."""
    if not name:
        return []
    names = name.split("+")
    if message:
        info = bot.send_message(message.chat.id, "Загружаю настройки...")
    else:
        info = None

    reload_data(scope="s")

    if info:
        bot.delete_message(info.chat.id, info.message_id)

    return list(filter(lambda x: x and x[0] in names, settings))


# ==============================
#  CRC и поиск по ID
# ==============================

ACTION_PREFIX = "action_"
QUANTITY_PREFIX = "quantity_"
MODEL_PREFIX = "model_"


def checksum(s: str) -> str:
    """CRC32 для строковых идентификаторов."""
    crc32_func = crcmod.predefined.mkCrcFun("crc-32")
    return str(crc32_func(s.encode("utf-8")))


def model_by_id(model_id: str):
    """Поиск названия модели по её CRC."""
    for m in set(map(tuple, models)):
        if m and m[0] != "@last_update" and checksum(m[0]) == model_id:
            return m[0]
    return None


def question_detail_by_id(question_id: str):
    """Поиск текста операции по её CRC."""
    for qd in set(map(tuple, questions_detail)):
        if qd and qd[0] != "@last_update" and len(qd) > 2 and checksum(qd[2]) == question_id:
            return qd[2]
    return None


class MessageUserSet(set):
    """Множество (chat_id, message_id), чтобы не обрабатывать callback дважды."""

    def add(self, value):
        new_message_id = value["message_id"]
        new_user_id = value["chat_id"]
        for record in self:
            rec = dict(record)
            if rec["chat_id"] == new_user_id:
                super().discard(record)
                break
        super().add(frozenset({"message_id": new_message_id, "chat_id": new_user_id}.items()))


message_user_set = MessageUserSet()


# ==============================
#  Бот и состояние пользователя
# ==============================

class MyBot(TeleBot):
    def __init__(self, token):
        super().__init__(token)
        # user_data[chat_id] = {
        #     "state": "...",
        #     "model": "...",
        #     "operation": "...",
        # }
        self.user_data = {}


bot = MyBot(TOKEN)


# ==============================
#  Клавиатуры
# ==============================

def build_main_reply_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    """Главное меню в виде reply‑клавиатуры с учетом прав админа."""
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    
    kb.row(
        KeyboardButton("📝 Заполнить отчет"),
        KeyboardButton("📊 Результат за день"),
    )
    kb.row(
        KeyboardButton("📅 Результат за месяц"),
        KeyboardButton("🕒 Последние 3 операции"),
    )
    
    # Анекдот + Админ в одном ряду
    if is_admin(user_id=user_id):
        kb.row(
            KeyboardButton("🎭 Анекдот"),
            KeyboardButton("🔧 Админ")
        )
    else:
        kb.row(KeyboardButton("🎭 Анекдот"))
    
    return kb


def get_def_buttons(show_all: int = 1) -> InlineKeyboardMarkup:
    """Основное inline‑меню (как в оригинале)."""
    buttons_list = [[InlineKeyboardButton("Заполнить отчет", callback_data="DEFAULT_DO")]]
    if show_all:
        buttons_list.append(
            [InlineKeyboardButton("Посмотреть результат за день", callback_data="DEFAULT_CALCULATE")]
        )
    return InlineKeyboardMarkup(buttons_list)


def get_buttons_with_models(message) -> InlineKeyboardMarkup:
    """Inline‑клавиатура с уникальными моделями."""
    keyboard_buttons = []

    models_tuples = [tuple(model) for model in models]
    unique_models = [list(m) for m in set(models_tuples)]
    unique_models.sort()

    for m in unique_models:
        if m and m[0] != "@last_update":
            keyboard_buttons.append(
                [InlineKeyboardButton(m[0], callback_data=MODEL_PREFIX + checksum(m[0]))]
            )
    keyboard_buttons.append(
        [InlineKeyboardButton("-Закрыть отчет-", callback_data=MODEL_PREFIX + "@QuitAndSave")]
    )
    return InlineKeyboardMarkup(keyboard_buttons)


def get_buttons_with_questions(path: str, message=None) -> InlineKeyboardMarkup | None:
    """Inline‑клавиатура с группами операций."""
    parts = path.split("_")
    model_crc = parts[0]
    group_crc = parts[1] if len(parts) > 1 else parts[0]
    level = int(parts[2]) if len(parts) > 2 else 1

    questions_set = set()
    for q in questions:
        if not q:
            continue
        if len(q) < 1:
            continue
        if checksum(q[0]) != model_crc:
            continue
        if len(q) < level + 1:
            continue
        group_str = ";".join(q[1 if level > 1 else 0 : 1 + (level - 1 if level > 1 else 0)])
        if checksum(group_str) != group_crc:
            continue
        questions_set.add(";".join(q[1 : 1 + level]))

    if not questions_set:
        return None

    buttons = []
    for q in sorted(questions_set):
        label = q.split(";")[-1]
        cb = ACTION_PREFIX + model_crc + "_" + checksum(q) + "_" + str(level + 1)
        buttons.append([InlineKeyboardButton(label, callback_data=cb)])
    buttons.append([InlineKeyboardButton("-Закрыть отчет-", callback_data=ACTION_PREFIX + "@QuitAndSave")])
    return InlineKeyboardMarkup(buttons)


def get_buttons_with_questions_detail(path: str, message=None) -> InlineKeyboardMarkup:
    """Inline‑клавиатура с конкретными операциями (деталями)."""
    parts = path.split("_")
    model_crc = parts[0]
    group_crc = parts[1] if len(parts) > 1 else parts[0]

    buttons = []
    for qd in questions_detail:
        if not qd or len(qd) <= 2:
            continue
        if checksum(qd[0]) != model_crc:
            continue
        if checksum(qd[1]) != group_crc:
            continue
        cb = QUANTITY_PREFIX + model_crc + "_" + checksum(qd[2])
        buttons.append([InlineKeyboardButton(qd[2], callback_data=cb)])
    buttons.append(
        [InlineKeyboardButton("-Закрыть отчет-", callback_data=QUANTITY_PREFIX + "@QuitAndSave")]
    )
    return InlineKeyboardMarkup(buttons)


# ==============================
#  Вспомогательные функции
# ==============================

def is_admin(message=None, user_id=None) -> bool:
    """Проверка, что текущий пользователь — администратор."""
    # Определяем user_id из параметров
    if user_id is None:
        if message is None:
            return False
        user_id = message.from_user.id

    reload_data(scope="s")
    # Проверяем в настройках
    for s in settings:
        if (s and len(s) >= 2 and 
            s[0] == "Администратор" and 
            s[1] == str(user_id)):
            return True
    return False


def get_last_rows(user_id: int, count: int = 5):
    """Последние N операций пользователя из листа ответов."""
    if not user_id:
        return []

    reload_data(scope="a")

    df = pd.DataFrame(
        answers,
        columns=[
            "Сотрудник имя",
            "Сотрудник ID",
            "Модель",
            "Операция",
            "Количество",
            "Дата",
            "Время",
            "Признак удаления",
        ],
    )
    df = df.loc[lambda x: x["Сотрудник ID"] == str(user_id)]
    df = df.loc[lambda x: x["Признак удаления"].isnull()]

    if df.empty:
        return []

    df["TimeStamp"] = pd.to_datetime(df["Время"], format="%d.%m.%Y %H:%M:%S")
    df = df.sort_values(by="TimeStamp", ascending=False).head(count)

    ops = []
    for _, row in df.iterrows():
        ops.append(
            [
                {
                    "chat_id": row["Сотрудник ID"],
                    "model": row["Модель"],
                    "operation": row["Операция"],
                    "count": row["Количество"],
                    "date": row["Дата"],
                    "add_date": row["Время"],
                }
            ]
        )
    return ops


def get_results(message, date: str = "") -> str:
    """Результат по пользователю на указанную дату."""
    today = datetime.now(pytz.utc).strftime("%d.%m.%Y")
    if not date:
        date = today

    reload_data(scope="r")
    if results and len(results[0]) > 1:
        for r in results:
            if r[1] == str(message.chat.id) and r[2] == str(date) and r[2]:
                return r[3]
    return "0"


# ==============================
#  Сохранение операции
# ==============================

def save_operation(message):
    """Сохранение введенного количества и выбранной операции в Google Sheets."""
    chat_id = message.chat.id
    user_state = bot.user_data.get(chat_id, {})

    if user_state.get("state") != "WAIT_QUANTITY":
        bot.send_message(chat_id, "Не вижу активной операции для сохранения. Начни отчет заново командой /report.")
        return

    try:
        quantity = int(message.text)
    except ValueError:
        bot.send_message(chat_id, "Количество должно быть числом. Попробуй еще раз.")
        return

    save_info = bot.send_message(chat_id, "Минуту, сохраняю данные...")
    model = user_state.get("model", "")
    operation = user_state.get("operation", "")

    row = [
        message.from_user.first_name,
        str(message.from_user.id),
        model,
        operation,
        quantity,
        datetime.now(pytz.utc).strftime("%d.%m.%Y"),
        datetime.now(pytz.utc).strftime("%d.%m.%Y %H:%M:%S"),
    ]
    answers_sheet.insert_row(row, index=2)

    # Формулы, как в исходном коде
    formula1 = "=ВПР(C2&D2;'{}'!E:F;2;ЛОЖЬ)*E2".format(questions_sheet_name)
    formula2 = "=ЕСЛИ(ПСТР(G2; 4; 7) = ПСТР(ТДАТА(); 4; 7);1;0)"
    formula3 = "=ЕСЛИОШИБКА(ВПР(B2;'{}'!A:C;3;ЛОЖЬ);A2)".format(goals_sheet_name)

    cells_to_update = [
        gspread.Cell(2, 9, formula1),
        gspread.Cell(2, 10, formula2),
        gspread.Cell(2, 11, formula3),
    ]
    answers_sheet.update_cells(cells_to_update, value_input_option="USER_ENTERED")

    # Сбрасываем состояние и предлагаем продолжить
    bot.user_data[chat_id] = {}
    user_id = message.from_user.id    
    keyboard = build_main_reply_keyboard(user_id)
    bot.delete_message(save_info.chat.id, save_info.message_id)    
    bot.send_message(chat_id, "Отлично, операция сохранена.", reply_markup=keyboard)
    report_command(message, "Продолжим заполнение отчета?")


# ==============================
#  Уведомления
# ==============================

def notify(exc: bool = False):
    """Фоновая отправка уведомлений пользователям по расписанию."""
    while True:
        try:
            reload_data(scope="s+g")
            filtered_settings = [x for x in settings if x and x[0] == "Интервал уведомлений"]
            if not filtered_settings:
                # если нет настроек — спим и повторяем
                time.sleep(600)
                if exc:
                    return
                continue

            time_notification = filtered_settings[0][1].split("_")
            if len(time_notification) < 3:
                time.sleep(600)
                if exc:
                    return
                continue

            now_time = datetime.now(pytz.utc).time()
            now_date = datetime.now(pytz.utc).strftime("%d.%m.%Y")

            week_days = time_notification[0].split(";")
            current_day_of_week = datetime.now().weekday() + 1

            if not any(day.startswith(str(current_day_of_week)) for day in week_days):
                time.sleep(600)
                if exc:
                    return
                continue

            t_start = datetime.strptime(time_notification[1], "%H:%M").time()
            t_end = datetime.strptime(time_notification[2], "%H:%M").time()
            if not (t_start <= now_time <= t_end):
                time.sleep(600)
                if exc:
                    return
                continue

            filtered_chat_ids = [x for x in goals if x and x[0] != "*"]

            global logs_sheet
            logs_sheet = gc.worksheet(logs_sheet_name)
            if logs_sheet.row_count > 100:
                last_logs = logs_sheet.get(f"A{logs_sheet.row_count - 100}:H")
            else:
                last_logs = logs_sheet.get("A1:H")

            for chat_id in filtered_chat_ids:
                last_logs_by_chat = [
                    row
                    for row in last_logs
                    if len(row) > 3
                    and row[1] != "datetime"
                    and datetime.strptime(row[1], "%d.%m.%Y %H:%M:%S").strftime("%d.%m.%Y")
                    == now_date
                    and t_start
                    <= datetime.strptime(row[1], "%d.%m.%Y %H:%M:%S").time()
                    <= t_end
                    and row[2] == chat_id[0]
                ]
                if not last_logs_by_chat:
                    msg = bot.send_message(chat_id[0], "😊")
                    msg = bot.send_message(
                        chat_id[0], filtered_settings[0][2], parse_mode="HTML"
                    )
                    logs_sheet.append_row(
                        [
                            "notify.daily",
                            datetime.now(pytz.utc).strftime("%d.%m.%Y %H:%M:%S"),
                            chat_id[0],
                            msg.text,
                        ]
                    )
        except Exception:
            time.sleep(60)
            notify(True)
        if exc:
            return
        time.sleep(600)


# ==============================
#  Команды пользователя
# ==============================

@bot.message_handler(commands=["start"])
def start_command(message):
    reload_data(scope="s", force=True, silent=True)
    user_id = message.chat.id
    main_kb = build_main_reply_keyboard(user_id)
    bot.send_message(
        message.chat.id,
        f"Привет, {message.from_user.first_name}!\n\n{welcome_message}",
        reply_markup=main_kb,
    )
    #def_command(message, "Ну что, начнем с заполнения первого отчета?")


def def_command(message, text="Вот мои команды:"):
    kb = get_def_buttons()
    last_question = bot.send_message(message.chat.id, text, reply_markup=kb)
    message_user_set.add(
        {"message_id": last_question.message_id, "chat_id": last_question.chat.id}
    )


@bot.message_handler(commands=["report"])
def report_command(message, adt: str = ""):
    reload_data(message, "m+q+qd+s")
    kb = get_buttons_with_models(message)
    prefix = (adt + "\n") if adt else ""
    last_question = bot.send_message(
        message.chat.id, prefix + "Выберите модель", reply_markup=kb
    )
    message_user_set.add(
        {"message_id": last_question.message_id, "chat_id": last_question.chat.id}
    )


@bot.message_handler(commands=["calculate"])
def calculate_command(message):
    finish(message)


@bot.message_handler(commands=["calculate_month"])
def calculate_month_command(message):
    users_report_month(message)


@bot.message_handler(commands=["last_3_rows"])
def last_rows_command(message):
    info = bot.send_message(message.chat.id, "Загружаю данные...")
    operations = get_last_rows(message.chat.id, count=3)
    bot.delete_message(message.chat.id, info.message_id)

    if not operations:
        bot.send_message(message.chat.id, "Ничего нет.")
        return

    for op in operations:
        data = op[0]
        buttons = [
            [
                InlineKeyboardButton(
                    "↑ Удалить ↑",
                    callback_data=f"delete_{data['chat_id']}_{data['add_date']}",
                )
            ]
        ]
        kb = InlineKeyboardMarkup(buttons)
        bot.send_message(
            message.chat.id,
            f"*Модель*: {data['model']}\n"
            f"*Операция*: {data['operation']}\n"
            f"*Количество*: {data['count']}\n"
            f"*Дата*: {data['date']}",
            reply_markup=kb,
            parse_mode="Markdown",
        )


# ==============================
#  Reply‑меню: обработка текстовых кнопок
# ==============================

@bot.message_handler(
    func=lambda m: m.text in [
        "📝 Заполнить отчет",
        "📊 Результат за день",
        "📅 Результат за месяц",
        "🕒 Последние 3 операции",
        "🎭 Анекдот",
        "🔧 Админ"  # Добавляем админскую кнопку
    ]
)
def main_menu_handler(message):
    user_id = message.from_user.id
    keyboard = build_main_reply_keyboard(user_id)
    
    if message.text == "📝 Заполнить отчет":
        report_command(message)
    elif message.text == "📊 Результат за день":
        finish(message)
    elif message.text == "📅 Результат за месяц":
        users_report_month(message)
    elif message.text == "🕒 Последние 3 операции":
        last_rows_command(message)
    elif message.text == "🎭 Анекдот":
        bot.send_message(message.chat.id, get_anekdot(), reply_markup=keyboard)
    elif message.text == "🔧 Админ":
        if is_admin(message):
            admin_command(message)
        else:
            bot.send_message(
                message.chat.id, 
                "❌ Доступ запрещен!", 
                reply_markup=keyboard
            )


# ==============================
#  Админ‑команды
# ==============================

@bot.message_handler(commands=["admin"])
def admin_command(message):
    if not is_admin(message):
        bot.send_message(message.chat.id, "Ты не админ!")
        return

    buttons = [
        [InlineKeyboardButton("Отчет по всем сотрудникам", callback_data="admin_UserReport")]
    ]
    kb = InlineKeyboardMarkup(buttons)
    last_question = bot.send_message(message.chat.id, "Меню администратора", reply_markup=kb)
    message_user_set.add(
        {"message_id": last_question.message_id, "chat_id": last_question.chat.id}
    )

@bot.message_handler(commands=["admin"])
def admin_command_handler(message):
    """Обработчик команды /admin."""
    if is_admin(message):
        admin_command(message)
    else:
        keyboard = build_main_reply_keyboard(message.from_user.id)
        bot.send_message(
            message.chat.id, 
            "❌ Доступ запрещен!", 
            reply_markup=keyboard
        )


# ==============================
#  Callback‑хендлеры
# ==============================

@bot.callback_query_handler(lambda q: q.data.startswith("delete"))
def delete_callback(callback_query):
    data = callback_query.data[len("delete_") :].split("_")
    chat_id, add_date = data[0], data[1]

    buttons = [
        [
            InlineKeyboardButton(
                "↑ Подтверждаю удаление ↑",
                callback_data=f"confdelete_{chat_id}_{add_date}",
            )
        ],
        [InlineKeyboardButton("↑ Отмена ↑", callback_data="canceldelete")],
    ]
    kb = InlineKeyboardMarkup(buttons)
    bot.edit_message_reply_markup(
        callback_query.message.chat.id, callback_query.message.message_id, reply_markup=kb
    )


@bot.callback_query_handler(lambda q: q.data.startswith("canceldelete"))
def cancel_delete_callback(callback_query):
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)


@bot.callback_query_handler(lambda q: q.data.startswith("confdelete_"))
def confirm_delete_callback(callback_query):
    data = callback_query.data[len("confdelete_") :].split("_")
    chat_id, find_time = data[0], data[1]

    find_date = answers_sheet.findall(find_time, in_column=7)
    for cell in find_date:
        row = answers_sheet.get(f"A{cell.row}:H{cell.row}")
        if row and row[0][1] == chat_id:
            answers_sheet.update_cell(cell.row, 8, "удалено")

    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)


@bot.callback_query_handler(lambda q: q.data.startswith("DEFAULT"))
def default_callback(callback_query):
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    data = callback_query.data[len("DEFAULT") :].split("_")
    if len(data) < 2:
        return
    if data[1] == "DO":
        report_command(callback_query.message)
    elif data[1] == "CALCULATE":
        reload_data(callback_query.message, "g")
        finish(callback_query.message, done=0)


@bot.callback_query_handler(lambda q: q.data.startswith("admin_UserReport"))
def admin_user_report_callback(callback_query):
    reload_data(scope="s", force=True)
    if not is_admin(user_id=callback_query.message.chat.id):
        bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
        bot.send_message(callback_query.message.chat.id, "Ты не админ!")
        return

    info = bot.send_message(callback_query.message.chat.id, "Загружаю данные...")
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)

    reload_data(scope="r", force=True)
    results_detail = results

    lines: list[str] = []
    current_user_id = None
    current_user_name = None
    total_line = None  # сохраняем итоговую строку заранее

    header = "Дата         Сумма       Часы"

    for row in results_detail:
        fio, user_id, date_raw, sum_raw, mot_raw,time_raw = row

        # финальное Итого по предприятию - сохраняем строку
        if fio == "Итого":             
            total_line = f"{'':<12} {sum_raw:<11} {time_raw}"
            continue

        # ------- ИТОГ ПО ПОЛЬЗОВАТЕЛЮ -------
        if isinstance(fio, str) and user_id.startswith("Всего ("):
            lines.append("—" * len(header))
            lines.append(f"{fio:<12} {sum_raw:<11} {time_raw}")
            lines.append("")  # пустая строка-разделитель
            current_user_id = None
            current_user_name = None
            continue
        # ------- КОНЕЦ БЛОКА ИТОГА -------

        # обычные строки с операциями
        if user_id != current_user_id:
            current_user_id = user_id
            current_user_name = fio
            lines.append(f"{current_user_name}")
            lines.append(header)
            lines.append("—" * len(header))

        date_str = str(date_raw or "")
        sum_str = str(sum_raw or "")
        time_str = str(time_raw or "")

        lines.append(f"{date_str:<12} {sum_str:<11} {time_str}")

    # добавляем общий итог по предприятию
    if total_line:
        lines.append("")
        lines.append("Итого по предприятию")
        lines.append("—" * len(header))
        lines.append(total_line)

    now = datetime.now()
    month_name = MONTHS_RU[now.month]
    
    bot.delete_message(callback_query.message.chat.id, info.message_id)
    
    # Разбиваем на блоки
    def send_in_chunks(lines_list, chat_id):
        chunk_size = 3800  # запас от 4096
        current_chunk = []
        current_length = 0
        
        for line in lines_list:
            test_chunk = current_chunk + [line]
            test_length = sum(len(l) for l in test_chunk) + len(test_chunk) - 1  # + \n между строками
            
            if current_length + len(line) + 1 > chunk_size or test_length > chunk_size:
                if current_chunk:
                    text = f"Текущая результативность за {month_name} {now.year}:\n<pre><code>\n" + "\n".join(current_chunk) + "\n</code></pre>"
                    bot.send_message(chat_id, text, parse_mode="HTML")
                current_chunk = [line]
                current_length = len(line)
            else:
                current_chunk.append(line)
                current_length = test_length
        
        # отправляем последний чанк
        if current_chunk:
            text = f"Текущая результативность за {month_name} {now.year}:\n<pre><code>\n" + "\n".join(current_chunk) + "\n</code></pre>"
            bot.send_message(chat_id, text, parse_mode="HTML")
    
    send_in_chunks(lines, callback_query.message.chat.id)


@bot.callback_query_handler(lambda q: q.data.startswith("admin_Hide"))
def admin_hide_callback(callback_query):
    info = bot.send_message(callback_query.message.chat.id, "Работаю...")
    obj = callback_query.data[len("admin_Hide") :].split("_")

    oper = "m"
    if len(obj) > 1 and obj[1].startswith("g"):
        oper = "g"
        obj[1] = obj[1][1:]
    if len(obj) > 1 and obj[1].startswith("o"):
        oper = "o"
        obj[1] = obj[1][1:]

    reload_data(scope="qd", force=True)
    data = questions_detail_sheet.get_all_values()

    done = ["", ""]
    for row in data:
        if not row:
            continue
        if checksum(row[0]) == obj[0]:
            idx = 1 if oper == "g" else 2
            if len(obj) > 1 and idx < len(row) and checksum(row[idx]) == obj[1]:
                done = [row[0], row[idx]]

    reload_data(force=True)

    kb = callback_query.message.reply_markup
    if kb and kb.keyboard:
        new_keyboard = []
        for key_row in kb.keyboard:
            btn = key_row[0]
            if not btn.callback_data.startswith(
                (ACTION_PREFIX, MODEL_PREFIX, QUANTITY_PREFIX)
            ):
                new_keyboard.append(key_row)
                continue

            prefix = btn.callback_data.split("_")[0]
            ids = btn.callback_data[len(prefix) + 1 :].split("_")
            cond0 = ids[0] == obj[0]
            cond1 = len(obj) > 1 and len(ids) > 1 and ids[1] == obj[1]
            cond2 = len(obj) > 2 and len(ids) > 2 and ids[2] == obj[2]
            if not (cond0 and (cond1 or len(obj) == 1) and (cond2 or len(obj) <= 2)):
                new_keyboard.append(key_row)

        kb.keyboard = new_keyboard
        bot.edit_message_reply_markup(
            callback_query.message.chat.id,
            callback_query.message.message_id,
            reply_markup=kb,
        )

    bot.delete_message(callback_query.message.chat.id, info.message_id)
    bot.send_message(
        callback_query.message.chat.id,
        f"Скрыто: {done[0]}; {done[1]}",
    )


@bot.callback_query_handler(lambda q: q.data.startswith(MODEL_PREFIX))
def model_callback(callback_query):
    reload_data(scope="q+qd")
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)

    key = frozenset(
        {"message_id": callback_query.message.message_id, "chat_id": callback_query.message.chat.id}.items()
    )
    if key not in message_user_set:
        return

    model_id = callback_query.data[len(MODEL_PREFIX) :]
    if model_id == "@QuitAndSave":
        finish(callback_query.message)
        return

    model_name = model_by_id(model_id)
    if model_name:
        bot.send_message(callback_query.message.chat.id, model_name)

    kb = get_buttons_with_questions(model_id, callback_query.message if is_admin(callback_query.message) else None)
    if not kb:
        kb = get_buttons_with_questions_detail(model_id, callback_query.message if is_admin(callback_query.message) else None)

    last_question = bot.send_message(
        callback_query.message.chat.id, "Выберите группу", reply_markup=kb
    )
    message_user_set.add(
        {"message_id": last_question.message_id, "chat_id": last_question.chat.id}
    )


@bot.callback_query_handler(lambda q: q.data.startswith(ACTION_PREFIX))
def group_callback(callback_query):
    reload_data(scope="q+qd")
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)

    key = frozenset(
        {"message_id": callback_query.message.message_id, "chat_id": callback_query.message.chat.id}.items()
    )
    if key not in message_user_set:
        return

    data = callback_query.data[len(ACTION_PREFIX) :].split(";")[0]
    if data == "@QuitAndSave":
        finish(callback_query.message)
        return

    kb = get_buttons_with_questions(
        data, callback_query.message if is_admin(callback_query.message) else None
    )
    if not kb:
        kb = get_buttons_with_questions_detail(
            data, callback_query.message if is_admin(callback_query.message) else None
        )

    last_question = bot.send_message(
        callback_query.message.chat.id, "Выберите операцию", reply_markup=kb
    )
    message_user_set.add(
        {"message_id": last_question.message_id, "chat_id": last_question.chat.id}
    )


@bot.callback_query_handler(lambda q: q.data.startswith(QUANTITY_PREFIX))
def quantity_callback(callback_query):
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)

    key = frozenset(
        {"message_id": callback_query.message.message_id, "chat_id": callback_query.message.chat.id}.items()
    )
    if key not in message_user_set:
        return

    data = callback_query.data[len(QUANTITY_PREFIX) :].split("_")
    if data[0] == "@QuitAndSave":
        finish(callback_query.message)
        return

    model_id, question_id = data[0], data[1]
    model_name = model_by_id(model_id)
    question_text = question_detail_by_id(question_id)

    if question_text:
        bot.send_message(callback_query.message.chat.id, question_text)

    last_question = bot.send_message(callback_query.message.chat.id, "Укажите количество:")
    message_user_set.add(
        {"message_id": "Укажите количество:", "chat_id": last_question.chat.id}
    )

    # сохраняем состояние
    bot.user_data[callback_query.message.chat.id] = {
        "state": "WAIT_QUANTITY",
        "model": model_name or "",
        "operation": question_text or "",
    }


# ==============================
#  Итоговые отчеты
# ==============================

def finish(message, done: int = 1):
    """Подсчет результата и вывод."""
    info = bot.send_message(message.chat.id, "Считаю результат...")
    reload_data(scope="r", force=True)
    res = get_results(message)
    bot.delete_message(info.chat.id, info.message_id)
    if done:
        bot.send_message(
            message.chat.id,
            f"На сегодня результат: {res}",
        )
    else:
        bot.send_message(
            message.chat.id,
            f"Результат за день: {res}",
        )


def users_report_month(message):
    """Отчет по дням за месяц по пользователю."""
    info = bot.send_message(message.chat.id, "Загружаю данные...")
    reload_data(scope="r")
    results_detail = results

    table = ""
    messages = []
    if len(results_detail) > 1:
        table += "Вот ваши результаты по дням:\n"
        for row in results_detail:
            if len(row) < 6:
                continue
            if row[1] != str(message.chat.id) and row[1] != f"Всего ({message.chat.id})":
                continue

            table += f"<b>{row[2].ljust(25 - len(row[2]))}</b>"
            table += f"<b>{row[3].rjust(15 - len(row[3]))}</b>"
            table += f"<b>{row[5].rjust(15 - len(row[5]))}</b> ч."
            if len(table) > 3000:
                messages.append(table)
                table = ""
            table += "\n"
        if table:
            messages.append(table)
    else:
        messages.append("Пока пусто")

    bot.delete_message(message.chat.id, info.message_id)
    for i, msg in enumerate(messages):
        bot.send_message(
            message.chat.id,
            ("\n ->" if i > 0 else "") + msg,
            parse_mode="HTML",
        )


# ==============================
#  Обработка текстов (количество)
# ==============================

@bot.message_handler(func=lambda m: True, content_types=["text"])
def text_handler(message):
    """Обработка текста, когда ждем количество."""
    user_state = bot.user_data.get(message.chat.id, {})
    if user_state.get("state") == "WAIT_QUANTITY":
        save_operation(message)
        return
    else:
        user_id = message.chat.id
        main_kb = build_main_reply_keyboard(user_id)
        bot.send_message(
            message.chat.id,
            f"Привет, {message.from_user.first_name}!",
            reply_markup=main_kb,
        )


# ==============================
#  Точка входа
# ==============================


def main():
    # Запуск фонового уведомителя
    notify_thread = threading.Thread(target=notify, daemon=True)
    notify_thread.start()

    # Запуск бота
    if is_running_in_docker():
        bot.infinity_polling(timeout=30, long_polling_timeout=30)
    else:
        bot.infinity_polling()


if __name__ == "__main__":
    # локальный запуск: и Flask, и бот
    threading.Thread(target=main, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))