import docker
import gspread
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import pytz
import time
import crcmod.predefined
import threading
import asyncio
import pandas as pd
from datetime import timedelta
import numpy as np
import json
import os

def is_running_in_docker():
    """
    Проверяет, запущен ли скрипт внутри Docker-контейнера.
    """
    # Проверяем наличие файла /.dockerenv (Docker создает его внутри контейнера)
    if os.path.exists('/.dockerenv'):
        return True

    # Проверяем cgroup (внутри контейнера обычно есть упоминание о Docker)
    try:
        with open('/proc/self/cgroup', 'r') as f:
            if 'docker' in f.read():
                return True
    except FileNotFoundError:
        pass

    return False

'''if is_running_in_docker():
    client = docker.from_env()
    # Получаем ID текущего контейнера
    container_id = open('/proc/self/cgroup').read().split('/docker/')[-1].strip()
    # Получаем объект контейнера
    container = client.containers.get(container_id)
    # Получаем параметры контейнера
    print(container.attrs)
    key_file = 'KEY/tb-fabric.json'
else:'''

key_path = os.getenv("KEY_PATH")
if key_path:
    key_file = key_path
    print(f"Используем ключ: {key_path}")
else:
    key_file = 'KEY/tb-fabric.json'

# Параметры авторизации в Google API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(key_file, scope)
with open(key_file, 'r', encoding='utf-8') as file:
    data = json.load(file)

# ID Google таблицы
spreadsheet_id =  data['spreadsheet_id']
# Настройки Telegram Bot API
TOKEN = data['botTOKEN']

welcome_message = 'Я твой личный помощник для ведения учета рабочего времени и отчетности. \n\nПосле каждого рабочего дня, я буду ждать тебя с отчетом о проделанной работе. Это поможет тебе не только точно отслеживать свой рабочий график, но и получать заработанные деньги вовремя. \n\nНо самое главное, не забывай отдыхать и заботиться о своем физическом и эмоциональном здоровье - это тоже очень важно!'

def get_anokdot():
    try:
        import requests
        from bs4 import BeautifulSoup # type: ignore
        # Загрузить страницу
        url = 'https://www.anekdot.ru/random/anekdot/'
        response = requests.get(url)

        soup = BeautifulSoup(response.text, 'html.parser')
        topicboxes = soup.find_all('div', {'class': ['content content-min', 'topicbox']})

        # Извлечь анекдоты, содержащие тег <div> с классом "votingbox"
        anecdotes = []
        for topicbox in topicboxes:
            votingbox = topicbox.find('div', {'class': 'votingbox'})
            if votingbox:
                 btn2 = votingbox.find('div', {'class': 'btn2'})
                 if btn2:
                    user_star = btn2.find('a', {'class': 'user-star'})
                    if user_star:
                        anecdotes.append({"rate":user_star.text.count('★'),"text":(''.join(str(item) for item in topicbox.find('div', {'class': 'text'}).contents))})
                        continue
        best_one = sorted(anecdotes, key=lambda x: x["rate"], reverse=True)[0]["text"]
        return 'Держи шутку:\n'+best_one.replace('<br/>','\n')
    except:
        return 'Шутки нет 🤷‍♂️'

# Настройки
settings_sheet_name = 'Настройки'
settings_range = 'A2:H'
# Результативность
results_sheet_name = 'Результативность'
results_range = 'J2:O'
# Целевые показатели
goals_sheet_name = 'Целевые показатели'
goals_range = 'A2:B'
# Область с вопросами
questions_sheet_name = 'Операции'
operations_detail_range = 'A2:H'
models_range = 'J2:J'
# Область с вопросами
questions_range = 'J2:K'
# Область с деталями
questions_detail_range = 'U2:AB'
# Область с ответами
answers_sheet_name = 'Отчет'
answer_range = 'A2:H'
# Область с логами
logs_sheet_name = 'LOGS'
logs_range = 'A2:H'

update_period = 300

gc = gspread.authorize(creds).open_by_key(spreadsheet_id)
settings_sheet = gc.worksheet(settings_sheet_name)
models_sheet = gc.worksheet(questions_sheet_name)
questions_sheet = gc.worksheet(questions_sheet_name)
questions_detail_sheet = gc.worksheet(questions_sheet_name)
answers_sheet = gc.worksheet(answers_sheet_name)
goals_sheet = gc.worksheet(goals_sheet_name)
logs_sheet = gc.worksheet(logs_sheet_name)
results_sheet = gc.worksheet(results_sheet_name)

goals_last_update = questions_detail_last_update = questions_last_update = models_last_update = settings_last_update = datetime(2023,1,1,23,59,59)

def reload_data(message = None, scope = 'm+q+qd+g+s', force = False, silent = False):
    if message and not silent:
        m = bot.send_message(message.chat.id, "Загружаю данные...")
    global models_sheet, models, questions_sheet, questions, questions_detail_sheet, answers, results
    global questions_detail, operations_detail, goals_sheet, goals, settings_sheet, settings
    global goals_last_update, questions_detail_last_update, questions_last_update, models_last_update, settings_last_update, answers_last_update, results_last_update, operations_detail_last_update
    current_time = datetime.today()

    for s in scope.split('+'):
        if s == 's':
            if (current_time - settings_last_update).total_seconds() > update_period or force:
                settings = settings_sheet.get(settings_range)
                settings_last_update = current_time
        if s == 'm':
            if (current_time - models_last_update).total_seconds() > update_period or force:
                models = models_sheet.get(models_range)
                models_last_update = current_time
        if s == 'q':
            if (current_time - questions_last_update).total_seconds() > update_period or force:
                questions = questions_sheet.get(questions_range)
                for sublst in questions:
                    groups = sublst[1].split(";")  # Разбиваем строку по разделителю ";"
                    sublst[1:] = groups           # Заменяем вложенный список на список групп
                questions_last_update = current_time
        if s == 'qd':
            if (current_time - questions_detail_last_update).total_seconds() > update_period or force:                    
                questions_detail = questions_detail_sheet.get(questions_detail_range)
                questions_detail_last_update = current_time
        if s == 'od':
            #if (current_time - questions_detail_last_update).total_seconds() > update_period or force:                    
            operations_detail = questions_sheet.get(operations_detail_range)
            operations_detail_last_update = current_time
        if s == 'g':
            if (current_time - goals_last_update).total_seconds() > update_period or force:
                goals = goals_sheet.get(goals_range)
                goals_last_update = current_time
        if s == 'a':
            answers = answers_sheet.get(answer_range)
            answers_last_update = current_time
        if s == 'r':
            results = results_sheet.get(results_range)
            results_last_update = current_time
    if message and not silent:
        bot.delete_message(m.chat.id, m.message_id)

def get_setting(message = None, name = None):
    name = name.split('+')
    if not name:
        return list()
    if message:
        m = bot.send_message(message.chat.id, "Загружаю данные...")

    reload_data(scope = 's')
    
    if message:
        bot.delete_message(m.chat.id, m.message_id)        

    return list(filter(lambda x: x[0] in name, settings))

# Константы для префикса имен callback_data
ACTION_PREFIX = 'action_'
QUANTITY_PREFIX = 'quantity_'
MODEL_PREFIX = 'model_'

def checksum(s):
    crc32_func = crcmod.predefined.mkCrcFun('crc-32')
    checksum = str(crc32_func(s.encode('utf-8')))
    return checksum

def model_by_id(id):
    for model in list(set(map(tuple, models))):
        if model and len(model) >= 0 and checksum(model[0]) == id and model[0] != '@last_update':
            return model[0]

def question_detail_by_id(id):
    for question_detail in list(set(map(tuple, questions_detail))):
        if question_detail and len(question_detail) >= 0 and checksum(question_detail[2]) == id and question_detail[0] != '@last_update':
            return question_detail[2]

class MessageUserSet(set):
    def add(self, value):
        new_message_id = value['message_id']
        new_user_id = value['chat_id']
        for record in self:
            if dict(record)['chat_id'] == new_user_id:
                super().discard(record)
                break
        super().add(frozenset({'message_id': new_message_id, 'chat_id': new_user_id}.items()))

message_user_set = MessageUserSet()

def notify(exc = False):
     while True:        
        try:
            #print('Уведомление!')
            reload_data(scope = 's+g')
            filtered_settings = list(filter(lambda x: x[0] == 'Интервал уведомлений', settings))
            if len(filtered_settings) > 0:
                time_notification = (filtered_settings[0][1]).split('_')
            if len(time_notification)> 0:
                now_time = datetime.now(pytz.utc).time()
                now_date = datetime.now(pytz.utc).strftime('%d.%m.%Y')
                week_days = time_notification[0].split(';')
                current_day_of_week = datetime.now().weekday() + 1
                if len(list(filter(lambda day: day[0] == str(current_day_of_week), week_days))) > 0:
                    t_start = datetime.strptime(time_notification[1], '%H:%M').time()
                    t_end = datetime.strptime(time_notification[2], '%H:%M').time()
                    if t_start <= now_time <= t_end:
                        filtered_chat_ids = list(filter(lambda x: x[0] != '*', goals))
                        global logs_sheet
                        logs_sheet = gc.worksheet(logs_sheet_name)
                        if logs_sheet.row_count > 100:
                            last_logs = logs_sheet.get(f"A{str(logs_sheet.row_count-100)}:H")
                        else:
                            last_logs = logs_sheet.get(f"A1:H")
                        for chat_id in filtered_chat_ids:
                            last_logs_by_chat_id = list(filter(lambda x: x[1] != 'datetime' and datetime.strptime(x[1], '%d.%m.%Y %H:%M:%S').strftime('%d.%m.%Y') == now_date and t_start <= datetime.strptime(x[1], '%d.%m.%Y %H:%M:%S').time() <= t_end and x[2] == chat_id[0], last_logs))
                            if len(last_logs_by_chat_id) == 0:
                                msg = bot.send_message(chat_id[0], '😊')
                                msg = bot.send_message(chat_id[0], filtered_settings[0][2],parse_mode='HTML')
                                logs_sheet.append_row(['notify.daily',datetime.now(pytz.utc).strftime('%d.%m.%Y %H:%M:%S'),chat_id[0], msg.text])
        except:
            time.sleep(60)
            notify(True)
            #return
        if exc:
            return
        time.sleep(600)

def get_buttons_with_models(message):
    # Создаем пустую клавиатуру
    keyboard = InlineKeyboardMarkup()
    # Инициализируем массив с кнопками
    buttons = []
    # Проходим по всем вопросам и создаем для каждого кнопку
    # Преобразуем список списков в список кортежей
    models_tuples = [tuple(model) for model in models]
    # Получаем уникальные записи из списка кортежей
    unique_models_tuples = set(models_tuples)
    # Преобразуем список кортежей обратно в список списков
    unique_models = [list(model) for model in unique_models_tuples]
    # Сортируем уникальные записи по алфавиту и выводим их на экран
    unique_models.sort()
    for model in unique_models:
        if model and len(model) > 0 and model[0] != '@last_update':
            # if is_admin(message):
            #     buttons.append([InlineKeyboardButton(model[0], callback_data=MODEL_PREFIX + checksum(model[0]), wrap_text=True) ,InlineKeyboardButton('⛔', callback_data= f"admin_Hide{checksum(model[0])}")])
            # else:
                buttons.append([InlineKeyboardButton(model[0], callback_data=MODEL_PREFIX + checksum(model[0]), wrap_text=True)])
    buttons.append([InlineKeyboardButton('-Закрыть отчет-', callback_data=MODEL_PREFIX + '@QuitAndSave')])
    # Добавляем кнопки в клавиатуру
    keyboard = InlineKeyboardMarkup(buttons)
    return keyboard

def get_buttons_with_questions(path, message = None):
    path = path.split('_')
    model = path[0]
    group = path[1] if len(path) > 1 else path[0]
    level = int(path[2]) if len(path) > 2 else 1
    # Создаем пустую клавиатуру
    keyboard = InlineKeyboardMarkup()
    # Инициализируем массив с кнопками
    buttons = []
    questions_f = set()
    # Проходим по всем вопросам и создаем для каждого кнопку    
    for question in list(filter(lambda q: checksum(q[0]) == model and checksum(";".join(q[1 if level > 1 else 0:1 + (level - 1 if level > 1 else 0)] if len(q) >= level + 1 else '0')) == group and len(q) >= level + 1, questions)):
        questions_f.add(";".join(question[1:1+level]))
        #if question and len(question) >= 0 and checksum(question[0]) == model and question[0] != '@last_update':
            # if is_admin(message):
            #     buttons.append([InlineKeyboardButton(question[1], callback_data=ACTION_PREFIX + model +'_'+checksum(question[1]), wrap_text=True), InlineKeyboardButton('⛔', callback_data= f"admin_Hide{model}_g{checksum(question[1])}") if is_admin(message) else None])
            # else:
    if len(questions_f) == 0:
        return None
    for question in sorted(questions_f):
        buttons.append([InlineKeyboardButton(question.split(';')[-1], callback_data=ACTION_PREFIX + model +'_'+checksum(question)+'_'+str(level+1), wrap_text=True)])
    # Добавляем кнопки в клавиатуру
    buttons.append([InlineKeyboardButton('-Закрыть отчет-', callback_data=ACTION_PREFIX + '@QuitAndSave')])
    keyboard = InlineKeyboardMarkup(buttons)
    return keyboard

def get_buttons_with_questions_detail(path,message = None):
    path = path.split('_')
    model = path[0]
    group = path[1] if len(path) > 1 else path[0]
    level = int(path[2]) - 1
    # Создаем пустую клавиатуру
    keyboard = InlineKeyboardMarkup()
    buttons = []
    # Проходим по всем ответам и создаем для каждого кнопку, если он соответствует выбранному вопросу
    for question_detail in list(filter(lambda qd: len(qd) > 2 and checksum(qd[0]) == model and checksum(qd[1]) == group, questions_detail)):
    #for question_detail in questions_detail:
        #if question_detail and len(question_detail) >= 3 and (len(question_detail[7]) == 0 if len(question_detail) > 7 else 1 == 1)  and checksum(question_detail[0]) == data[0] and checksum(question_detail[1]) == data[1]  and question_detail[0] != '@last_update':
            # if is_admin(message):
            #     buttons.append([InlineKeyboardButton(question_detail[2], callback_data=QUANTITY_PREFIX + data[0] +'_' +checksum(question_detail[2]), wrap_text=True), InlineKeyboardButton('⛔', callback_data= f"admin_Hide{data[0]}_o{checksum(question_detail[2])}") if is_admin(message) else None])
            # else:
        buttons.append([InlineKeyboardButton(question_detail[2], callback_data=QUANTITY_PREFIX + model +'_' +checksum(question_detail[2]), wrap_text=True)])
    # Добавляем кнопки в клавиатуру
    buttons.append([InlineKeyboardButton('-Закрыть отчет-', callback_data=QUANTITY_PREFIX + '@QuitAndSave')])
    keyboard = InlineKeyboardMarkup(buttons)
    return keyboard

def get_def_buttons(all = 1):
    # Создаем пустую клавиатуру
    #keyboard = InlineKeyboardMarkup()
    # Инициализируем массив с кнопками
    #buttons = []
    #buttons.append(InlineKeyboardButton('Заполнить отчет', callback_data='DEFAULT_DO'))
    #if all != 0:
     #   buttons.append(InlineKeyboardButton('Посмотреть результат за день', callback_data='DEFAULT_CALCULATE'))
    #keyboard.add(*buttons)

    buttons_list = [[InlineKeyboardButton('Заполнить отчет', callback_data='DEFAULT_DO')]]
    if all != 0:
        buttons_list.append([InlineKeyboardButton('Посмотреть результат за день', callback_data='DEFAULT_CALCULATE')])
    # Создание разметки для кнопок
    keyboard = InlineKeyboardMarkup(buttons_list)
    return keyboard

def get_last_rows(id = 0,count = 5):
    operations_set = []
    if id == 0: return
    reload_data(scope='a')
    answer = pd.DataFrame(answers, columns=['Сотрудник имя',	'Сотрудник ID'	,'Модель',	'Операция',	'Количество',	'Дата',	'Время',	'Признак удаления']) \
    .loc[lambda x: x['Сотрудник ID'] == str(id)] \
    .loc[lambda x: x['Признак удаления'].isnull()] 
    answer['TimeStamp'] = pd.to_datetime(answer['Время'],format='%d.%m.%Y %H:%M:%S')    
    answer = answer.sort_values(by='TimeStamp',ascending=False).head(count)
    for index,row in answer.iterrows():
        operations_set.append([{'chat_id':row['Сотрудник ID'],'model':row['Модель'],'operation':row['Операция'],'count':row['Количество'],'date':row['Дата'],'add_date':row['Время']}])
    return operations_set

# Создаем бота
from telebot import TeleBot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton

class MyBot(TeleBot):
    def __init__(self, token):
        super().__init__(token)
        self.user_data = {}

bot = MyBot(TOKEN)

# Функция для загрузки списка кнопок
def load_buttons():
    # Формируем список кнопок
    button_list = []
    button_list.append(telebot.types.InlineKeyboardButton('Начать заполнять', callback_data='/start'))
    return button_list

# Загружаем список кнопок
button_list = load_buttons()

# Формируем меню с кнопками
keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
keyboard.add(*button_list)

# Обработчик команды /start - выводим первый вопрос
@bot.message_handler(commands=['start'])
def start_command(message):
    global message_user_set
    # Отправляем сообщение с вопросом и кнопками
    keyboard = get_def_buttons(0)
    bot.send_message(message.chat.id, 'Привет ' + message.from_user.first_name + '! \n'+welcome_message)
    last_question = bot.send_message(message.chat.id, 'Ну, что, предлагаю начать с заполнения первого твоего отчета', reply_markup=keyboard)
    #message_user_set.add({'message_id': last_question.message_id, 'chat_id': last_question.chat.id})
    #bot.send_message(message.chat.id, "Выберите операцию", reply_markup=keyboard)

def is_admin(message):
    #Получаем админов
    if message == None:
        return
    for setting in settings:
        if setting[0] == 'Администратор' and setting[1] == str(message.chat.id):
            return True
    return False

@bot.message_handler(commands=['admin'])
def admin_command(message):
    if not is_admin(message):
        bot.send_message(message.chat.id, "Ты не админ!")
        return
    keyboard = InlineKeyboardMarkup()
    # Инициализируем массив с кнопками
    buttons = []
    # Добавляем кнопки в клавиатуру
    buttons.append([InlineKeyboardButton('Отчет по всем сотрудникам', callback_data='admin_UserReport')])
    keyboard = InlineKeyboardMarkup(buttons)
    last_question = bot.send_message(message.chat.id, 'Меню администратора', reply_markup=keyboard)
    message_user_set.add({'message_id': last_question.message_id, 'chat_id': last_question.chat.id})


@bot.message_handler(commands=['report'])
def report_command(message,adt=''):
    global message_user_set
    reload_data(message,'m+q+qd+s')
    #reload_data(message,'s',silent=True,force=True)
    # Загружаем кнопки с первым вопросом
    keyboard = get_buttons_with_models(message)
    # Отправляем сообщение с вопросом и кнопками
    last_question = bot.send_message(message.chat.id, (adt + '\n' ) if len(adt) > 0 else '' + ' Выберите модель', reply_markup=keyboard)
    message_user_set.add({'message_id': last_question.message_id, 'chat_id': last_question.chat.id})

@bot.message_handler(commands=['calculate'])
def calculate_command(message):
    finish(message)

@bot.message_handler(commands=['calculate_month'])
def calculate_command(message):
    users_report_mounth(message)

@bot.message_handler(commands=['last_3_rows'])
def calculate_command(message):
    info = bot.send_message(message.chat.id,"Загружаю данные...")
    operations = get_last_rows(message.chat.id,count = 3)
    bot.delete_message(message.chat.id, info.message_id)
    if len(operations) == 0:
        bot.send_message(message.chat.id, f"Ничего нет.")
        return
    for index, operation in enumerate(operations):
        buttons = []
        buttons.append([InlineKeyboardButton('↑ Удалить ↑', callback_data='delete_'+operation[0]['chat_id']+'_'+operation[0]['add_date'])])
        keyboard = InlineKeyboardMarkup(buttons)
        # if (index == 0):
        #     bot.edit_message_text(f"*Модель*: {operation[0]['model']}\n*Операция*: {operation[0]['operation']}\n*Количество*: {operation[0]['count']}\n*Дата*: {operation[0]['date']}",message.chat.id,info.message_id,reply_markup=keyboard, parse_mode= 'Markdown')
        # else:
        bot.send_message(message.chat.id, f"*Модель*: {operation[0]['model']}\n*Операция*: {operation[0]['operation']}\n*Количество*: {operation[0]['count']}\n*Дата*: {operation[0]['date']}", reply_markup=keyboard, parse_mode= 'Markdown')


def def_command(message, def_message_text = 'Вот мои команды:'):
    global message_user_set
    # Отправляем сообщение с вопросом и кнопками
    keyboard = get_def_buttons()
    last_question = bot.send_message(message.chat.id, def_message_text, reply_markup=keyboard)
    message_user_set.add({'message_id': last_question.message_id, 'chat_id': last_question.chat.id})
    #bot.send_message(message.chat.id, "Выберите операцию", reply_markup=keyboard)

# Обработчик нажатия на кнопку удаления
@bot.callback_query_handler(lambda query: query.data.startswith('delete'))
def process_question_callback(callback_query):
    #bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    data = callback_query.data[len('delete_'):].split('_')
    buttons = []
    buttons.append([InlineKeyboardButton('↑ Подтверждаю удаление ↑', callback_data='confdelete_'+data[0]+'_'+data[1])])
    buttons.append([InlineKeyboardButton('↑ Отмена ↑', callback_data='canceldelete')])
    keyboard = InlineKeyboardMarkup(buttons)
    bot.edit_message_reply_markup(callback_query.message.chat.id, callback_query.message.message_id, reply_markup=keyboard)
    return

@bot.callback_query_handler(lambda query: query.data.startswith('canceldelete'))
def process_question_callback(callback_query):
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    return

@bot.callback_query_handler(lambda query: query.data.startswith('confdelete_'))
def process_question_callback(callback_query):
    data = callback_query.data[len('confdelete_'):].split('_')
    find_date = answers_sheet.findall(data[1],in_column=7)
    for date in find_date:
        row = answers_sheet.get('A'+str(date.row)+':H'+str(date.row))
        if row[0][1] == data[0]:
            answers_sheet.update_cell(date.row,8,'удалено')
            continue
    # for row in answers:
    #     if row[1] == data[0] and row[6] == data[1]:
    #         answers.remove(row)
    #         continue
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    return

# Обработчик нажатия на кнопку основного меню
@bot.callback_query_handler(lambda query: query.data.startswith('DEFAULT'))
def process_question_callback(callback_query):
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    data = callback_query.data[len('DEFAULT'):].split('_')
    if data[1] == 'DO':
        report_command(callback_query.message)
        return
    if data[1] == 'CALCULATE':
        reload_data(callback_query.message,'g')
        finish(callback_query.message,0)
        return

# Обработчик нажатия на кнопку с отчетом по всем пользователям
@bot.callback_query_handler(lambda query: query.data.startswith('admin_UserReport'))
def all_users_report_callback(callback_query):
    info = bot.send_message(callback_query.message.chat.id,"Загружаю данные...")
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    reload_data(scope='r')
    results_detail = results    
    table = ""
    prev = 0
    messages = []
    if len(results_detail) > 1:
        for data in results_detail:
            if len(data[1]) > 0:
                if (prev != data[1] and data[0] != ''):
                    table += "\n"
                table += f"{('<b><u>' + data[0] + '</u></b>') if prev != data[1] else ''} \n"
                #table += f"{data[1] if prev != data[0] else ''} \t"
                table += f"<b>{data[2].ljust(25-len(data[2]))}</b>"  # Поле шириной 20 символов
                table += f"<b>{data[3].rjust(20-len(data[3]))}</b>"  # Центрированное поле шириной 15 символов
                table += f"<b>{data[5].rjust(15-len(data[5]))}</b>"  # Правостороннее поле шириной 25 символов                
                if len(table) > 3000:
                    messages.append(table)
                    table = ''
                #table += "\n"
            prev = data[1]
        messages.append(table)
    else:
        messages.append('Пока пусто')
    bot.delete_message(callback_query.message.chat.id, info.message_id)

    # отправляем каждое сообщение
    for i, message in enumerate(messages):
        bot.send_message(callback_query.message.chat.id, ('\n ->' if i > 0 else '') + f'{message}', parse_mode="HTML")
    #bot.send_message(callback_query.message.chat.id, f'{table}', parse_mode="HTML")
    return

# Обработчик нажатия на кнопку с отчетом по всем пользователям
@bot.callback_query_handler(lambda query: query.data.startswith('admin_Hide'))
def all_users_report_callback(callback_query):    
    #bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    info = bot.send_message(callback_query.message.chat.id,"Работаю...")
    obj = callback_query.data[len('admin_Hide'):].split('_')

    oper = 'm'
    if len(obj) > 1 and obj[1].startswith('g'):
        oper = 'g'
        obj[1] = obj[1][len('g'):]
    if len(obj) > 1 and obj[1].startswith('o'):
        oper = 'o'
        obj[1] = obj[1][len('o'):]

    reload_data(scope='qd',force=True)
    data = questions_detail_sheet.get_all_values()
    done = set()
    range = "H"
    for row_index, row in enumerate(data):
        if checksum(row[0]) == obj[0] and (checksum(row[1 if oper == 'g' else 2]) == obj[1] if len(obj)>1 else True):
            done = [row[0],row[1 if oper == 'g' else 2] if len(obj)>1 else '']
            #questions_detail_sheet.update_cell(row_index + 1,8,'1')
            #questions_detail_sheet.batch_update
    reload_data(force=True)

    for key in callback_query.message.reply_markup.keyboard:
        if key[0].callback_data.startswith((ACTION_PREFIX,MODEL_PREFIX,QUANTITY_PREFIX)):
            prefix = key[0].callback_data.split("_")[0]
            ids = key[0].callback_data[len(prefix)+1:].split('_')
            if ids[0] == obj[0] and (ids[1] == obj[1]) if len(obj)>1 else True and (ids[1] == obj[2]) if len(obj)>2 else True:
                callback_query.message.reply_markup.keyboard.remove(key)

    bot.delete_message(callback_query.message.chat.id, info.message_id)
    bot.edit_message_reply_markup(callback_query.message.chat.id, callback_query.message.message_id, reply_markup=callback_query.message.reply_markup)
    info = bot.send_message(callback_query.message.chat.id,f"Скрыто: {done[0]}; {done[1]}")
    return

# @bot.callback_query_handler(lambda query: query.data.startswith('UserReport'))
def users_report_mounth(message):
    info = bot.send_message(message.chat.id,"Загружаю данные...")
    bot.delete_message(message.chat.id, message.message_id)
    reload_data(scope='r')
    results_detail = results    
    table = ""
    messages = []
    if len(results_detail) > 1:
        table += "Вот ваши результаты по дням:\n"
        for data in results_detail:
            """if len(data[1]) > 0 and (data[1] == str(message.chat.id) or data[1] == 'Всего (' + str(message.chat.id) + ')'):
                #table += f"{('<b><u>' + data[0] + '</u></b>') if prev != data[1] else ''} \n"
                #table += f"{data[1] if prev != data[0] else ''} \t"
                table += f"\t\t\t\t\t\t {data[2]}"
                table += f"\t\t\t\t\t\t <b> {data[3]} </b>"
                table += f"\t\t\t\t\t\t\t\t\t <b> {data[5]} </b>"""
            if len(data[1]) > 0 and (data[1] == str(message.chat.id) or data[1] == 'Всего (' + str(message.chat.id) + ')'):
            # Форматирование данных с фиксированной шириной полей
                table += f"<b>{data[2].ljust(25-len(data[2]))}</b>"  # Поле шириной 20 символов
                table += f"<b>{data[3].rjust(15-len(data[3]))}</b>"  # Центрированное поле шириной 15 символов
                table += f"<b>{data[5].rjust(15-len(data[5]))}</b> ч."  # Правостороннее поле шириной 25 символов                
                if len(table) > 3000:                
                    messages.append(table)
                    table = ''
                table += "\n"
        messages.append(table)
    else:
        messages.append('Пока пусто')
    bot.delete_message(message.chat.id, info.message_id)

    # отправляем каждое сообщение
    for i, mess in enumerate(messages):
        bot.send_message(message.chat.id, ('\n ->' if i > 0 else '') + f'{mess}', parse_mode="HTML")
    #bot.send_message(callback_query.message.chat.id, f'{table}', parse_mode="HTML")
    return

"""     info = bot.send_message(message.chat.id,"Загружаю данные...")    
    reload_data(scope='g+s+a+r+od',force=True)
    results_detail = results
    answer_detail = answers

    table = "Вот ваши результаты по дням:\n"
    month = datetime.now().month
    year = datetime.now().year    
    detail = pd.DataFrame(operations_detail, columns=['Модель',	'Группа',	'Операция',	'Общее количество'	,'МодельОперация',	'Цена'	,'Секунды'	,'Не отображать'])
    answer = pd.DataFrame(answer_detail, columns=['Сотрудник имя',	'Сотрудник ID'	,'Модель',	'Операция',	'Количество',	'Дата',	'Время', 'Признак удаления']) \
    .loc[lambda x: x['Сотрудник ID'] == str(message.chat.id)] \
    .loc[lambda x: x['Признак удаления'].isnull()] 
    merge = pd.merge(detail, answer, on=['Модель', 'Операция'])
    merge = merge[merge['Секунды'] != '']
    merge[['Количество', 'Секунды']] = merge[['Количество', 'Секунды']].fillna(0).astype(float)
    merge['Дата'] = pd.to_datetime(merge['Дата'],format='%d.%m.%Y')
    merge['Активность'] = merge['Количество']*merge['Секунды']

    group = merge.groupby(['Сотрудник ID', 'Дата'])['Активность'].sum().reset_index()
    group = group[(group['Дата'].dt.month == month) & (group['Дата'].dt.year == year)]    
    group['Активность'] = group['Активность'].apply(lambda x: str(timedelta(seconds=x)))
    summ = 0
    if results_detail and len(results_detail[0]) > 1:
        for data in list(filter(lambda x: x[1] == str(message.chat.id) and (len(x[2]) == 10 and int(x[2].split('.')[1]) == month), results_detail)):
            table += f"\t {data[2]}"
            table += f"\t <b> {data[3]} </b>"
            act = group[group['Дата'] == datetime.strptime(data[2], '%d.%m.%Y')]
            #tabs = '\t'*(20 - len(data[3]))
            table += f"\t <b> {act['Активность'].values[0] if len(act) > 0 else '-'} </b>\n"
            summ += float(data[3].replace(',','.').replace(u'\xa0', u''))    
        table += f"<b>Итого за месяц:\t\t\t{str(round(summ, 2))} </b>\n"
    else:
        table = f"<b> Пусто </b>\n"
    bot.delete_message(message.chat.id, info.message_id)
    bot.send_message(message.chat.id, f'{table}', parse_mode="HTML")
    return """

# Обработчик нажатия на кнопку с моделью
@bot.callback_query_handler(lambda query: query.data.startswith(MODEL_PREFIX))
def process_model_callback(callback_query):
    global message_user_set
    reload_data(scope="q+qd")
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    if not frozenset({'message_id': callback_query.message.message_id, 'chat_id': callback_query.message.chat.id}.items()) in message_user_set:
        return
    # if not (value['message_id'], callback_query.message.message_id) and (value['user_id'], callback_query.user_id) in message_user_set:
    #     return
    # Извлекаем выбранный вопрос из callback_data
    question = callback_query.data[len(MODEL_PREFIX):]
    if question == '@QuitAndSave':
        finish(callback_query.message)
        return
    bot.send_message(callback_query.message.chat.id, model_by_id(question))
    # Загружаем кнопки с ответами на данный вопрос
    keyboard = get_buttons_with_questions(question, callback_query.message if is_admin(callback_query.message) else None)
    # Отправляем сообщение с вопросом и кнопками
    last_question = bot.send_message(callback_query.message.chat.id, "Выберите группу", reply_markup=keyboard)
    message_user_set.add({'message_id': last_question.message_id, 'chat_id': last_question.chat.id})

# Обработчик нажатия на кнопку с операцией
@bot.callback_query_handler(lambda query: query.data.startswith(ACTION_PREFIX))
def process_group_callback(callback_query):
    global message_user_set
    reload_data(scope="q+qd")
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    if not frozenset({'message_id': callback_query.message.message_id, 'chat_id': callback_query.message.chat.id}.items()) in message_user_set:
        return
    # Извлекаем выбранный вопрос из callback_data
    questions = callback_query.data[len(ACTION_PREFIX):].split(";")[0]    

    if questions == '@QuitAndSave':
        finish(callback_query.message)
        return
    # Загружаем кнопки с ответами на данный вопрос
    keyboard = get_buttons_with_questions(questions, callback_query.message if is_admin(callback_query.message) else None)
    if not(keyboard):
        keyboard = get_buttons_with_questions_detail(questions, callback_query.message if is_admin(callback_query.message) else None)
    # Отправляем сообщение с вопросом и кнопками
    last_question = bot.send_message(callback_query.message.chat.id, "Выберите операцию", reply_markup=keyboard)
    message_user_set.add({'message_id': last_question.message_id, 'chat_id': last_question.chat.id})

# Обработчик нажатия на кнопку с количеством
@bot.callback_query_handler(lambda query: query.data.startswith(QUANTITY_PREFIX))
def process_questions_detail_callback(callback_query):
    global message_user_set
    bot.delete_message(callback_query.message.chat.id, callback_query.message.message_id)
    if not frozenset({'message_id': callback_query.message.message_id, 'chat_id': callback_query.message.chat.id}.items()) in message_user_set:
        return
    # Извлекаем выбранный ответ и количество из callback_data
    data = callback_query.data[len(QUANTITY_PREFIX):].split('_')
    if data[0] == '@QuitAndSave':
        finish(callback_query.message)
        return
    model = model_by_id(data[0])
    question_detail = question_detail_by_id(data[1])
    bot.send_message(callback_query.message.chat.id, question_detail)
    # Задаем вопрос - укажите количество
    last_question = bot.send_message(callback_query.message.chat.id, 'Укажите количество:')
    message_user_set.add({'message_id': 'Укажите количество:', 'chat_id': last_question.chat.id})
    # Сохраняем ответ на вопрос
    bot.user_data[callback_query.message.chat.id] = {'Модель':model,'Операция': question_detail}

def save(message):
    #bot.send_message(message.chat.id, 'Спасибо отчет за день сохранен.')
    # Сохраняем ответ и отправляем отчет
    save_message_text = 'Минуту, сохраняю данные...'
    if frozenset({'message_id': save_message_text, 'chat_id': message.chat.id}.items()) in message_user_set:
        return
    smessage = bot.send_message(message.chat.id, save_message_text)
    message_user_set.add({'message_id': save_message_text, 'chat_id': smessage.chat.id})
    quantity = int(message.text)
    user_data = bot.user_data.get(message.chat.id, {})
    row = [message.from_user.first_name, message.from_user.id, user_data.get('Модель', ''), user_data.get('Операция', ''), quantity, datetime.now(pytz.utc).strftime('%d.%m.%Y'),datetime.now(pytz.utc).strftime('%d.%m.%Y %H:%M:%S')]
    answers_sheet.insert_row(row,index=2)
    """
    formula = "=ВПР(C2&D2;'"+questions_sheet_name+"'!E:F;2;ЛОЖЬ)*E2"
    answers_sheet.update_cell(2,9, formula)
    formula = "=ЕСЛИ(ПСТР(G2; 4; 7) = ПСТР(ТДАТА(); 4; 7);1;0)"
    answers_sheet.update_cell(2,10, formula)
    formula = "=ЕСЛИОШИБКА(ВПР(B2;'"+goals_sheet_name+"'!A:C;3;ЛОЖЬ);A2)"
    answers_sheet.update_cell(2,11, formula)
    """

    # Создаем список объектов Cell
    cells_to_update = []

    # Формула 1
    formula1 = "=ВПР(C2&D2;'"+questions_sheet_name+"'!E:F;2;ЛОЖЬ)*E2"
    cells_to_update.append(gspread.Cell(2, 9, formula1))  # Ячейка (2, 9)
    # Формула 2
    formula2 = "=ЕСЛИ(ПСТР(G2; 4; 7) = ПСТР(ТДАТА(); 4; 7);1;0)"
    cells_to_update.append(gspread.Cell(2, 10, formula2))  # Ячейка (2, 10)
    # Формула 3
    formula3 = "=ЕСЛИОШИБКА(ВПР(B2;'"+goals_sheet_name+"'!A:C;3;ЛОЖЬ);A2)"
    cells_to_update.append(gspread.Cell(2, 11, formula3))  # Ячейка (2, 11)
    # Обновляем все ячейки разом
    answers_sheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')

    bot.delete_message(smessage.chat.id, smessage.message_id)
    bot.user_data[message.chat.id] = set()
    # Отправляем сообщение об успешном сохранении ответа
    report_command(message,'Продолжим заполнение отчета?')

def get_results(message, date = ''):
    today = datetime.now(pytz.utc).strftime('%d.%m.%Y')
    if len(date) == 0:
        date = today
    results_detail = gc.worksheet(results_sheet_name).get(results_range)
    if results_detail and len(results_detail[0]) > 1:
        for result in results_detail:
            if result[1] == str(message.chat.id) and result[2] == str(date):
                if len(result[2]) > 0:
                    return result[3]
    return '0'

def get_results_admin(message, date = ''):
    today = datetime.now(pytz.utc).strftime('%d.%m.%Y')
    if len(date) == 0:
        date = today
    results_detail = gc.worksheet(results_sheet_name).get(results_range)
    if results_detail and len(results_detail[0]) > 1:
        for result in results_detail:
            if result[1] == str(message.chat.id) and result[2] == str(date):
                if len(result[2]) > 0:
                    return result[3]
    return '0'

def finish(message,done = 1):
    smessage = bot.send_message(message.chat.id, 'Считаю результат...')
    #result = get_results(message)
    #reload_data(scope='g+s+a+q+r',force=True)
    reload_data(scope='r',force=True)
    results_detail = results    
    #setting_rule = get_setting(name = 'Смайлик+Шутка+Показывать сумму')

    """
    month = datetime.now().month
    year = datetime.now().year
    day = datetime.now().day

    detail = pd.DataFrame(questions_detail, columns=['Модель',	'Группа',	'Операция',	'Общее количество'	,'МодельОперация',	'Цена'	,'Секунды'])
    answer = pd.DataFrame(answers, columns=['Сотрудник имя',	'Сотрудник ID'	,'Модель',	'Операция',	'Количество',	'Дата',	'Время',	'Признак удаления']) \
    .loc[lambda x: x['Сотрудник ID'] == str(message.chat.id)] \
    .loc[lambda x: x['Признак удаления'].isnull()] \
    .loc[lambda x: x['Дата'] == datetime.now().strftime('%d.%m.%Y')]
    

    merge = pd.merge(detail, answer, on=['Модель', 'Операция'])
    merge[['Количество', 'Секунды']] = merge[['Количество', 'Секунды']].fillna(0).astype(float)
    merge['Дата'] = pd.to_datetime(merge['Дата'],format='%d.%m.%Y')
    merge['Активность'] = merge['Количество']*merge['Секунды']

    group = merge.groupby(['Сотрудник ID', 'Дата'])['Активность'].sum().reset_index()
    group['Активность'] = group['Активность'].apply(lambda x: str(timedelta(seconds=x)))"""

    user_result_detail = pd.DataFrame(results_detail).loc[lambda x:x[1] == str(message.chat.id)].loc[lambda x:x[2] == datetime.now().strftime('%d.%m.%Y')]

    """
    result_float =  float(user_result_detail.values[0][3].replace(',','.').replace(u'\xa0', u'')) if user_result_detail.size != 0 else 0.0
    uni_goal_value = 0.0
    goal_value = 0.0
    #Получаем цели    
    for goal in goals:
        if goal[0] == '*':
            uni_goal_value = float(goal[1].replace(',','.').replace(u'\xa0', u''))
        if goal[0] == str(message.chat.id):
            goal_value = float(goal[1].replace(',','.').replace(u'\xa0', u''))

    goal_value = goal_value if goal_value > 0 else uni_goal_value

    emoji = ''
    emoji_text = ''

    if result_float > 0 and next(filter(lambda x: x[0] == 'Смайлик', setting_rule), None)[1] == '1':
        if result_float >= goal_value:
            emoji = '😉'
            emoji_text = 'Так держать, хороший результат!'
        if result_float >= goal_value*2:
            emoji = '🥳'
            emoji_text = 'Вот это да! Великолепный результат!'
        if result_float < goal_value:
            emoji = '😏'
            emoji_text = 'Хорошо, но я знаю ты можешь и лучше!'
        if result_float < goal_value*0.5:
            emoji = '😔'
            emoji_text = 'Пока не очень хорошо получается, давай поднажмем!'
    """
    bot.delete_message(smessage.chat.id, smessage.message_id)

    if done == 1:
        bot.send_message(message.chat.id, 'Спасибо за работу!')
        if user_result_detail.size != 0:
            bot.send_message(message.chat.id, 'Ваша активность сегодня - ' + user_result_detail.values[0][5] + ' ч.')
        #if next(filter(lambda x: x[0] == 'Показывать сумму', setting_rule), None)[1] == '1':
            #bot.send_message(message.chat.id, 'Вы заработали - ' + str(result_float))
            bot.send_message(message.chat.id, 'Вы заработали - ' + user_result_detail.values[0][3])
    """"
    if len(emoji) > 0 and next(filter(lambda x: x[0] == 'Шутка', setting_rule), None)[1] == '1':
        bot.send_message(message.chat.id, emoji)
        bot.send_message(message.chat.id, emoji_text)        
        bot.send_message(message.chat.id, get_anokdot(), parse_mode= 'Markdown')
    """
    # response = requests.get('http://www.anekdot.ru/rss/random.html')
    # result = response.text.encode('latin1').decode('cp1251').split('[\\"')[1].replace("\\","").replace("<br>","\n").split("\",\"")
    # for joke in result:
    #     if joke.find("Путин") == -1:
    #         bot.send_message(message.chat.id, 'Держи шутку:\n' + joke, parse_mode="HTML")
    #         break    
    #if done == 1:
    #    def_command(message, 'До встречи!')
    #else:
    #    def_command(message, 'Чем еще могу помочь?')

@bot.message_handler(func=lambda message: True)
def quantity_handler(message):
    # Отправляем клавиатуру с кнопкой "Меню" пользователю
    #bot.send_message(message.chat.id, 'Выберите действие:', reply_markup=menu_keyboard)
    #bot.send_message(chat_id=message.from_user.id, text='Выберите продукт:', reply_markup=keyboard)

    if message.text == '@QuitAndSave':
        save(message)
        return
    # else:
    #     user_data = bot.user_data.get(message.chat.id, {})
    #     if len(user_data) == 0:
    #         bot.send_message(message.chat.id, 'Извините, я пока не понимаю этой команды')
    #         def_command(message)
    #         return

    global message_user_set
    if not frozenset({'message_id': 'Укажите количество:', 'chat_id': message.chat.id}.items()) in message_user_set:
        bot.send_message(message.chat.id, 'Извините, я не понимаю этой команды')
        # bot.user_data[message.chat.id] = set()
        # def_command(message)
        return
    #if len(user_data) == 0:
        #start_command(message)
        #return
    # Проверяем, является ли сообщение числом
    try:
        quantity = int(message.text)
        if quantity <= 0:
            bot.send_message(message.chat.id, 'Укажите число больше нуля')
            return
    except ValueError:
        bot.send_message(message.chat.id, 'Укажите целое число')
        return
    #save(message)
    try:
        save(message)
    except Exception as e:
        bot.send_message(message.chat.id, '🤷‍♂️')
        bot.send_message(message.chat.id, 'Что-то пошло не так, пожалуйста, повторите...')
        #print('Save error: {}'.format(e))
        return
    
# def main():
#     while True:
#         try:
#             reload_data()
#             bot.polling(none_stop=True)
#         except Exception as e:
#             try:
#                 logs_sheet.append_row(['ERROR',datetime.now(pytz.utc).strftime('%d.%m.%Y %H:%M:%S'),'*','Error: {}'.format(e)])
#             except Exception as e:
#                 continue
#         time.sleep(2) 


# thread_main = threading.Thread(target=main)
# thread_main.daemon = True
# thread_main.start()

def exception_hook(*args):
    sleep = 5;
    print('Exception! Sleep {} sec'.format(sleep))
    time.sleep(sleep)

#class ExHandler: 
#    def handle(self, error): 
#        print('Error: ', error)

def run_bot_polling():
#    bot.exception_handler = exception_hook            
    while True:
        try:
            bot.polling(none_stop=True)
        except Exception as e:
            try:
                logs_sheet.append_row(['ERROR',datetime.now(pytz.utc).strftime('%d.%m.%Y %H:%M:%S'),'*','Error: {}'.format(e)])
            except Exception as e:
                exception_hook(e)       

async def main():
    reload_data()

#    bot_thread = threading.Thread(target=notify)
#    bot_thread.daemon = True
#    bot_thread.start()

    # Выполнение другой работы в основном потоке
    # Запуск бота в отдельном потоке
    bot_thread = threading.Thread(target=run_bot_polling)
    bot_thread.daemon = True
    bot_thread.start()

    # Ожидание завершения задачи
    while True:
        time.sleep(60)
        
import sys


if __name__ == '__main__':
    # Запуск асинхронного цикла событий в отдельном потоке
    asyncio.run(main())