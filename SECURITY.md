import logging

import sqlite3

import requests

import time

import re

import os

import pandas as pd

from datetime import datetime

from html import escape

from collections import defaultdict

import io



try:

&nbsp;   from dotenv import load\_dotenv

&nbsp;   load\_dotenv()

except ImportError:

&nbsp;   pass



BOT\_TOKEN = os.environ.get('BOT\_TOKEN')

if not BOT\_TOKEN:

&nbsp;   logging.error("BOT\_TOKEN environment variable is not set!")

&nbsp;   exit(1)



ADMINS = \[admin.strip() for admin in os.environ.get('ADMINS', 'r1kuza,nadya\_yakovleva01,Priikalist').split(',') if admin.strip()]



MAX\_MESSAGE\_LENGTH = 4000

MAX\_USERS\_PER\_CLASS = 30

MAX\_REQUESTS\_PER\_MINUTE = 20



BASE\_URL = f"https://api.telegram.org/bot{BOT\_TOKEN}"



logging.basicConfig(

&nbsp;   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',

&nbsp;   level=logging.INFO

)

logger = logging.getLogger(\_\_name\_\_)



class RateLimiter:

&nbsp;   def \_\_init\_\_(self, max\_requests=MAX\_REQUESTS\_PER\_MINUTE, window=60):

&nbsp;       self.requests = defaultdict(list)

&nbsp;       self.max\_requests = max\_requests

&nbsp;       self.window = window

&nbsp;   

&nbsp;   def is\_limited(self, user\_id):

&nbsp;       now = time.time()

&nbsp;       user\_requests = self.requests\[user\_id]

&nbsp;       user\_requests = \[req for req in user\_requests if now - req < self.window]

&nbsp;       

&nbsp;       if len(user\_requests) >= self.max\_requests:

&nbsp;           return True

&nbsp;       

&nbsp;       user\_requests.append(now)

&nbsp;       self.requests\[user\_id] = user\_requests\[-self.max\_requests:]

&nbsp;       return False



class SimpleSchoolBot:

&nbsp;   def \_\_init\_\_(self):

&nbsp;       self.last\_update\_id = 0

&nbsp;       self.admin\_states = {}

&nbsp;       self.user\_states = {}

&nbsp;       self.processed\_updates = set()

&nbsp;       self.rate\_limiter = RateLimiter()

&nbsp;       self.init\_db()

&nbsp;   

&nbsp;   def init\_db(self):

&nbsp;       db\_path = os.environ.get('DATABASE\_PATH', 

&nbsp;                               os.path.join(os.path.dirname(os.path.abspath(\_\_file\_\_)), "school\_bot.db"))

&nbsp;       self.conn = sqlite3.connect(db\_path, check\_same\_thread=False)

&nbsp;       self.create\_tables()

&nbsp;   

&nbsp;   def create\_tables(self):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.executescript("""

&nbsp;           CREATE TABLE IF NOT EXISTS users (

&nbsp;               user\_id INTEGER PRIMARY KEY,

&nbsp;               full\_name TEXT NOT NULL,

&nbsp;               class TEXT NOT NULL,

&nbsp;               role TEXT DEFAULT 'user',

&nbsp;               registered\_at TIMESTAMP DEFAULT CURRENT\_TIMESTAMP

&nbsp;           );

&nbsp;           

&nbsp;           CREATE TABLE IF NOT EXISTS schedule (

&nbsp;               id INTEGER PRIMARY KEY AUTOINCREMENT,

&nbsp;               class TEXT NOT NULL,

&nbsp;               day TEXT NOT NULL,

&nbsp;               lesson\_number INTEGER,

&nbsp;               subject TEXT,

&nbsp;               teacher TEXT,

&nbsp;               room TEXT,

&nbsp;               UNIQUE(class, day, lesson\_number)

&nbsp;           );



&nbsp;           CREATE TABLE IF NOT EXISTS bell\_schedule (

&nbsp;               lesson\_number INTEGER PRIMARY KEY,

&nbsp;               start\_time TEXT NOT NULL,

&nbsp;               end\_time TEXT NOT NULL

&nbsp;           );

&nbsp;       """)

&nbsp;       

&nbsp;       cursor.execute("SELECT COUNT(\*) FROM bell\_schedule")

&nbsp;       if cursor.fetchone()\[0] == 0:

&nbsp;           bell\_schedule = \[

&nbsp;               (1, '8:00', '8:40'),

&nbsp;               (2, '8:50', '9:30'),

&nbsp;               (3, '9:40', '10:20'),

&nbsp;               (4, '10:30', '11:10'),

&nbsp;               (5, '11:25', '12:05'),

&nbsp;               (6, '12:10', '12:50'),

&nbsp;               (7, '13:00', '13:40')

&nbsp;           ]

&nbsp;           cursor.executemany(

&nbsp;               "INSERT OR REPLACE INTO bell\_schedule (lesson\_number, start\_time, end\_time) VALUES (?, ?, ?)",

&nbsp;               bell\_schedule

&nbsp;           )

&nbsp;       

&nbsp;       self.conn.commit()

&nbsp;   

&nbsp;   def safe\_message(self, text):

&nbsp;       if not text:

&nbsp;           return ""

&nbsp;       text = str(text)

&nbsp;       # Удаляем HTML теги

&nbsp;       text = re.sub(r'<\[^>]+>', '', text)

&nbsp;       # Экранируем оставшийся текст

&nbsp;       text = escape(text)

&nbsp;       return text

&nbsp;   

&nbsp;   def truncate\_message(self, text, max\_length=MAX\_MESSAGE\_LENGTH):

&nbsp;       if len(text) <= max\_length:

&nbsp;           return text

&nbsp;       return text\[:max\_length-3] + "..."

&nbsp;   

&nbsp;   def send\_message(self, chat\_id, text, reply\_markup=None):

&nbsp;       safe\_text = self.truncate\_message(self.safe\_message(text))

&nbsp;       

&nbsp;       url = f"{BASE\_URL}/sendMessage"

&nbsp;       data = {

&nbsp;           "chat\_id": chat\_id,

&nbsp;           "text": safe\_text,

&nbsp;           "parse\_mode": "HTML"

&nbsp;       }

&nbsp;       if reply\_markup:

&nbsp;           data\["reply\_markup"] = reply\_markup

&nbsp;       

&nbsp;       try:

&nbsp;           response = requests.post(url, json=data, timeout=10)

&nbsp;           return response.json()

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка отправки сообщения: {e}")

&nbsp;           return None



&nbsp;   def send\_document(self, chat\_id, document, filename=None):

&nbsp;       url = f"{BASE\_URL}/sendDocument"

&nbsp;       data = {"chat\_id": chat\_id}

&nbsp;       files = {"document": (filename, document)}

&nbsp;       

&nbsp;       try:

&nbsp;           response = requests.post(url, data=data, files=files, timeout=30)

&nbsp;           return response.json()

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка отправки документа: {e}")

&nbsp;           return None

&nbsp;   

&nbsp;   def get\_file(self, file\_id):

&nbsp;       url = f"{BASE\_URL}/getFile"

&nbsp;       data = {"file\_id": file\_id}

&nbsp;       

&nbsp;       try:

&nbsp;           response = requests.post(url, json=data, timeout=10)

&nbsp;           result = response.json()

&nbsp;           if result.get("ok"):

&nbsp;               return result\["result"]

&nbsp;           return None

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка получения файла: {e}")

&nbsp;           return None

&nbsp;   

&nbsp;   def download\_file(self, file\_path):

&nbsp;       url = f"https://api.telegram.org/file/bot{BOT\_TOKEN}/{file\_path}"

&nbsp;       

&nbsp;       try:

&nbsp;           response = requests.get(url, timeout=30)

&nbsp;           if response.status\_code == 200:

&nbsp;               return response.content

&nbsp;           return None

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка загрузки файла: {e}")

&nbsp;           return None

&nbsp;   

&nbsp;   def log\_security\_event(self, event\_type, user\_id, details):

&nbsp;       logger.warning(f"SECURITY: {event\_type} - User: {user\_id} - {details}")

&nbsp;   

&nbsp;   def get\_updates(self):

&nbsp;       url = f"{BASE\_URL}/getUpdates"

&nbsp;       params = {

&nbsp;           "offset": self.last\_update\_id + 1,

&nbsp;           "timeout": 10,

&nbsp;           "limit": 100

&nbsp;       }

&nbsp;       

&nbsp;       try:

&nbsp;           response = requests.get(url, params=params, timeout=15)

&nbsp;           result = response.json()

&nbsp;           

&nbsp;           if not result.get("ok") and "Conflict" in str(result.get("description", "")):

&nbsp;               logger.warning("Обнаружен конфликт getUpdates")

&nbsp;               return {"ok": False, "conflict": True}

&nbsp;               

&nbsp;           return result

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка получения обновлений: {e}")

&nbsp;           return {"ok": False}

&nbsp;   

&nbsp;   def get\_user(self, user\_id):

&nbsp;       if not self.is\_valid\_user\_id(user\_id):

&nbsp;           return None

&nbsp;           

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("SELECT \* FROM users WHERE user\_id = ?", (user\_id,))

&nbsp;       return cursor.fetchone()

&nbsp;   

&nbsp;   def is\_valid\_user\_id(self, user\_id):

&nbsp;       return isinstance(user\_id, int) and user\_id > 0

&nbsp;   

&nbsp;   def create\_user(self, user\_id, full\_name, class\_name):

&nbsp;       if not self.is\_valid\_user\_id(user\_id):

&nbsp;           return False

&nbsp;           

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("SELECT COUNT(\*) FROM users WHERE class = ?", (class\_name,))

&nbsp;       count = cursor.fetchone()\[0]

&nbsp;       

&nbsp;       if count >= MAX\_USERS\_PER\_CLASS:

&nbsp;           self.log\_security\_event("class\_limit\_exceeded", user\_id, f"Class: {class\_name}")

&nbsp;           return False

&nbsp;       

&nbsp;       cursor.execute(

&nbsp;           "INSERT OR REPLACE INTO users (user\_id, full\_name, class) VALUES (?, ?, ?)",

&nbsp;           (user\_id, full\_name, class\_name)

&nbsp;       )

&nbsp;       self.conn.commit()

&nbsp;       return True

&nbsp;   

&nbsp;   def delete\_user(self, user\_id):

&nbsp;       if not self.is\_valid\_user\_id(user\_id):

&nbsp;           return False

&nbsp;           

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("DELETE FROM users WHERE user\_id = ?", (user\_id,))

&nbsp;       self.conn.commit()

&nbsp;       return cursor.rowcount > 0

&nbsp;   

&nbsp;   def get\_all\_users(self):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("SELECT user\_id, full\_name, class, registered\_at FROM users ORDER BY registered\_at DESC")

&nbsp;       return cursor.fetchall()

&nbsp;   

&nbsp;   def get\_schedule(self, class\_name, day):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute(

&nbsp;           "SELECT lesson\_number, subject, teacher, room FROM schedule WHERE class = ? AND day = ? ORDER BY lesson\_number",

&nbsp;           (class\_name, day)

&nbsp;       )

&nbsp;       return cursor.fetchall()

&nbsp;   

&nbsp;   def save\_schedule(self, class\_name, day, lessons):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("DELETE FROM schedule WHERE class = ? AND day = ?", (class\_name, day))

&nbsp;       

&nbsp;       for lesson\_num, subject, teacher, room in lessons:

&nbsp;           subject = subject\[:100] if subject else ""

&nbsp;           teacher = teacher\[:50] if teacher else ""

&nbsp;           room = room\[:20] if room else ""

&nbsp;           

&nbsp;           cursor.execute(

&nbsp;               "INSERT OR REPLACE INTO schedule (class, day, lesson\_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?)",

&nbsp;               (class\_name, day, lesson\_num, subject, teacher, room)

&nbsp;           )

&nbsp;       

&nbsp;       self.conn.commit()

&nbsp;   

&nbsp;   def get\_bell\_schedule(self):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("SELECT lesson\_number, start\_time, end\_time FROM bell\_schedule ORDER BY lesson\_number")

&nbsp;       return cursor.fetchall()

&nbsp;   

&nbsp;   def is\_admin(self, username):

&nbsp;       return username and username.lower() in \[admin.lower() for admin in ADMINS]

&nbsp;   

&nbsp;   def main\_menu\_keyboard(self):

&nbsp;       return {

&nbsp;           "keyboard": \[

&nbsp;               \[{"text": "📚 Моё расписание"}, {"text": "🏫 Общее расписание"}],

&nbsp;               \[{"text": "🔔 Звонки"}, {"text": "ℹ️ Помощь"}]

&nbsp;           ],

&nbsp;           "resize\_keyboard": True

&nbsp;       }

&nbsp;   

&nbsp;   def admin\_menu\_keyboard(self):

&nbsp;       return {

&nbsp;           "keyboard": \[

&nbsp;               \[{"text": "👥 Список пользователей"}, {"text": "❌ Удалить пользователя"}],

&nbsp;               \[{"text": "📝 Редактировать расписание"}, {"text": "🏫 Управление классами"}],

&nbsp;               \[{"text": "🕧 Управление звонками"}, {"text": "📤 Загрузить Excel"}],

&nbsp;               \[{"text": "📊 Статистика"}, {"text": "⬅️ Назад"}]

&nbsp;           ],

&nbsp;           "resize\_keyboard": True

&nbsp;       }

&nbsp;   

&nbsp;   def classes\_management\_keyboard(self):

&nbsp;       return {

&nbsp;           "keyboard": \[

&nbsp;               \[{"text": "➕ Добавить класс"}, {"text": "➖ Удалить класс"}],

&nbsp;               \[{"text": "⬅️ Назад в админку"}]

&nbsp;           ],

&nbsp;           "resize\_keyboard": True

&nbsp;       }

&nbsp;   

&nbsp;   def bells\_management\_keyboard(self):

&nbsp;       return {

&nbsp;           "keyboard": \[

&nbsp;               \[{"text": "✏️ Изменить звонок"}, {"text": "👀 Посмотреть все звонки"}],

&nbsp;               \[{"text": "⬅️ Назад в админку"}]

&nbsp;           ],

&nbsp;           "resize\_keyboard": True

&nbsp;       }

&nbsp;   

&nbsp;   def day\_selection\_inline\_keyboard(self):

&nbsp;       """Inline клавиатура для выбора дней недели (вертикальное расположение)"""

&nbsp;       return {

&nbsp;           "inline\_keyboard": \[

&nbsp;               \[{"text": "Понедельник", "callback\_data": "day\_monday"}],

&nbsp;               \[{"text": "Вторник", "callback\_data": "day\_tuesday"}],

&nbsp;               \[{"text": "Среда", "callback\_data": "day\_wednesday"}],

&nbsp;               \[{"text": "Четверг", "callback\_data": "day\_thursday"}],

&nbsp;               \[{"text": "Пятница", "callback\_data": "day\_friday"}],

&nbsp;               \[{"text": "Суббота", "callback\_data": "day\_saturday"}]

&nbsp;           ]

&nbsp;       }

&nbsp;   

&nbsp;   def class\_selection\_keyboard(self):

&nbsp;       """Клавиатура для выбора класса"""

&nbsp;       classes = \[]

&nbsp;       

&nbsp;       for grade in range(5, 10):

&nbsp;           for letter in \['А', 'Б', 'В']:

&nbsp;               classes.append(f"{grade}{letter}")

&nbsp;       

&nbsp;       classes.extend(\["10П", "10Р", "11Р"])

&nbsp;       

&nbsp;       keyboard = \[]

&nbsp;       row = \[]

&nbsp;       for i, cls in enumerate(classes):

&nbsp;           row.append({"text": cls})

&nbsp;           if (i + 1) % 3 == 0:

&nbsp;               keyboard.append(row)

&nbsp;               row = \[]

&nbsp;       if row:

&nbsp;           keyboard.append(row)

&nbsp;       

&nbsp;       keyboard.append(\[{"text": "⬅️ Назад"}])

&nbsp;       

&nbsp;       return {"keyboard": keyboard, "resize\_keyboard": True}

&nbsp;   

&nbsp;   def shift\_selection\_keyboard(self):

&nbsp;       """Клавиатура для выбора смены"""

&nbsp;       return {

&nbsp;           "keyboard": \[

&nbsp;               \[{"text": "1 смена"}, {"text": "2 смена"}],

&nbsp;               \[{"text": "❌ Отменить"}]

&nbsp;           ],

&nbsp;           "resize\_keyboard": True

&nbsp;       }

&nbsp;   

&nbsp;   def cancel\_keyboard(self):

&nbsp;       return {

&nbsp;           "keyboard": \[\[{"text": "❌ Отменить"}]],

&nbsp;           "resize\_keyboard": True

&nbsp;       }

&nbsp;   

&nbsp;   def is\_valid\_class(self, class\_str):

&nbsp;       class\_str = class\_str.strip().upper()

&nbsp;       

&nbsp;       if re.match(r'^\[5-9]\[А-В]$', class\_str):

&nbsp;           return True

&nbsp;       

&nbsp;       if class\_str in \['10П', '10Р', '11Р']:

&nbsp;           return True

&nbsp;       

&nbsp;       return False

&nbsp;   

&nbsp;   def is\_valid\_fullname(self, name):

&nbsp;       name = name.strip()

&nbsp;       if len(name) > 100:

&nbsp;           return False

&nbsp;           

&nbsp;       parts = name.split()

&nbsp;       if len(parts) < 2:

&nbsp;           return False

&nbsp;       

&nbsp;       for part in parts:

&nbsp;           if not part.isalpha() or len(part) < 2 or len(part) > 20:

&nbsp;               return False

&nbsp;       

&nbsp;       return True

&nbsp;   

&nbsp;   def is\_valid\_time(self, time\_str):

&nbsp;       return bool(re.match(r'^(\[0-1]?\[0-9]|2\[0-3]):\[0-5]\[0-9]$', time\_str))

&nbsp;   

&nbsp;   def get\_existing\_classes(self):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("SELECT DISTINCT class FROM users ORDER BY class")

&nbsp;       return \[row\[0] for row in cursor.fetchall()]

&nbsp;   

&nbsp;   def add\_class(self, class\_name):

&nbsp;       return self.is\_valid\_class(class\_name)

&nbsp;   

&nbsp;   def delete\_class(self, class\_name):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute("DELETE FROM users WHERE class = ?", (class\_name,))

&nbsp;       deleted\_count = cursor.rowcount

&nbsp;       self.conn.commit()

&nbsp;       return deleted\_count > 0

&nbsp;   

&nbsp;   def update\_bell\_schedule(self, lesson\_number, start\_time, end\_time):

&nbsp;       cursor = self.conn.cursor()

&nbsp;       cursor.execute(

&nbsp;           "UPDATE bell\_schedule SET start\_time = ?, end\_time = ? WHERE lesson\_number = ?",

&nbsp;           (start\_time, end\_time, lesson\_number)

&nbsp;       )

&nbsp;       self.conn.commit()

&nbsp;       return cursor.rowcount > 0



&nbsp;   def parse\_excel\_schedule(self, file\_content, shift):

&nbsp;       """Парсинг Excel файла для конкретной смены с улучшенным логированием"""

&nbsp;       try:

&nbsp;           import pandas as pd

&nbsp;           

&nbsp;           lessons\_data = \[]

&nbsp;           

&nbsp;           logger.info(f"Начинаем парсинг Excel файла для смены {shift}")

&nbsp;           

&nbsp;           # Сначала получим список всех листов в файле

&nbsp;           try:

&nbsp;               excel\_file = pd.ExcelFile(io.BytesIO(file\_content))

&nbsp;               sheet\_names = excel\_file.sheet\_names

&nbsp;               logger.info(f"Доступные листы в файле: {sheet\_names}")

&nbsp;               

&nbsp;               # Определим возможные названия листа для смены

&nbsp;               possible\_sheet\_names = \[

&nbsp;                   f"{shift} СМЕНА",

&nbsp;                   f"{shift} смена", 

&nbsp;                   f"{shift} смена",

&nbsp;                   f"Смена {shift}",

&nbsp;                   f"СМЕНА {shift}",

&nbsp;                   f"1 СМЕНА",  # Для обратной совместимости

&nbsp;                   "1 СМЕНА"   # Для обратной совместимости

&nbsp;               ]

&nbsp;               

&nbsp;               # Добавим все листы, если не нашли по стандартным именам

&nbsp;               if shift == "1":

&nbsp;                   possible\_sheet\_names.extend(sheet\_names)

&nbsp;               

&nbsp;               # Попробуем найти подходящий лист

&nbsp;               selected\_sheet = None

&nbsp;               for sheet\_name in possible\_sheet\_names:

&nbsp;                   if sheet\_name in sheet\_names:

&nbsp;                       selected\_sheet = sheet\_name

&nbsp;                       logger.info(f"Найден лист: {selected\_sheet}")

&nbsp;                       break

&nbsp;               

&nbsp;               if not selected\_sheet and sheet\_names:

&nbsp;                   # Если не нашли по имени, возьмем первый лист

&nbsp;                   selected\_sheet = sheet\_names\[0]

&nbsp;                   logger.warning(f"Лист для смены {shift} не найден, используем первый лист: {selected\_sheet}")

&nbsp;               

&nbsp;               if not selected\_sheet:

&nbsp;                   logger.error("В файле нет листов!")

&nbsp;                   return None

&nbsp;               

&nbsp;               # Читаем выбранный лист

&nbsp;               df = pd.read\_excel(io.BytesIO(file\_content), sheet\_name=selected\_sheet, header=None)

&nbsp;               logger.info(f"Смена {shift}: успешно загружен лист '{selected\_sheet}', размер {df.shape}, колонки: {len(df.columns)}")

&nbsp;               

&nbsp;               # Логируем информацию о файле для отладки

&nbsp;               logger.info(f"Первые 10 строк и 10 колонок файла:")

&nbsp;               for i in range(min(10, len(df))):

&nbsp;                   row\_preview = df.iloc\[i, :min(10, len(df.columns))].tolist()

&nbsp;                   logger.info(f"Строка {i}: {row\_preview}")

&nbsp;               

&nbsp;               # Парсим таблицу

&nbsp;               self.\_parse\_table\_schedule(df, shift, lessons\_data, selected\_sheet)

&nbsp;               

&nbsp;           except Exception as e:

&nbsp;               logger.error(f"Ошибка чтения Excel файла для смены {shift}: {e}")

&nbsp;               logger.error(f"Тип ошибки: {type(e).\_\_name\_\_}")

&nbsp;               import traceback

&nbsp;               logger.error(f"Трассировка: {traceback.format\_exc()}")

&nbsp;               return None

&nbsp;           

&nbsp;           logger.info(f"Для смены {shift} найдено {len(lessons\_data)} уроков")

&nbsp;           return lessons\_data if lessons\_data else None

&nbsp;           

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Общая ошибка парсинга Excel для смены {shift}: {e}")

&nbsp;           logger.error(f"Тип ошибки: {type(e).\_\_name\_\_}")

&nbsp;           import traceback

&nbsp;           logger.error(f"Трассировка: {traceback.format\_exc()}")

&nbsp;           return None



&nbsp;   def \_parse\_table\_schedule(self, df, shift, lessons\_data, sheet\_name):

&nbsp;       """Парсинг табличного расписания с улучшенным логированием"""

&nbsp;       try:

&nbsp;           logger.info(f"Парсим смену {shift}, лист '{sheet\_name}', размер таблицы: {df.shape}")

&nbsp;           

&nbsp;           # Ищем строку с заголовками классов

&nbsp;           class\_row\_idx = self.\_find\_class\_header\_row(df)

&nbsp;           if class\_row\_idx is None:

&nbsp;               logger.error("Не удалось найти строку с заголовками классов")

&nbsp;               # Попробуем найти классы в других строках

&nbsp;               for i in range(min(20, len(df))):

&nbsp;                   row\_classes = self.\_find\_classes\_in\_row(df, i)

&nbsp;                   if row\_classes:

&nbsp;                       class\_row\_idx = i

&nbsp;                       logger.info(f"Найдены классы в строке {i}: {list(row\_classes.keys())}")

&nbsp;                       break

&nbsp;               

&nbsp;               if class\_row\_idx is None:

&nbsp;                   logger.error("Не удалось найти классы ни в одной строке")

&nbsp;                   return

&nbsp;           

&nbsp;           logger.info(f"Найдена строка с классами: строка {class\_row\_idx}")

&nbsp;           logger.info(f"Содержимое строки классов: {df.iloc\[class\_row\_idx].fillna('').tolist()}")

&nbsp;           

&nbsp;           # Собираем информацию о классах и их колонках

&nbsp;           class\_columns = self.\_extract\_class\_columns(df, class\_row\_idx)

&nbsp;           if not class\_columns:

&nbsp;               logger.error("Не удалось определить классы и их колонки")

&nbsp;               return

&nbsp;           

&nbsp;           logger.info(f"Найдены классы и колонки: {class\_columns}")

&nbsp;           

&nbsp;           # Ищем строки с уроками

&nbsp;           lesson\_rows = self.\_find\_lesson\_rows(df, class\_row\_idx + 1)

&nbsp;           if not lesson\_rows:

&nbsp;               logger.error("Не удалось найти строки с уроками")

&nbsp;               return

&nbsp;           

&nbsp;           logger.info(f"Найдены строки с уроками: {lesson\_rows}")

&nbsp;           

&nbsp;           # Парсим каждый урок

&nbsp;           for row\_idx, lesson\_num in lesson\_rows:

&nbsp;               self.\_parse\_lesson\_row(df, row\_idx, lesson\_num, class\_columns, lessons\_data, shift)

&nbsp;           

&nbsp;           logger.info(f"Успешно распаршено {len(lessons\_data)} уроков для смены {shift}")

&nbsp;           

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка парсинга таблицы для смены {shift}: {e}")

&nbsp;           logger.error(f"Тип ошибки: {type(e).\_\_name\_\_}")

&nbsp;           import traceback

&nbsp;           logger.error(f"Трассировка: {traceback.format\_exc()}")



&nbsp;   def \_find\_class\_header\_row(self, df):

&nbsp;       """Находит строку с заголовками классов"""

&nbsp;       for i in range(min(15, len(df))):

&nbsp;           row = df.iloc\[i]

&nbsp;           class\_count = 0

&nbsp;           for cell in row:

&nbsp;               if pd.notna(cell) and self.\_is\_class\_header(str(cell)):

&nbsp;                   class\_count += 1

&nbsp;           if class\_count >= 2:  # Если найдено несколько классов в строке

&nbsp;               return i

&nbsp;       return None



&nbsp;   def \_find\_classes\_in\_row(self, df, row\_idx):

&nbsp;       """Находит классы в указанной строке"""

&nbsp;       row = df.iloc\[row\_idx]

&nbsp;       class\_columns = {}

&nbsp;       

&nbsp;       for j, cell in enumerate(row):

&nbsp;           if pd.notna(cell):

&nbsp;               cell\_str = str(cell).strip()

&nbsp;               class\_name = self.\_extract\_class\_name(cell\_str)

&nbsp;               if class\_name:

&nbsp;                   class\_columns\[class\_name] = j

&nbsp;       

&nbsp;       return class\_columns



&nbsp;   def \_extract\_class\_columns(self, df, class\_row\_idx):

&nbsp;       """Извлекает информацию о классах и их колонках"""

&nbsp;       class\_columns = {}

&nbsp;       class\_row = df.iloc\[class\_row\_idx]

&nbsp;       

&nbsp;       for j, cell in enumerate(class\_row):

&nbsp;           if pd.notna(cell):

&nbsp;               cell\_str = str(cell).strip()

&nbsp;               class\_name = self.\_extract\_class\_name(cell\_str)

&nbsp;               if class\_name:

&nbsp;                   class\_columns\[class\_name] = j

&nbsp;                   logger.debug(f"Найден класс {class\_name} в колонке {j}")

&nbsp;       

&nbsp;       return class\_columns



&nbsp;   def \_find\_lesson\_rows(self, df, start\_row):

&nbsp;       """Находит строки с номерами уроков"""

&nbsp;       lesson\_rows = \[]

&nbsp;       for i in range(start\_row, min(start\_row + 20, len(df))):  # Ограничиваем поиск 20 строками

&nbsp;           row = df.iloc\[i]

&nbsp;           # Проверяем первую колонку на номер урока

&nbsp;           if pd.notna(row\[0]) and str(row\[0]).strip().isdigit():

&nbsp;               lesson\_num = int(str(row\[0]).strip())

&nbsp;               if 1 <= lesson\_num <= 10:  # Уроки обычно от 1 до 7-10

&nbsp;                   lesson\_rows.append((i, lesson\_num))

&nbsp;                   logger.debug(f"Найден урок {lesson\_num} в строке {i}")

&nbsp;       

&nbsp;       return lesson\_rows



&nbsp;   def \_parse\_lesson\_row(self, df, row\_idx, lesson\_num, class\_columns, lessons\_data, shift):

&nbsp;       """Парсит строку с уроком"""

&nbsp;       row = df.iloc\[row\_idx]

&nbsp;       

&nbsp;       for class\_name, col\_idx in class\_columns.items():

&nbsp;           if col\_idx < len(row) and pd.notna(row\[col\_idx]):

&nbsp;               subject = str(row\[col\_idx]).strip()

&nbsp;               

&nbsp;               # Пропускаем пустые предметы и дни недели

&nbsp;               if not subject or subject in \['-', '—', ''] or any(day in subject.lower() for day in \['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота']):

&nbsp;                   continue

&nbsp;               

&nbsp;               # Ищем кабинет в следующей колонке

&nbsp;               room = ""

&nbsp;               if col\_idx + 1 < len(row) and pd.notna(row\[col\_idx + 1]):

&nbsp;                   room\_cell = str(row\[col\_idx + 1]).strip()

&nbsp;                   if room\_cell and not any(day in room\_cell.lower() for day in \['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота']):

&nbsp;                       room = room\_cell

&nbsp;               

&nbsp;               # Определяем день недели

&nbsp;               day = self.\_detect\_day\_from\_position(df, row\_idx, col\_idx)

&nbsp;               

&nbsp;               lessons\_data.append({

&nbsp;                   'class': class\_name,

&nbsp;                   'day': day,

&nbsp;                   'lesson\_number': lesson\_num,

&nbsp;                   'subject': subject,

&nbsp;                   'room': room,

&nbsp;                   'teacher': '',

&nbsp;                   'shift': shift

&nbsp;               })

&nbsp;               

&nbsp;               logger.debug(f"Добавлен урок: {class\_name}, {day}, {lesson\_num}, {subject}, {room}")



&nbsp;   def \_is\_class\_header(self, text):

&nbsp;       """Проверяет, является ли текст заголовком класса"""

&nbsp;       text = text.lower().strip()

&nbsp;       patterns = \[

&nbsp;           r'^\\d\[абв]$',        # 5а, 6б и т.д.

&nbsp;           r'^10\[пр]$',         # 10п, 10р

&nbsp;           r'^11\[р]$',          # 11р

&nbsp;           r'^\\d\[абв]\\s\*$',

&nbsp;           r'^\\d\[абв].\*класс',

&nbsp;           r'^класс.\*\\d\[абв]'

&nbsp;       ]

&nbsp;       return any(re.match(pattern, text) for pattern in patterns)



&nbsp;   def \_extract\_class\_name(self, text):

&nbsp;       """Извлекает название класса из текста"""

&nbsp;       text = text.lower().strip()

&nbsp;       

&nbsp;       # Удаляем лишние слова

&nbsp;       text = re.sub(r'(класс|смена|урок|расписание|№)', '', text).strip()

&nbsp;       

&nbsp;       # Ищем шаблоны классов

&nbsp;       patterns = \[

&nbsp;           (r'(\\d\[абв])', 1),    # 5а, 6б и т.д.

&nbsp;           (r'(10\[пр])', 1),     # 10п, 10р

&nbsp;           (r'(11\[р])', 1)       # 11р

&nbsp;       ]

&nbsp;       

&nbsp;       for pattern, group in patterns:

&nbsp;           match = re.search(pattern, text)

&nbsp;           if match:

&nbsp;               class\_name = match.group(group).upper()

&nbsp;               logger.debug(f"Извлечен класс '{class\_name}' из текста '{text}'")

&nbsp;               return class\_name

&nbsp;       

&nbsp;       logger.debug(f"Не удалось извлечь класс из текста '{text}'")

&nbsp;       return None



&nbsp;   def \_detect\_day\_from\_position(self, df, row\_idx, col):

&nbsp;       """Определяет день недели по позиции в таблице"""

&nbsp;       # Сначала ищем день недели в левой части таблицы (первые колонки)

&nbsp;       for i in range(max(0, row\_idx-10), min(row\_idx+1, len(df))):

&nbsp;           for j in range(min(5, len(df.iloc\[i]))):

&nbsp;               if pd.notna(df.iloc\[i]\[j]) and isinstance(df.iloc\[i]\[j], str):

&nbsp;                   cell\_value = str(df.iloc\[i]\[j]).lower()

&nbsp;                   day\_map = {

&nbsp;                       'понедельник': 'monday',

&nbsp;                       'вторник': 'tuesday',

&nbsp;                       'среда': 'wednesday',

&nbsp;                       'четверг': 'thursday',

&nbsp;                       'пятница': 'friday',

&nbsp;                       'суббота': 'saturday'

&nbsp;                   }

&nbsp;                   for ru\_day, en\_day in day\_map.items():

&nbsp;                       if ru\_day in cell\_value:

&nbsp;                           logger.debug(f"Найден день '{en\_day}' в ячейке \[{i},{j}]: '{cell\_value}'")

&nbsp;                           return en\_day

&nbsp;       

&nbsp;       # Если не нашли, используем дефолтный

&nbsp;       logger.warning(f"Не удалось определить день для строки {row\_idx}, колонки {col}, используем понедельник")

&nbsp;       return 'monday'



&nbsp;   def import\_schedule\_from\_excel(self, file\_content, shift):

&nbsp;       """Импорт расписания из Excel в базу данных для конкретной смены"""

&nbsp;       try:

&nbsp;           lessons\_data = self.parse\_excel\_schedule(file\_content, shift)

&nbsp;           if not lessons\_data:

&nbsp;               return False, f"Не удалось распарсить Excel файл для {shift} смены"

&nbsp;           

&nbsp;           cursor = self.conn.cursor()

&nbsp;           imported\_count = 0

&nbsp;           error\_count = 0

&nbsp;           

&nbsp;           # Удаляем только те уроки, которые относятся к классам из этой смены

&nbsp;           # Для этого сначала соберем все классы из импортируемых данных

&nbsp;           imported\_classes = set(lesson\['class'] for lesson in lessons\_data)

&nbsp;           

&nbsp;           for class\_name in imported\_classes:

&nbsp;               cursor.execute("DELETE FROM schedule WHERE class = ?", (class\_name,))

&nbsp;               logger.info(f"Удалены старые уроки для класса {class\_name}")

&nbsp;           

&nbsp;           for lesson in lessons\_data:

&nbsp;               try:

&nbsp;                   lesson\_number = int(lesson\['lesson\_number'])

&nbsp;                   class\_name = lesson\['class']

&nbsp;                   day = lesson\['day']

&nbsp;                   

&nbsp;                   cursor.execute(

&nbsp;                       "INSERT OR REPLACE INTO schedule (class, day, lesson\_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?)",

&nbsp;                       (class\_name, day, lesson\_number, lesson\['subject'], lesson\['teacher'], lesson\['room'])

&nbsp;                   )

&nbsp;                   imported\_count += 1

&nbsp;               except Exception as e:

&nbsp;                   logger.error(f"Ошибка импорта урока {lesson}: {e}")

&nbsp;                   error\_count += 1

&nbsp;           

&nbsp;           self.conn.commit()

&nbsp;           

&nbsp;           message = f"Успешно импортировано {imported\_count} уроков для {shift} смены"

&nbsp;           if error\_count > 0:

&nbsp;               message += f", ошибок: {error\_count}"

&nbsp;               

&nbsp;           return True, message

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка импорта из Excel для смены {shift}: {e}")

&nbsp;           return False, f"Ошибка импорта для {shift} смены: {str(e)}"

&nbsp;   

&nbsp;   def handle\_start(self, chat\_id, user):

&nbsp;       user\_data = self.get\_user(user\["id"])

&nbsp;       

&nbsp;       if user\_data:

&nbsp;           text = (

&nbsp;               f"Привет, {self.safe\_message(user.get('first\_name', 'друг'))}!\\n"

&nbsp;               f"Ты уже зарегистрирован в системе.\\n"

&nbsp;               f"Твой класс: {self.safe\_message(user\_data\[2])}"

&nbsp;           )

&nbsp;       else:

&nbsp;           text = (

&nbsp;               f"Привет, {self.safe\_message(user.get('first\_name', 'друг'))}!\\n"

&nbsp;               "Я бот для просмотра расписания школы.\\n\\n"

&nbsp;               "Для начала работы необходимо зарегистрироваться.\\n"

&nbsp;               "Пожалуйста, введите своё ФИО и класс в формате:\\n"

&nbsp;               "<b>Фамилия Имя Отчество, Класс</b>\\n\\n"

&nbsp;               "Например: <i>Иванов Иван Иванович, 10П</i>\\n\\n"

&nbsp;               "<b>Доступные классы:</b>\\n"

&nbsp;               "5-9 классы: А, Б, В\\n"

&nbsp;               "10 класс: П, Р\\n"

&nbsp;               "11 класс: Р"

&nbsp;           )

&nbsp;       

&nbsp;       self.send\_message(chat\_id, text, self.main\_menu\_keyboard() if user\_data else None)

&nbsp;   

&nbsp;   def handle\_help(self, chat\_id, username):

&nbsp;       text = (

&nbsp;           "📚 <b>Школьный бот - помощь</b>\\n\\n"

&nbsp;           "Я помогу тебе узнать расписание уроков.\\n\\n"

&nbsp;           "<b>Основные команды:</b>\\n"

&nbsp;           "• /start - начать работу\\n"

&nbsp;           "• /help - показать эту справку\\n\\n"

&nbsp;           "<b>Возможности:</b>\\n"

&nbsp;           "• <b>Моё расписание</b> - расписание для твоего класса\\n"

&nbsp;           "• <b>Общее расписание</b> - расписание для любого класса\\n"

&nbsp;           "• <b>Звонки</b> - расписание звонков\\n\\n"

&nbsp;           "Для регистрации введи своё ФИО и класс в формате:\\n"

&nbsp;           "<i>Фамилия Имя Отчество, Класс</i>\\n\\n"

&nbsp;           "<b>Доступные классы:</b>\\n"

&nbsp;           "5-9 классы: А, Б, В\\n"

&nbsp;           "10 класс: П, Р\\n"

&nbsp;           "11 класс: Р\\n\\n"

&nbsp;           "🛠 <b>Техническая помощь</b>\\n"

&nbsp;           "Если вы обнаружили ошибку или у вас есть предложения, "

&nbsp;           "напишите разработчику: @r1kuza"

&nbsp;       )

&nbsp;       

&nbsp;       if self.is\_admin(username):

&nbsp;           text += "\\n\\n🔐 <b>Секретная команда для админа:</b>\\n/admin\_panel"

&nbsp;       

&nbsp;       self.send\_message(chat\_id, text)

&nbsp;   

&nbsp;   def handle\_admin\_panel(self, chat\_id, username):

&nbsp;       if not self.is\_admin(username):

&nbsp;           self.log\_security\_event("unauthorized\_admin\_access", chat\_id, f"Username: {username}")

&nbsp;           self.send\_message(chat\_id, "❌ У вас нет доступа к админ-панели")

&nbsp;           return

&nbsp;       

&nbsp;       text = "👨‍💼 <b>Панель администратора</b>\\n\\nВыберите действие:"

&nbsp;       self.send\_message(chat\_id, text, self.admin\_menu\_keyboard())

&nbsp;   

&nbsp;   def show\_classes\_management(self, chat\_id, username):

&nbsp;       self.admin\_states\[username] = {"menu": "classes\_management"}

&nbsp;       self.send\_message(chat\_id, "🏫 Управление классами", self.classes\_management\_keyboard())

&nbsp;   

&nbsp;   def show\_bells\_management(self, chat\_id, username):

&nbsp;       self.admin\_states\[username] = {"menu": "bells\_management"}

&nbsp;       self.send\_message(chat\_id, "🕧 Управление расписанием звонков", self.bells\_management\_keyboard())

&nbsp;   

&nbsp;   def start\_add\_class(self, chat\_id, username):

&nbsp;       self.admin\_states\[username] = {"action": "add\_class\_input"}

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           "Введите название класса для добавления:\\n\\n"

&nbsp;           "Формат: 5А, 10П, 11Р и т.д.\\n"

&nbsp;           "Доступные классы: 5-9 классы (А, Б, В), 10-11 классы (П, Р)",

&nbsp;           self.cancel\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def start\_delete\_class(self, chat\_id, username):

&nbsp;       self.admin\_states\[username] = {"action": "delete\_class\_input"}

&nbsp;       

&nbsp;       classes = self.get\_existing\_classes()

&nbsp;       classes\_text = "Существующие классы:\\n" + "\\n".join(classes) if classes else "❌ Нет зарегистрированных классов"

&nbsp;       

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           f"{classes\_text}\\n\\nВведите название класса для удаления:",

&nbsp;           self.cancel\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def start\_edit\_bell(self, chat\_id, username):

&nbsp;       self.admin\_states\[username] = {"action": "edit\_bell\_number"}

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           "Введите номер урока для изменения (1-7):",

&nbsp;           self.cancel\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def show\_all\_bells(self, chat\_id):

&nbsp;       bells = self.get\_bell\_schedule()

&nbsp;       bells\_text = "🔔 <b>Текущее расписание звонков</b>\\n\\n"

&nbsp;       for bell in bells:

&nbsp;           bells\_text += f"{bell\[0]}. {bell\[1]} - {bell\[2]}\\n"

&nbsp;       self.send\_message(chat\_id, bells\_text)

&nbsp;   

&nbsp;   def handle\_management\_menus(self, chat\_id, username, text):

&nbsp;       if text == "➕ Добавить класс":

&nbsp;           self.start\_add\_class(chat\_id, username)

&nbsp;       elif text == "➖ Удалить класс":

&nbsp;           self.start\_delete\_class(chat\_id, username)

&nbsp;       elif text == "✏️ Изменить звонок":

&nbsp;           self.start\_edit\_bell(chat\_id, username)

&nbsp;       elif text == "👀 Посмотреть все звонки":

&nbsp;           self.show\_all\_bells(chat\_id)

&nbsp;       elif text == "⬅️ Назад в админку":

&nbsp;           self.handle\_admin\_panel(chat\_id, username)

&nbsp;       elif text == "📤 Загрузить Excel":

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               "📤 <b>Загрузка расписания из Excel</b>\\n\\n"

&nbsp;               "Выберите смену для загрузки:",

&nbsp;               self.shift\_selection\_keyboard()

&nbsp;           )

&nbsp;           self.admin\_states\[username] = {"action": "select\_shift"}

&nbsp;   

&nbsp;   def handle\_class\_input(self, chat\_id, username, text):

&nbsp;       if username not in self.admin\_states:

&nbsp;           return

&nbsp;       

&nbsp;       action = self.admin\_states\[username].get("action")

&nbsp;       class\_name = text.strip().upper()

&nbsp;       

&nbsp;       if not self.is\_valid\_class(class\_name):

&nbsp;           self.send\_message(chat\_id, "❌ Неверный формат класса", self.classes\_management\_keyboard())

&nbsp;           del self.admin\_states\[username]

&nbsp;           return

&nbsp;       

&nbsp;       if action == "add\_class\_input":

&nbsp;           if self.add\_class(class\_name):

&nbsp;               self.send\_message(chat\_id, f"✅ Класс {class\_name} доступен для регистрации", self.classes\_management\_keyboard())

&nbsp;           else:

&nbsp;               self.send\_message(chat\_id, f"❌ Неверный формат класса", self.classes\_management\_keyboard())

&nbsp;       elif action == "delete\_class\_input":

&nbsp;           if self.delete\_class(class\_name):

&nbsp;               self.send\_message(chat\_id, f"✅ Класс {class\_name} и все связанные пользователи удалены", self.classes\_management\_keyboard())

&nbsp;           else:

&nbsp;               self.send\_message(chat\_id, f"❌ Класс {class\_name} не найден или в нем нет пользователей", self.classes\_management\_keyboard())

&nbsp;       

&nbsp;       del self.admin\_states\[username]

&nbsp;   

&nbsp;   def handle\_bell\_input(self, chat\_id, username, text):

&nbsp;       if username not in self.admin\_states:

&nbsp;           return

&nbsp;       

&nbsp;       state = self.admin\_states\[username]

&nbsp;       

&nbsp;       if state.get("action") == "edit\_bell\_number":

&nbsp;           try:

&nbsp;               lesson\_number = int(text)

&nbsp;               if 1 <= lesson\_number <= 7:

&nbsp;                   state\["action"] = "edit\_bell\_start"

&nbsp;                   state\["lesson\_number"] = lesson\_number

&nbsp;                   self.send\_message(chat\_id, f"Урок {lesson\_number}. Введите время начала (формат ЧЧ:ММ):", self.cancel\_keyboard())

&nbsp;               else:

&nbsp;                   self.send\_message(chat\_id, "❌ Номер урока должен быть от 1 до 7", self.bells\_management\_keyboard())

&nbsp;                   del self.admin\_states\[username]

&nbsp;           except ValueError:

&nbsp;               self.send\_message(chat\_id, "❌ Введите число от 1 до 7", self.bells\_management\_keyboard())

&nbsp;               del self.admin\_states\[username]

&nbsp;       

&nbsp;       elif state.get("action") == "edit\_bell\_start":

&nbsp;           if self.is\_valid\_time(text):

&nbsp;               state\["action"] = "edit\_bell\_end"

&nbsp;               state\["start\_time"] = text

&nbsp;               self.send\_message(chat\_id, f"Введите время окончания (формат ЧЧ:ММ):", self.cancel\_keyboard())

&nbsp;           else:

&nbsp;               self.send\_message(chat\_id, "❌ Неверный формат времени. Используйте ЧЧ:ММ", self.bells\_management\_keyboard())

&nbsp;               del self.admin\_states\[username]

&nbsp;       

&nbsp;       elif state.get("action") == "edit\_bell\_end":

&nbsp;           if self.is\_valid\_time(text):

&nbsp;               lesson\_number = state\["lesson\_number"]

&nbsp;               start\_time = state\["start\_time"]

&nbsp;               end\_time = text

&nbsp;               

&nbsp;               if self.update\_bell\_schedule(lesson\_number, start\_time, end\_time):

&nbsp;                   self.send\_message(chat\_id, f"✅ Звонок для урока {lesson\_number} обновлен: {start\_time} - {end\_time}", self.bells\_management\_keyboard())

&nbsp;               else:

&nbsp;                   self.send\_message(chat\_id, f"❌ Ошибка обновления звонка", self.bells\_management\_keyboard())

&nbsp;               

&nbsp;               del self.admin\_states\[username]

&nbsp;           else:

&nbsp;               self.send\_message(chat\_id, "❌ Неверный формат времени. Используйте ЧЧ:ММ", self.bells\_management\_keyboard())

&nbsp;               del self.admin\_states\[username]

&nbsp;   

&nbsp;   def handle\_main\_menu(self, chat\_id, user\_id, text, username):

&nbsp;       user\_data = self.get\_user(user\_id)

&nbsp;       

&nbsp;       if text == "📚 Моё расписание":

&nbsp;           if not user\_data:

&nbsp;               self.send\_message(

&nbsp;                   chat\_id,

&nbsp;                   "❌ Вы не зарегистрированы. Пожалуйста, введите своё ФИО и класс для регистрации."

&nbsp;               )

&nbsp;               return

&nbsp;           

&nbsp;           class\_name = user\_data\[2]

&nbsp;           self.user\_states\[user\_id] = {"action": "my\_schedule", "class": class\_name}

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               f"Выберите день недели для расписания {self.safe\_message(class\_name)} класса:",

&nbsp;               self.day\_selection\_inline\_keyboard()

&nbsp;           )

&nbsp;       

&nbsp;       elif text == "🏫 Общее расписание":

&nbsp;           self.user\_states\[user\_id] = {"action": "general\_schedule"}

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               "Выберите класс:",

&nbsp;               self.class\_selection\_keyboard()

&nbsp;           )

&nbsp;       

&nbsp;       elif text == "🔔 Звонки":

&nbsp;           bells = self.get\_bell\_schedule()

&nbsp;           bells\_text = "🔔 <b>Расписание звонков</b>\\n\\n"

&nbsp;           for bell in bells:

&nbsp;               bells\_text += f"{bell\[0]}. {bell\[1]} - {bell\[2]}\\n"

&nbsp;               if bell\[0] == 4:

&nbsp;                   bells\_text += "    ⏰ Перемена 15 минут\\n"

&nbsp;               elif bell\[0] == 5:

&nbsp;                   bells\_text += "    ⏰ Перемена 5 минут\\n"

&nbsp;               elif bell\[0] < 7:

&nbsp;                   bells\_text += "    ⏰ Перемена 10 минут\\n"

&nbsp;           

&nbsp;           bells\_text += "\\n📝 Уроки по 40 минут"

&nbsp;           self.send\_message(chat\_id, bells\_text)

&nbsp;       

&nbsp;       elif text == "ℹ️ Помощь":

&nbsp;           self.handle\_help(chat\_id, username)

&nbsp;       

&nbsp;       elif text == "⬅️ Назад":

&nbsp;           if user\_id in self.user\_states:

&nbsp;               del self.user\_states\[user\_id]

&nbsp;           self.send\_message(chat\_id, "Главное меню", self.main\_menu\_keyboard())

&nbsp;       

&nbsp;       # Обработка выбора класса

&nbsp;       elif self.is\_valid\_class(text):

&nbsp;           self.handle\_class\_selection(chat\_id, user\_id, text)

&nbsp;   

&nbsp;   def handle\_callback\_query(self, update):

&nbsp;       """Обработка inline-кнопок"""

&nbsp;       callback\_query = update.get("callback\_query")

&nbsp;       if not callback\_query:

&nbsp;           return

&nbsp;           

&nbsp;       chat\_id = callback\_query\["message"]\["chat"]\["id"]

&nbsp;       user = callback\_query\["from"]

&nbsp;       user\_id = user\["id"]

&nbsp;       username = user.get("username", "")

&nbsp;       data = callback\_query\["data"]

&nbsp;       

&nbsp;       # Обработка выбора дня недели

&nbsp;       if data.startswith("day\_"):

&nbsp;           day\_code = data\[4:]  # Извлекаем день из callback\_data (day\_monday -> monday)

&nbsp;           day\_map = {

&nbsp;               'monday': 'понедельник',

&nbsp;               'tuesday': 'вторник', 

&nbsp;               'wednesday': 'среда',

&nbsp;               'thursday': 'четверг',

&nbsp;               'friday': 'пятница',

&nbsp;               'saturday': 'суббота'

&nbsp;           }

&nbsp;           day\_text = day\_map.get(day\_code, day\_code)

&nbsp;           self.handle\_day\_selection(chat\_id, user\_id, day\_text)

&nbsp;           

&nbsp;           # Ответим на callback, чтобы убрать "часики" у кнопки

&nbsp;           self.answer\_callback\_query(callback\_query\["id"])

&nbsp;   

&nbsp;   def answer\_callback\_query(self, callback\_query\_id, text=None):

&nbsp;       """Ответ на callback query"""

&nbsp;       url = f"{BASE\_URL}/answerCallbackQuery"

&nbsp;       data = {"callback\_query\_id": callback\_query\_id}

&nbsp;       if text:

&nbsp;           data\["text"] = text

&nbsp;       

&nbsp;       try:

&nbsp;           response = requests.post(url, json=data, timeout=10)

&nbsp;           return response.json()

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка ответа на callback: {e}")

&nbsp;           return None

&nbsp;   

&nbsp;   def handle\_day\_selection(self, chat\_id, user\_id, day\_text):

&nbsp;       """Обработка выбора дня недели"""

&nbsp;       if user\_id not in self.user\_states:

&nbsp;           self.send\_message(chat\_id, "❌ Ошибка: действие не найдено", self.main\_menu\_keyboard())

&nbsp;           return

&nbsp;       

&nbsp;       state = self.user\_states\[user\_id]

&nbsp;       day\_map = {

&nbsp;           'понедельник': 'monday',

&nbsp;           'вторник': 'tuesday',

&nbsp;           'среда': 'wednesday',

&nbsp;           'четверг': 'thursday',

&nbsp;           'пятница': 'friday',

&nbsp;           'суббота': 'saturday'

&nbsp;       }

&nbsp;       

&nbsp;       day\_code = day\_map.get(day\_text.lower())

&nbsp;       if not day\_code:

&nbsp;           self.send\_message(chat\_id, "❌ Неверный день недели", self.main\_menu\_keyboard())

&nbsp;           return

&nbsp;       

&nbsp;       if state.get("action") == "my\_schedule":

&nbsp;           class\_name = state.get("class")

&nbsp;           if not class\_name:

&nbsp;               self.send\_message(chat\_id, "❌ Ошибка: класс не найден", self.main\_menu\_keyboard())

&nbsp;               return

&nbsp;           

&nbsp;           self.show\_schedule(chat\_id, class\_name, day\_code, day\_text)

&nbsp;       

&nbsp;       elif state.get("action") == "general\_schedule":

&nbsp;           class\_name = state.get("selected\_class")

&nbsp;           if not class\_name:

&nbsp;               self.send\_message(chat\_id, "❌ Ошибка: класс не выбран", self.main\_menu\_keyboard())

&nbsp;               return

&nbsp;           

&nbsp;           self.show\_schedule(chat\_id, class\_name, day\_code, day\_text)

&nbsp;   

&nbsp;   def handle\_class\_selection(self, chat\_id, user\_id, class\_name):

&nbsp;       """Обработка выбора класса"""

&nbsp;       if user\_id not in self.user\_states:

&nbsp;           self.send\_message(chat\_id, "❌ Ошибка: действие не найдено", self.main\_menu\_keyboard())

&nbsp;           return

&nbsp;       

&nbsp;       state = self.user\_states\[user\_id]

&nbsp;       

&nbsp;       if state.get("action") == "general\_schedule":

&nbsp;           self.user\_states\[user\_id] = {

&nbsp;               "action": "general\_schedule",

&nbsp;               "selected\_class": class\_name

&nbsp;           }

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               f"Выбран класс: {class\_name}\\nТеперь выберите день недели:",

&nbsp;               self.day\_selection\_inline\_keyboard()

&nbsp;           )

&nbsp;   

&nbsp;   def show\_schedule(self, chat\_id, class\_name, day\_code, day\_name):

&nbsp;       """Показать расписание для класса и дня"""

&nbsp;       schedule = self.get\_schedule(class\_name, day\_code)

&nbsp;       

&nbsp;       if schedule:

&nbsp;           schedule\_text = f"📅 <b>Расписание {self.safe\_message(class\_name)} класса</b>\\n{day\_name}\\n\\n"

&nbsp;           for lesson in schedule:

&nbsp;               schedule\_text += f"{lesson\[0]}. <b>{self.safe\_message(lesson\[1])}</b>"

&nbsp;               if lesson\[2]:

&nbsp;                   schedule\_text += f" ({self.safe\_message(lesson\[2])})"

&nbsp;               if lesson\[3]:

&nbsp;                   schedule\_text += f" - {self.safe\_message(lesson\[3])}"

&nbsp;               schedule\_text += "\\n"

&nbsp;       else:

&nbsp;           schedule\_text = f"❌ Расписание для {self.safe\_message(class\_name)} класса на {day\_name.lower()} не найдено"

&nbsp;       

&nbsp;       self.send\_message(chat\_id, schedule\_text, self.main\_menu\_keyboard())

&nbsp;   

&nbsp;   def handle\_admin\_menu(self, chat\_id, username, text):

&nbsp;       if not self.is\_admin(username):

&nbsp;           self.log\_security\_event("unauthorized\_admin\_action", chat\_id, f"Action: {text}")

&nbsp;           self.send\_message(chat\_id, "❌ У вас нет доступа к этой функции")

&nbsp;           return

&nbsp;       

&nbsp;       if text == "👥 Список пользователей":

&nbsp;           self.show\_users\_list(chat\_id)

&nbsp;       elif text == "❌ Удалить пользователя":

&nbsp;           self.start\_delete\_user(chat\_id, username)

&nbsp;       elif text == "📝 Редактировать расписание":

&nbsp;           self.start\_edit\_schedule(chat\_id, username)

&nbsp;       elif text == "🏫 Управление классами":

&nbsp;           self.show\_classes\_management(chat\_id, username)

&nbsp;       elif text == "🕧 Управление звонками":

&nbsp;           self.show\_bells\_management(chat\_id, username)

&nbsp;       elif text == "📤 Загрузить Excel":

&nbsp;           self.handle\_management\_menus(chat\_id, username, text)

&nbsp;       elif text == "📊 Статистика":

&nbsp;           self.show\_statistics(chat\_id)

&nbsp;       elif text == "⬅️ Назад":

&nbsp;           self.send\_message(chat\_id, "Главное меню", self.main\_menu\_keyboard())

&nbsp;       elif text in \["➕ Добавить класс", "➖ Удалить класс", "⬅️ Назад в админку", 

&nbsp;                     "✏️ Изменить звонок", "👀 Посмотреть все звонки"]:

&nbsp;           self.handle\_management\_menus(chat\_id, username, text)

&nbsp;       elif text in \["1 смена", "2 смена"]:

&nbsp;           self.handle\_shift\_selection(chat\_id, username, text)

&nbsp;   

&nbsp;   def handle\_shift\_selection(self, chat\_id, username, shift\_text):

&nbsp;       """Обработка выбора смены для загрузки Excel"""

&nbsp;       if username not in self.admin\_states:

&nbsp;           return

&nbsp;       

&nbsp;       shift = "1" if shift\_text == "1 смена" else "2"

&nbsp;       self.admin\_states\[username] = {"action": "waiting\_excel", "shift": shift}

&nbsp;       

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           f"📤 <b>Загрузка расписания для {shift\_text}</b>\\n\\n"

&nbsp;           f"Отправьте Excel файл с расписанием для {shift\_text}.\\n"

&nbsp;           f"После загрузки файла расписание для {shift\_text} будет автоматически обновлено.",

&nbsp;           self.cancel\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def show\_users\_list(self, chat\_id):

&nbsp;       users = self.get\_all\_users()

&nbsp;       

&nbsp;       if not users:

&nbsp;           self.send\_message(chat\_id, "❌ Нет зарегистрированных пользователей")

&nbsp;           return

&nbsp;       

&nbsp;       users\_text = "👥 <b>Список пользователей</b>\\n\\n"

&nbsp;       for user in users:

&nbsp;           reg\_date = user\[3].split()\[0] if user\[3] else "неизвестно"

&nbsp;           users\_text += f"👤 {self.safe\_message(user\[1])} - {self.safe\_message(user\[2])} (ID: {user\[0]})\\n"

&nbsp;           users\_text += f"   📅 Зарегистрирован: {reg\_date}\\n\\n"

&nbsp;       

&nbsp;       self.send\_message(chat\_id, users\_text)

&nbsp;   

&nbsp;   def start\_delete\_user(self, chat\_id, username):

&nbsp;       self.admin\_states\[username] = {"action": "delete\_user"}

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           "Введите ID пользователя для удаления:\\n\\n"

&nbsp;           "ID можно узнать через команду '👥 Список пользователей'",

&nbsp;           self.cancel\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def delete\_user\_by\_id(self, chat\_id, admin\_username, user\_id\_str):

&nbsp;       try:

&nbsp;           user\_id = int(user\_id\_str)

&nbsp;           if not self.is\_valid\_user\_id(user\_id):

&nbsp;               self.send\_message(chat\_id, "❌ Неверный формат ID пользователя", self.admin\_menu\_keyboard())

&nbsp;               return

&nbsp;               

&nbsp;           if self.delete\_user(user\_id):

&nbsp;               self.log\_security\_event("user\_deleted", admin\_username, f"Deleted user: {user\_id}")

&nbsp;               self.send\_message(chat\_id, f"✅ Пользователь с ID {user\_id} удален", self.admin\_menu\_keyboard())

&nbsp;           else:

&nbsp;               self.send\_message(chat\_id, f"❌ Пользователь с ID {user\_id} не найден", self.admin\_menu\_keyboard())

&nbsp;       except ValueError:

&nbsp;           self.send\_message(chat\_id, "❌ Неверный формат ID. ID должен быть числом", self.admin\_menu\_keyboard())

&nbsp;       

&nbsp;       if admin\_username in self.admin\_states:

&nbsp;           del self.admin\_states\[admin\_username]

&nbsp;   

&nbsp;   def start\_edit\_schedule(self, chat\_id, username):

&nbsp;       self.admin\_states\[username] = {"action": "edit\_schedule\_class"}

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           "Выберите класс для редактирования расписания:",

&nbsp;           self.class\_selection\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def handle\_schedule\_class\_selection(self, chat\_id, username, class\_name):

&nbsp;       if username not in self.admin\_states:

&nbsp;           return

&nbsp;       

&nbsp;       self.admin\_states\[username] = {

&nbsp;           "action": "edit\_schedule\_day",

&nbsp;           "class": class\_name

&nbsp;       }

&nbsp;       

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           f"Выбран класс: {self.safe\_message(class\_name)}\\nТеперь выберите день недели:",

&nbsp;           self.day\_selection\_inline\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def handle\_schedule\_day\_selection(self, chat\_id, username, day\_name):

&nbsp;       if username not in self.admin\_states:

&nbsp;           return

&nbsp;       

&nbsp;       class\_name = self.admin\_states\[username].get("class")

&nbsp;       if not class\_name:

&nbsp;           self.send\_message(chat\_id, "❌ Ошибка: класс не выбран", self.admin\_menu\_keyboard())

&nbsp;           return

&nbsp;       

&nbsp;       # Преобразуем русское название дня в английский код

&nbsp;       day\_map = {

&nbsp;           "понедельник": "monday",

&nbsp;           "вторник": "tuesday",

&nbsp;           "среда": "wednesday",

&nbsp;           "четверг": "thursday",

&nbsp;           "пятница": "friday",

&nbsp;           "суббота": "saturday"

&nbsp;       }

&nbsp;       

&nbsp;       day\_code = day\_map.get(day\_name.lower(), day\_name.lower())

&nbsp;       

&nbsp;       current\_schedule = self.get\_schedule(class\_name, day\_code)

&nbsp;       

&nbsp;       schedule\_text = ""

&nbsp;       if current\_schedule:

&nbsp;           schedule\_text = "<b>Текущее расписание:</b>\\n"

&nbsp;           for lesson in current\_schedule:

&nbsp;               schedule\_text += f"{lesson\[0]}. {self.safe\_message(lesson\[1])}"

&nbsp;               if lesson\[2]:

&nbsp;                   schedule\_text += f" ({self.safe\_message(lesson\[2])})"

&nbsp;               if lesson\[3]:

&nbsp;                   schedule\_text += f" - {self.safe\_message(lesson\[3])}"

&nbsp;               schedule\_text += "\\n"

&nbsp;           schedule\_text += "\\n"

&nbsp;       

&nbsp;       self.admin\_states\[username] = {

&nbsp;           "action": "edit\_schedule\_input",

&nbsp;           "class": class\_name,

&nbsp;           "day": day\_code

&nbsp;       }

&nbsp;       

&nbsp;       self.send\_message(

&nbsp;           chat\_id,

&nbsp;           f"Редактирование расписания:\\n"

&nbsp;           f"Класс: {self.safe\_message(class\_name)}\\n"

&nbsp;           f"День: {day\_name}\\n\\n"

&nbsp;           f"{schedule\_text}"

&nbsp;           f"Введите новое расписание в формате:\\n\\n"

&nbsp;           f"<code>1. Математика\\n2. Физика (Иванов) - 201\\n3. Химия - 301</code>\\n\\n"

&nbsp;           f"Или отправьте '-' для очистки расписания.",

&nbsp;           self.cancel\_keyboard()

&nbsp;       )

&nbsp;   

&nbsp;   def handle\_schedule\_input(self, chat\_id, username, text):

&nbsp;       if username not in self.admin\_states:

&nbsp;           return

&nbsp;       

&nbsp;       class\_name = self.admin\_states\[username].get("class")

&nbsp;       day\_code = self.admin\_states\[username].get("day")

&nbsp;       

&nbsp;       if not class\_name or not day\_code:

&nbsp;           self.send\_message(chat\_id, "❌ Ошибка: данные не найдены", self.admin\_menu\_keyboard())

&nbsp;           return

&nbsp;       

&nbsp;       if text == '-':

&nbsp;           self.save\_schedule(class\_name, day\_code, \[])

&nbsp;           self.send\_message(chat\_id, "✅ Расписание очищено!", self.admin\_menu\_keyboard())

&nbsp;       else:

&nbsp;           lessons = \[]

&nbsp;           lines = text.split('\\n')

&nbsp;           

&nbsp;           for line in lines:

&nbsp;               line = line.strip()

&nbsp;               if not line or not line\[0].isdigit():

&nbsp;                   continue

&nbsp;                   

&nbsp;               parts = line.split('.', 1)

&nbsp;               if len(parts) < 2:

&nbsp;                   continue

&nbsp;                   

&nbsp;               try:

&nbsp;                   lesson\_num = int(parts\[0].strip())

&nbsp;                   lesson\_info = parts\[1].strip()

&nbsp;                   

&nbsp;                   subject = lesson\_info

&nbsp;                   teacher = ""

&nbsp;                   room = ""

&nbsp;                   

&nbsp;                   if '(' in lesson\_info and ')' in lesson\_info:

&nbsp;                       start = lesson\_info.find('(')

&nbsp;                       end = lesson\_info.find(')')

&nbsp;                       teacher = lesson\_info\[start+1:end]

&nbsp;                       subject = lesson\_info\[:start].strip()

&nbsp;                       lesson\_info = lesson\_info\[end+1:].strip()

&nbsp;                   

&nbsp;                   if ' - ' in lesson\_info:

&nbsp;                       room\_parts = lesson\_info.split(' - ', 1)

&nbsp;                       subject = subject if subject else room\_parts\[0].strip()

&nbsp;                       room = room\_parts\[1].strip()

&nbsp;                   elif lesson\_info and not subject:

&nbsp;                       subject = lesson\_info

&nbsp;                   

&nbsp;                   if subject:

&nbsp;                       lessons.append((lesson\_num, subject, teacher, room))

&nbsp;               except ValueError:

&nbsp;                   continue

&nbsp;           

&nbsp;           self.save\_schedule(class\_name, day\_code, lessons)

&nbsp;           self.send\_message(chat\_id, f"✅ Расписание для {self.safe\_message(class\_name)} класса обновлено!", self.admin\_menu\_keyboard())

&nbsp;       

&nbsp;       if username in self.admin\_states:

&nbsp;           del self.admin\_states\[username]

&nbsp;   

&nbsp;   def show\_statistics(self, chat\_id):

&nbsp;       users = self.get\_all\_users()

&nbsp;       total\_users = len(users)

&nbsp;       

&nbsp;       classes = {}

&nbsp;       for user in users:

&nbsp;           class\_name = user\[2]

&nbsp;           if class\_name in classes:

&nbsp;               classes\[class\_name] += 1

&nbsp;           else:

&nbsp;               classes\[class\_name] = 1

&nbsp;       

&nbsp;       stats\_text = "📊 <b>Статистика бота</b>\\n\\n"

&nbsp;       stats\_text += f"👥 Всего пользователей: {total\_users}\\n\\n"

&nbsp;       

&nbsp;       if classes:

&nbsp;           stats\_text += "<b>Распределение по классам:</b>\\n"

&nbsp;           for class\_name, count in sorted(classes.items()):

&nbsp;               stats\_text += f"• {self.safe\_message(class\_name)}: {count} чел.\\n"

&nbsp;       

&nbsp;       self.send\_message(chat\_id, stats\_text)

&nbsp;   

&nbsp;   def handle\_registration(self, chat\_id, user\_id, text):

&nbsp;       if self.get\_user(user\_id):

&nbsp;           self.send\_message(chat\_id, "Вы уже зарегистрированы!", self.main\_menu\_keyboard())

&nbsp;           return

&nbsp;       

&nbsp;       parts = text.split(',')

&nbsp;       if len(parts) != 2:

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               "❌ Неверный формат. Пожалуйста, введите данные в формате:\\n"

&nbsp;               "<b>Фамилия Имя Отчество, Класс</b>\\n\\n"

&nbsp;               "Например: <i>Иванов Иван Иванович, 10П</i>\\n\\n"

&nbsp;               "<b>Доступные классы:</b>\\n"

&nbsp;               "5-9 классы: А, Б, В\\n"

&nbsp;               "10 класс: П, Р\\n"

&nbsp;               "11 класс: Р"

&nbsp;           )

&nbsp;           return

&nbsp;       

&nbsp;       full\_name = parts\[0].strip()

&nbsp;       class\_name = parts\[1].strip()

&nbsp;       

&nbsp;       if not self.is\_valid\_fullname(full\_name):

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               "❌ Неверный формат ФИО. ФИО должно содержать как минимум 2 слова, "

&nbsp;               "состоять только из букв и каждое слово должно быть от 2 до 20 символов."

&nbsp;           )

&nbsp;           return

&nbsp;       

&nbsp;       if not self.is\_valid\_class(class\_name):

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               "❌ Неверный формат класса.\\n\\n"

&nbsp;               "<b>Доступные классы:</b>\\n"

&nbsp;               "5-9 классы: А, Б, В\\n"

&nbsp;               "10 класс: П, Р\\n"

&nbsp;               "11 класс: Р\\n\\n"

&nbsp;               "Пример: 5А, 10П, 11Р"

&nbsp;           )

&nbsp;           return

&nbsp;       

&nbsp;       class\_name = class\_name.upper()

&nbsp;       if self.create\_user(user\_id, full\_name, class\_name):

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               f"✅ Регистрация прошла успешно!\\nФИО: {self.safe\_message(full\_name)}\\nКласс: {class\_name}",

&nbsp;               self.main\_menu\_keyboard()

&nbsp;           )

&nbsp;       else:

&nbsp;           self.send\_message(

&nbsp;               chat\_id,

&nbsp;               f"❌ Не удалось зарегистрироваться. Возможно, достигнут лимит пользователей в классе {class\_name}.",

&nbsp;               self.main\_menu\_keyboard()

&nbsp;           )

&nbsp;   

&nbsp;   def process\_update(self, update):

&nbsp;       update\_id = update.get("update\_id")

&nbsp;       

&nbsp;       if update\_id in self.processed\_updates:

&nbsp;           logger.info(f"Пропускаем уже обработанное обновление: {update\_id}")

&nbsp;           return

&nbsp;       

&nbsp;       self.processed\_updates.add(update\_id)

&nbsp;       

&nbsp;       if len(self.processed\_updates) > 1000:

&nbsp;           self.processed\_updates = set(list(self.processed\_updates)\[-500:])

&nbsp;       

&nbsp;       try:

&nbsp;           # Обработка callback query (inline кнопки)

&nbsp;           if "callback\_query" in update:

&nbsp;               self.handle\_callback\_query(update)

&nbsp;               return

&nbsp;           

&nbsp;           if "message" in update:

&nbsp;               message = update\["message"]

&nbsp;               chat\_id = message\["chat"]\["id"]

&nbsp;               user = message.get("from", {})

&nbsp;               user\_id = user.get("id")

&nbsp;               username = user.get("username", "")

&nbsp;               

&nbsp;               if user\_id and self.rate\_limiter.is\_limited(user\_id):

&nbsp;                   self.log\_security\_event("rate\_limit\_exceeded", user\_id, f"Username: {username}")

&nbsp;                   self.send\_message(chat\_id, "⚠️ Слишком много запросов. Пожалуйста, подождите.")

&nbsp;                   return

&nbsp;               

&nbsp;               # Обработка документов (Excel файлов)

&nbsp;               if "document" in message and username in self.admin\_states and self.admin\_states\[username].get("action") == "waiting\_excel":

&nbsp;                   document = message\["document"]

&nbsp;                   file\_id = document\["file\_id"]

&nbsp;                   file\_name = document.get("file\_name", "")

&nbsp;                   shift = self.admin\_states\[username].get("shift", "1")

&nbsp;                   

&nbsp;                   if not file\_name.lower().endswith(('.xlsx', '.xls')):

&nbsp;                       self.send\_message(chat\_id, "❌ Пожалуйста, отправьте файл в формате Excel (.xlsx или .xls)")

&nbsp;                       return

&nbsp;                   

&nbsp;                   self.send\_message(chat\_id, f"📥 Начинаю загрузку файла для {shift} смены...")

&nbsp;                   

&nbsp;                   # Получаем информацию о файле

&nbsp;                   file\_info = self.get\_file(file\_id)

&nbsp;                   if not file\_info:

&nbsp;                       self.send\_message(chat\_id, "❌ Ошибка получения информации о файле")

&nbsp;                       return

&nbsp;                   

&nbsp;                   # Скачиваем файл

&nbsp;                   file\_content = self.download\_file(file\_info\["file\_path"])

&nbsp;                   if not file\_content:

&nbsp;                       self.send\_message(chat\_id, "❌ Ошибка загрузки файла")

&nbsp;                       return

&nbsp;                   

&nbsp;                   self.send\_message(chat\_id, f"🔍 Обрабатываю расписание для {shift} смены...")

&nbsp;                   

&nbsp;                   # Импортируем расписание для выбранной смены

&nbsp;                   success, message = self.import\_schedule\_from\_excel(file\_content, shift)

&nbsp;                   

&nbsp;                   if success:

&nbsp;                       self.send\_message(chat\_id, f"✅ {message}", self.admin\_menu\_keyboard())

&nbsp;                   else:

&nbsp;                       self.send\_message(chat\_id, f"❌ {message}", self.admin\_menu\_keyboard())

&nbsp;                   

&nbsp;                   if username in self.admin\_states:

&nbsp;                       del self.admin\_states\[username]

&nbsp;                   return

&nbsp;               

&nbsp;               if "text" in message:

&nbsp;                   text = message\["text"]

&nbsp;                   

&nbsp;                   # Обработка отмены действий

&nbsp;                   if text == "❌ Отменить":

&nbsp;                       if username in self.admin\_states:

&nbsp;                           del self.admin\_states\[username]

&nbsp;                       if user\_id in self.user\_states:

&nbsp;                           del self.user\_states\[user\_id]

&nbsp;                       self.send\_message(chat\_id, "Действие отменено", self.main\_menu\_keyboard())

&nbsp;                       return

&nbsp;                   

&nbsp;                   # Обработка состояний админа

&nbsp;                   if username in self.admin\_states:

&nbsp;                       state = self.admin\_states\[username]

&nbsp;                       

&nbsp;                       if state.get("action") in \["add\_class\_input", "delete\_class\_input"]:

&nbsp;                           self.handle\_class\_input(chat\_id, username, text)

&nbsp;                           return

&nbsp;                       

&nbsp;                       if state.get("action") in \["edit\_bell\_number", "edit\_bell\_start", "edit\_bell\_end"]:

&nbsp;                           self.handle\_bell\_input(chat\_id, username, text)

&nbsp;                           return

&nbsp;                       

&nbsp;                       if state.get("action") == "delete\_user":

&nbsp;                           self.delete\_user\_by\_id(chat\_id, username, text)

&nbsp;                           return

&nbsp;                       elif state.get("action") == "edit\_schedule\_input":

&nbsp;                           self.handle\_schedule\_input(chat\_id, username, text)

&nbsp;                           return

&nbsp;                       elif state.get("action") == "edit\_schedule\_class":

&nbsp;                           self.handle\_schedule\_class\_selection(chat\_id, username, text)

&nbsp;                           return

&nbsp;                       elif state.get("action") == "edit\_schedule\_day":

&nbsp;                           self.handle\_schedule\_day\_selection(chat\_id, username, text)

&nbsp;                           return

&nbsp;                       elif state.get("action") == "select\_shift":

&nbsp;                           self.handle\_shift\_selection(chat\_id, username, text)

&nbsp;                           return

&nbsp;                   

&nbsp;                   # Обработка основных команд

&nbsp;                   if text.startswith("/start"):

&nbsp;                       self.handle\_start(chat\_id, user)

&nbsp;                   elif text.startswith("/help"):

&nbsp;                       self.handle\_help(chat\_id, username)

&nbsp;                   elif text.startswith("/admin\_panel"):

&nbsp;                       self.handle\_admin\_panel(chat\_id, username)

&nbsp;                   elif text in \["📚 Моё расписание", "🏫 Общее расписание", "🔔 Звонки", "ℹ️ Помощь"]:

&nbsp;                       self.handle\_main\_menu(chat\_id, user\_id, text, username)

&nbsp;                   elif text in \["👥 Список пользователей", "❌ Удалить пользователя", "📝 Редактировать расписание", 

&nbsp;                                 "🏫 Управление классами", "🕧 Управление звонками", "📤 Загрузить Excel", "📊 Статистика", "⬅️ Назад",

&nbsp;                                 "➕ Добавить класс", "➖ Удалить класс", "⬅️ Назад в админку", 

&nbsp;                                 "✏️ Изменить звонок", "👀 Посмотреть все звонки", "1 смена", "2 смена"]:

&nbsp;                       self.handle\_admin\_menu(chat\_id, username, text)

&nbsp;                   elif text == "⬅️ Назад" or self.is\_valid\_class(text):

&nbsp;                       # Обработка навигации и выбора

&nbsp;                       self.handle\_main\_menu(chat\_id, user\_id, text, username)

&nbsp;                   else:

&nbsp;                       # Если это не команда, пробуем регистрацию

&nbsp;                       self.handle\_registration(chat\_id, user\_id, text)

&nbsp;       

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка в process\_update: {e}")

&nbsp;           import traceback

&nbsp;           logger.error(traceback.format\_exc())

&nbsp;   

&nbsp;   def run(self):

&nbsp;       logger.info("Бот запущен")

&nbsp;       

&nbsp;       try:

&nbsp;           delete\_url = f"{BASE\_URL}/deleteWebhook"

&nbsp;           response = requests.get(delete\_url, timeout=10)

&nbsp;           if response.json().get("ok"):

&nbsp;               logger.info("Вебхук очищен, используется long polling")

&nbsp;           else:

&nbsp;               logger.warning("Не удалось очистить вебхук")

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Ошибка при очистке вебхука: {e}")

&nbsp;       

&nbsp;       conflict\_count = 0

&nbsp;       max\_conflicts = 5

&nbsp;       

&nbsp;       while True:

&nbsp;           try:

&nbsp;               updates = self.get\_updates()

&nbsp;               

&nbsp;               if updates.get("conflict"):

&nbsp;                   conflict\_count += 1

&nbsp;                   logger.warning(f"Обнаружен конфликт getUpdates ({conflict\_count}/{max\_conflicts})")

&nbsp;                   

&nbsp;                   if conflict\_count >= max\_conflicts:

&nbsp;                       logger.error("Достигнуто максимальное количество конфликтов. Завершаем работу.")

&nbsp;                       break

&nbsp;                   

&nbsp;                   time.sleep(10)

&nbsp;                   continue

&nbsp;               else:

&nbsp;                   conflict\_count = 0

&nbsp;               

&nbsp;               if updates.get("ok") and "result" in updates:

&nbsp;                   for update in updates\["result"]:

&nbsp;                       self.last\_update\_id = update\["update\_id"]

&nbsp;                       self.process\_update(update)

&nbsp;               else:

&nbsp;                   if "description" in updates:

&nbsp;                       error\_desc = updates.get('description', '')

&nbsp;                       if "Conflict" not in error\_desc:

&nbsp;                           logger.error(f"Ошибка Telegram API: {error\_desc}")

&nbsp;               

&nbsp;               time.sleep(0.5)

&nbsp;               

&nbsp;           except Exception as e:

&nbsp;               logger.error(f"Ошибка в основном цикле: {e}")

&nbsp;               time.sleep(5)



if \_\_name\_\_ == "\_\_main\_\_":

&nbsp;   bot = SimpleSchoolBot()

&nbsp;   bot.run()

