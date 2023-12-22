from telethon import TelegramClient, events
import pandas as pd
from datetime import datetime
import os
import asyncio
import logging
from pathlib import Path

# Глобальные настройки
api_id = my_id
api_hash = 'my_hash'
channel = 'SDPhw'
time_interval = 5  # Интервал в секундах

client = TelegramClient('anon', api_id, api_hash)

# Папка для сохранения файлов Parquet
parquet_folder = "parquet_data"
path_to_data = Path(parquet_folder)
if not os.path.exists(parquet_folder):
    os.makedirs(parquet_folder)

async def save_messages_to_parquet(content, channel_name, verbose=False):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    data = {
        "timestamp": content.date,  # Используем атрибут 'date' для временной метки
        "source": channel_name,
        "text": content.text,
        "has_media": content.media is not None,
    }
    if verbose:
        logging.info(f"Ready to write message: {data}\n")

    df = pd.DataFrame([data])
    path_to_file = path_to_data / f"{channel_name}_{str(timestamp)}.gzip"
    df.to_parquet(path_to_file, compression="gzip")


# Обработчик новых сообщений
@client.on(events.NewMessage(chats=channel))
async def my_event_handler(event):
    message_data = {
        "time": event.message.date.astimezone(),  # Установка времени в локальный часовой пояс
        "source": channel,
        "text": event.message.message,
        "has_media": event.message.media is not None
    }
    # Создаем DataFrame и определяем типы данных
    df = pd.DataFrame([message_data])
    df['time'] = pd.to_datetime(df['time'], utc=True)  # Преобразование в pandas datetime с UTC
    # Остальные типы данных остаются без изменений
    await save_messages_to_parquet(event.message, channel, verbose=True)

with client:
    client.loop.run_until_complete(asyncio.gather(
        client.run_until_disconnected()
    ))
