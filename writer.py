import asyncio
import random
from aiogram import Bot

# Токен вашего бота
TOKEN = 'my_token'

# ID вашего канала
CHANNEL_ID = '@SDPhw'

bot = Bot(TOKEN)

async def send_random_message():
    messages = ["Привет!", "Как дела?", "Это случайное сообщение.", "Telegram Bot на связи!"]
    message = random.choice(messages)
    await bot.send_message(chat_id=CHANNEL_ID, text=message)

async def main():
    while True:
        await send_random_message()
        await asyncio.sleep(random.random()*4)  # Пауза 3 секунды

asyncio.run(main())
