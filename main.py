# main.py

import os
import random
import logging
import datetime
import re
import swisseph as swe

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
import pytz

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения")

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", f"/webhook/{BOT_TOKEN}")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("PORT", os.getenv("WEBAPP_PORT", "8080")))

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

geolocator = Nominatim(user_agent="astro-bot")
tf = TimezoneFinder()

CITY_NORMALIZATION = {
    "алма-аты": "Алма-Ата",
    "алма аты": "Алма-Ата",
    "алматы": "Алма-Ата",
    "петербург": "Санкт-Петербург",
    "спб": "Санкт-Петербург"
}

PLANET_NAMES = {
    swe.SUN: "Sun",
    swe.MOON: "Moon",
    swe.MERCURY: "Mercury",
    swe.VENUS: "Venus",
    swe.MARS: "Mars",
    swe.JUPITER: "Jupiter",
    swe.SATURN: "Saturn",
    swe.URANUS: "Uranus",
    swe.NEPTUNE: "Neptune",
    swe.PLUTO: "Pluto"
}


def calculate_planet_positions(year, month, day, hour, minute, latitude, longitude):
    swe.set_ephe_path(".")  # каталог с эфемеридами

    decimal_time = hour + minute / 60.0
    jd = swe.julday(year, month, day, decimal_time)

    results = []
    cuspids, _ = swe.houses(jd, latitude, longitude, b'P')

    def find_house(lon, cuspids):
        for i in range(12):
            start = cuspids[i]
            end = cuspids[(i + 1) % 12]
            if start < end:
                if start <= lon < end:
                    return i + 1
            else:
                if lon >= start or lon < end:
                    return i + 1
        return None

    for pid, name in PLANET_NAMES.items():
        try:
            result, _ = swe.calc_ut(jd, pid)
            lon = result[0]
            sign_index = int(lon // 30)
            sign = SIGNS[sign_index]
            house = find_house(lon, cuspids)

            if house:
                results.append(f"{name} — {sign} в {house} доме")
            else:
                results.append(f"{name} — {sign} (дом не найден)")
        except Exception as e:
            logging.warning(f"Ошибка при расчёте {name}: {e}")
            results.append(f"{name} — данные не найдены.")
    return results


SIGNS = ["Овен", "Телец", "Близнецы", "Рак", "Лев", "Дева", "Весы", "Скорпион", "Стрелец", "Козерог", "Водолей", "Рыбы"]


ZODIAC_SIGNS = ['♈ Aries', '♉ Taurus', '♊ Gemini', '♋ Cancer',
                '♌ Leo', '♍ Virgo', '♎ Libra', '♏ Scorpio',
                '♐ Sagittarius', '♑ Capricorn', '♒ Aquarius', '♓ Pisces']

class BirthData(StatesGroup):
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_place = State()

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    greetings = [
        "Привет! Добро пожаловать туда, где небеса шепчут свои тайны. Начнём с даты рождения: ДД.ММ.ГГГГ ✨",
        "Ты на пороге личного космоса. Введи дату рождения в формате ДД.ММ.ГГГГ 🌌",
        "Именно здесь ты узнаешь, кто ты по звёздам. Введи дату рождения: ДД.ММ.ГГГГ 🗓️"
    ]
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("Сбросить 🌑"))
    await message.reply(random.choice(greetings), parse_mode='HTML', reply_markup=kb)
    await BirthData.waiting_for_date.set()

@dp.message_handler(lambda message: message.text == "Сбросить 🌑", state="*")
async def handle_reset_button(message: types.Message, state: FSMContext):
    await reset_state(message, state)

@dp.message_handler(state=BirthData.waiting_for_date)
async def process_date(message: types.Message, state: FSMContext):
    try:
        match = re.match(r"^\d{2}\.\d{2}\.\d{4}$", message.text.strip())
        if not match:
            await message.reply("Формат даты должен быть ДД.ММ.ГГГГ. Например: 25.06.1992")
            return
        await state.update_data(date=message.text.strip())
        await BirthData.waiting_for_time.set()
        await message.reply("Теперь введи время рождения в формате ЧЧ:ММ (например, 19:45) 🕰")
    except Exception as e:
        logging.exception("Ошибка при обработке даты:")
        await message.reply(f"Ошибка: {e}")

@dp.message_handler(state=BirthData.waiting_for_time)
async def process_time(message: types.Message, state: FSMContext):
    try:
        time_raw = message.text.strip().replace(".", ":")
        match = re.match(r"^\d{1,2}:\d{2}$", time_raw)
        if not match:
            await message.reply("Формат времени должен быть ЧЧ:ММ. Например: 19:45")
            return
        await state.update_data(time=time_raw)
        await BirthData.waiting_for_place.set()
        await message.reply("И наконец, введи город рождения 🌍")
    except Exception as e:
        logging.exception("Ошибка при обработке времени:")
        await message.reply(f"Ошибка: {e}")

@dp.message_handler(state=BirthData.waiting_for_place)
async def process_place(message: types.Message, state: FSMContext):
    try:
        place = message.text.strip().lower()
        place_normalized = CITY_NORMALIZATION.get(place, place.title())
        location = geolocator.geocode(place_normalized)
        if not location:
            await message.reply("Не удалось определить координаты. Попробуй другой город.")
            return

        user_data = await state.get_data()
        if "date" not in user_data or "time" not in user_data:
            await message.reply("Кажется, я потерял дату или время 😢 Введи /start заново.")
            await state.finish()
            return

        await message.reply("🌟 Отлично! Все данные получены. Сейчас я построю твою натальную карту... 🔭")

        date_parts = user_data["date"].split(".")
        time_parts = user_data["time"].split(":")
        birth_dt = datetime.datetime(
            int(date_parts[2]), int(date_parts[1]), int(date_parts[0]),
            int(time_parts[0]), int(time_parts[1])
        )

        timezone_str = tf.timezone_at(lng=location.longitude, lat=location.latitude)
        if not timezone_str:
            await message.reply("Не удалось определить часовой пояс.")
            return
        tz = pytz.timezone(timezone_str)
        birth_dt_localized = tz.localize(birth_dt)

        # Переводим в UTC
        birth_dt_utc = birth_dt_localized.astimezone(pytz.utc)

        # Установка топоцентрических координат
        swe.set_topo(location.longitude, location.latitude, 0)

        # Юлианская дата по UTC-времени
        julday = swe.julday(
            birth_dt_utc.year,
            birth_dt_utc.month,
            birth_dt_utc.day,
            birth_dt_utc.hour + birth_dt_utc.minute / 60
    )

        # Система домов: Placidus или Equal по широте
        house_system = b'P'
        if abs(location.latitude) > 60:
            house_system = b'E'

        cuspids, ascmc = swe.houses(julday, location.latitude, location.longitude, house_system)

        def find_house(lon, cuspids):
            for i in range(12):
                start = cuspids[i]
                end = cuspids[(i + 1) % 12]
                if start < end:
                    if start <= lon < end:
                        return i + 1
                else:
                    if lon >= start or lon < end:
                        return i + 1
            return 12

        planet_data = []
        for planet_id in PLANET_NAMES:
            try:
                result, _ = swe.calc_ut(julday, planet_id)
                lon = result[0]
                sign_index = int(lon // 30)
                sign = ZODIAC_SIGNS[sign_index]
                house = find_house(lon, cuspids)

                if house:
                    planet_data.append(f"{PLANET_NAMES[planet_id]} — {sign} в доме {house}")
                else:
                    planet_data.append(f"{PLANET_NAMES[planet_id]} — {sign} (дом не найден)")
            except Exception as e:
                logging.warning(f"Ошибка при расчёте {PLANET_NAMES[planet_id]}: {e}")
                planet_data.append(f"{PLANET_NAMES[planet_id]} — данные не найдены.")

        # Добавим ASC и DSC
        asc = ascmc[0]
        dsc = (asc + 180.0) % 360
        asc_sign = ZODIAC_SIGNS[int(asc // 30)]
        dsc_sign = ZODIAC_SIGNS[int(dsc // 30)]
        planet_data.append(f"Ascendant (ASC) — {asc_sign} ({asc:.2f}°)")
        planet_data.append(f"Descendant (DSC) — {dsc_sign} ({dsc:.2f}°)")
        await message.reply("🪐 Вот базовые позиции планет на момент твоего рождения:\n\n" + "\n".join(planet_data))
        await state.finish()

    except Exception as e:
        logging.exception("Ошибка при расчёте карты:")
        await message.reply(f"Что-то пошло не так 😕 Ошибка: {e}")
        await state.finish()

@dp.message_handler(commands=['reset'])
async def cmd_reset(message: types.Message, state: FSMContext):
    await reset_state(message, state)

@dp.message_handler(commands=['ping'])
async def cmd_ping(message: types.Message):
    await message.reply("✅ Я на связи!")

async def reset_state(message: types.Message, state: FSMContext):
    await state.finish()
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("Сбросить 🌑"))
    await message.reply("🔄 Всё сброшено. Начнём заново!\nВведи дату рождения в формате ДД.ММ.ГГГГ ✨", reply_markup=kb)
    await BirthData.waiting_for_date.set()

@dp.message_handler()
async def fallback_handler(message: types.Message):
    await message.reply("Я пока не знаю, что с этим делать 🤔 Попробуй /start.")

async def on_startup(dispatcher):
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
        logging.info("Webhook установлен: %s", WEBHOOK_URL)

async def on_shutdown(dispatcher):
    if WEBHOOK_URL:
        await bot.delete_webhook()
        logging.info("Webhook удалён")
    await dispatcher.storage.close()
    await dispatcher.storage.wait_closed()

if __name__ == '__main__':
    if WEBHOOK_URL:
        logging.info("Бот запускается в webhook-режиме на %s:%s", WEBAPP_HOST, WEBAPP_PORT)
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            skip_updates=True,
            host=WEBAPP_HOST,
            port=WEBAPP_PORT,
        )
    else:
        logging.info("WEBHOOK_HOST не задан, запускаюсь в polling-режиме")
        executor.start_polling(dp, skip_updates=True, on_shutdown=on_shutdown)
