# main.py

import os
import random
import logging
import datetime
import re
import json
import swisseph as swe

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
import pytz
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения")

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", f"/webhook/{BOT_TOKEN}")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("PORT", os.getenv("WEBAPP_PORT", "8080")))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "profiles")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

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

PERSONAL_PLANETS = {"Sun", "Moon", "Mercury", "Venus", "Mars"}
PRIORITY_POINTS = ["Sun", "Moon", "Ascendant", "Mercury", "Venus", "Mars"]

SIGN_MODALITIES = {
    "Овен": "Кардинальный",
    "Рак": "Кардинальный",
    "Весы": "Кардинальный",
    "Козерог": "Кардинальный",
    "Телец": "Фиксированный",
    "Лев": "Фиксированный",
    "Скорпион": "Фиксированный",
    "Водолей": "Фиксированный",
    "Близнецы": "Мутабельный",
    "Дева": "Мутабельный",
    "Стрелец": "Мутабельный",
    "Рыбы": "Мутабельный",
}

SIGN_ELEMENTS = {
    "Овен": "Огонь",
    "Лев": "Огонь",
    "Стрелец": "Огонь",
    "Телец": "Земля",
    "Дева": "Земля",
    "Козерог": "Земля",
    "Близнецы": "Воздух",
    "Весы": "Воздух",
    "Водолей": "Воздух",
    "Рак": "Вода",
    "Скорпион": "Вода",
    "Рыбы": "Вода",
}

MODALITY_TRAITS = {
    "Кардинальный": "вы запускаете процессы, любите движение и чувствуете себя увереннее, когда можете влиять на ход событий.",
    "Фиксированный": "в вас много устойчивости, верности своим решениям и умения удерживать курс даже тогда, когда вокруг всё меняется.",
    "Мутабельный": "вы гибко подстраиваетесь к обстоятельствам, быстро считываете настроение среды и умеете находить нестандартные решения.",
}

ELEMENT_TRAITS = {
    "Огонь": "вас ведут энергия, смелость, вдохновение и желание жить ярко.",
    "Земля": "для вас важны опора, практичность, надёжность и ощутимый результат.",
    "Воздух": "вы мыслите через идеи, смыслы, общение и интеллектуальное движение.",
    "Вода": "ваша сила в чувствительности, интуиции, глубине переживаний и эмоциональной связи.",
}

ARCHETYPE_OPENERS = {
    ("Кардинальный", "Огонь"): "Архетип лидера-инициатора. Вы загораетесь идеей быстро и умеете увлекать ею других.",
    ("Кардинальный", "Земля"): "Архетип создателя структуры. Вы умеете не только начать, но и придать идее форму.",
    ("Кардинальный", "Воздух"): "Архетип вдохновителя идей. Вы запускаете движение через слово, мысль и контакт с людьми.",
    ("Кардинальный", "Вода"): "Архетип эмоционального проводника. Вы начинаете новое, опираясь на тонкое чувство момента.",
    ("Фиксированный", "Огонь"): "Архетип внутреннего пламени. В вас много воли, достоинства и способности держать курс.",
    ("Фиксированный", "Земля"): "Архетип надёжной опоры. Вы умеете строить долгоиграющие результаты и не распыляться.",
    ("Фиксированный", "Воздух"): "Архетип убеждённого мыслителя. Ваши идеи отличаются стойкостью, принципами и верностью выбранной позиции.",
    ("Фиксированный", "Вода"): "Архетип глубины и верности. Вы проживаете всё интенсивно и редко относитесь к чему-то поверхностно.",
    ("Мутабельный", "Огонь"): "Архетип живого импульса. Вам важно движение, вдохновение и свобода менять маршрут по ходу пути.",
    ("Мутабельный", "Земля"): "Архетип мастера адаптации. Вы умеете быть полезной, точной и гибкой одновременно.",
    ("Мутабельный", "Воздух"): "Архетип коммуникатора и исследователя. Вы быстро улавливаете новые идеи и легко переходите между разными контекстами.",
    ("Мутабельный", "Вода"): "Архетип тонкого эмпата. Вы хорошо чувствуете оттенки настроений и умеете мягко подстраиваться под поток жизни.",
}

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_dominant_categories(counts):
    if not counts:
        return []
    max_count = max(counts.values())
    return [name for name, count in counts.items() if count == max_count and count > 0]


def combine_labels(labels):
    return "-".join(labels)


def build_category_counts(signs):
    modality_counts = {"Кардинальный": 0, "Фиксированный": 0, "Мутабельный": 0}
    element_counts = {"Огонь": 0, "Земля": 0, "Воздух": 0, "Вода": 0}

    for sign in signs:
        modality = SIGN_MODALITIES.get(sign)
        element = SIGN_ELEMENTS.get(sign)
        if modality:
            modality_counts[modality] += 1
        if element:
            element_counts[element] += 1

    return modality_counts, element_counts


def resolve_dominant_category(counts, personal_signs, priority_signs, category_map):
    leaders = get_dominant_categories(counts)
    if len(leaders) == 1:
        return leaders[0], leaders, "overall"

    personal_counts = {name: 0 for name in counts}
    for sign in personal_signs:
        category = category_map.get(sign)
        if category:
            personal_counts[category] += 1

    personal_leaders = get_dominant_categories(personal_counts)
    filtered_personal = [name for name in personal_leaders if name in leaders]
    if len(filtered_personal) == 1:
        return filtered_personal[0], leaders, "personal"

    for point_name in PRIORITY_POINTS:
        sign = priority_signs.get(point_name)
        category = category_map.get(sign) if sign else None
        if category in leaders:
            return category, leaders, f"priority:{point_name}"

    return leaders[0], leaders, "fallback"


def analyze_archetype(all_signs, personal_signs, priority_signs):
    modality_counts, element_counts = build_category_counts(all_signs)
    dominant_modality, modality_leaders, modality_source = resolve_dominant_category(
        modality_counts, personal_signs, priority_signs, SIGN_MODALITIES
    )
    dominant_element, element_leaders, element_source = resolve_dominant_category(
        element_counts, personal_signs, priority_signs, SIGN_ELEMENTS
    )

    archetype_name = f"{dominant_modality} {dominant_element}"

    return {
        "modality_counts": modality_counts,
        "element_counts": element_counts,
        "dominant_modality": dominant_modality,
        "dominant_element": dominant_element,
        "modality_leaders": modality_leaders,
        "element_leaders": element_leaders,
        "modality_source": modality_source,
        "element_source": element_source,
        "personal_signs": personal_signs,
        "archetype_name": archetype_name,
    }


def build_archetype_report(all_signs, personal_signs, priority_signs):
    archetype_data = analyze_archetype(all_signs, personal_signs, priority_signs)
    dominant_modality = archetype_data["dominant_modality"]
    dominant_element = archetype_data["dominant_element"]
    archetype_name = archetype_data["archetype_name"]

    parts = [
        f"✨ Твой ведущий архетип: <b>{archetype_name}</b>."
    ]

    opener = ARCHETYPE_OPENERS.get((dominant_modality, dominant_element))
    if opener:
        parts.append(opener)

    modality_text = MODALITY_TRAITS[dominant_modality]
    element_text = ELEMENT_TRAITS[dominant_element]
    parts.append(f"По крестам это значит, что {modality_text}")
    parts.append(f"По стихиям карта показывает, что {element_text}")

    if archetype_data["modality_source"] != "overall":
        parts.append("По крестам в карте есть близкие по силе акценты, поэтому окончательный ведущий тип мы уточнили по личным планетам и приоритетным точкам.")
    if archetype_data["element_source"] != "overall":
        parts.append("По стихиям в карте есть конкурирующие акценты, поэтому ведущую стихию мы определили через личные планеты и ключевые точки проявления.")

    parts.append(
        "Этот архетип мы определили по тому, в каких знаках находится большинство планет твоей натальной карты."
    )

    return "\n\n".join(parts), archetype_data


def get_google_worksheet():
    if not GOOGLE_SHEET_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
        return None

    try:
        credentials_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        credentials = Credentials.from_service_account_info(credentials_info, scopes=GOOGLE_SCOPES)
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        try:
            worksheet = spreadsheet.worksheet(GOOGLE_WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=GOOGLE_WORKSHEET_NAME, rows=1000, cols=30)

        if not worksheet.cell(1, 1).value:
            worksheet.update("A1:R1", [[
                "saved_at",
                "telegram_user_id",
                "username",
                "first_name",
                "birth_date",
                "birth_time",
                "birth_city",
                "latitude",
                "longitude",
                "timezone",
                "sun_sign",
                "moon_sign",
                "asc_sign",
                "dominant_modalities",
                "dominant_elements",
                "archetype_name",
                "planet_signs_json",
                "planet_houses_json",
            ]])
        return worksheet
    except Exception:
        logging.exception("Не удалось подключиться к Google Sheets")
        return None


def save_profile_to_google_sheets(message, profile_data):
    worksheet = get_google_worksheet()
    if not worksheet:
        return

    try:
        worksheet.append_row([
            datetime.datetime.utcnow().isoformat(),
            str(message.from_user.id),
            message.from_user.username or "",
            message.from_user.first_name or "",
            profile_data["birth_date"],
            profile_data["birth_time"],
            profile_data["birth_city"],
            str(profile_data["latitude"]),
            str(profile_data["longitude"]),
            profile_data["timezone"],
            profile_data["sun_sign"],
            profile_data["moon_sign"],
            profile_data["asc_sign"],
            ", ".join(profile_data["dominant_modalities"]),
            ", ".join(profile_data["dominant_elements"]),
            profile_data["archetype_name"],
            json.dumps(profile_data["planet_signs"], ensure_ascii=False),
            json.dumps(profile_data["planet_houses"], ensure_ascii=False),
        ])
    except Exception:
        logging.exception("Не удалось сохранить профиль в Google Sheets")


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

        planet_signs = []
        personal_signs = []
        priority_signs = {}
        planet_houses = {}
        sun_sign_ru = ""
        moon_sign_ru = ""
        for planet_id in PLANET_NAMES:
            try:
                result, _ = swe.calc_ut(julday, planet_id)
                lon = result[0]
                sign_index = int(lon // 30)
                sign_ru = SIGNS[sign_index]
                house = find_house(lon, cuspids)
                planet_signs.append(sign_ru)
                point_name = PLANET_NAMES[planet_id]
                planet_houses[point_name] = house
                priority_signs[point_name] = sign_ru

                if point_name in PERSONAL_PLANETS:
                    personal_signs.append(sign_ru)

                if planet_id == swe.SUN:
                    sun_sign_ru = sign_ru
                if planet_id == swe.MOON:
                    moon_sign_ru = sign_ru
            except Exception as e:
                logging.warning(f"Ошибка при расчёте {PLANET_NAMES[planet_id]}: {e}")

        # Добавим ASC и DSC
        asc = ascmc[0]
        asc_sign_ru = SIGNS[int(asc // 30)]
        priority_signs["Ascendant"] = asc_sign_ru

        archetype_report, archetype_data = build_archetype_report(planet_signs, personal_signs, priority_signs)
        save_profile_to_google_sheets(message, {
            "birth_date": user_data["date"],
            "birth_time": user_data["time"],
            "birth_city": place_normalized,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "timezone": timezone_str,
            "sun_sign": sun_sign_ru,
            "moon_sign": moon_sign_ru,
            "asc_sign": asc_sign_ru,
            "dominant_modalities": [archetype_data["dominant_modality"]],
            "dominant_elements": [archetype_data["dominant_element"]],
            "archetype_name": archetype_data["archetype_name"],
            "planet_signs": planet_signs,
            "planet_houses": planet_houses,
        })
        await message.reply(archetype_report, parse_mode="HTML")
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
