# main.py

import os
import random
import logging
import datetime
import re
import json
from pathlib import Path
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
from aiohttp import web

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения")

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", f"/webhook/{BOT_TOKEN}")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("PORT", os.getenv("WEBAPP_PORT", "8080")))
BASE_DIR = Path(__file__).resolve().parent
CARD_OF_DAY_PATH = "/miniapp/card-of-day"
MINI_APP_URL = os.getenv("MINI_APP_URL") or (f"{WEBHOOK_HOST}{CARD_OF_DAY_PATH}" if WEBHOOK_HOST else None)
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_WORKSHEET_NAME = os.getenv("GOOGLE_WORKSHEET_NAME", "profiles")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
SUPPORT_WORKSHEET_NAME = os.getenv("GOOGLE_SUPPORT_WORKSHEET_NAME", "support_requests")
DEV_TELEGRAM_IDS = {
    int(value.strip())
    for value in os.getenv("DEV_TELEGRAM_IDS", "").split(",")
    if value.strip().isdigit()
}

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

ELEMENT_DATIVE = {
    "Огонь": "огню",
    "Земля": "земле",
    "Воздух": "воздуху",
    "Вода": "воде",
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

ARCHETYPE_DATABASE = {
    ("Кардинальный", "Огонь"): {
        "name": "ПЕРВОПРОХОДЕЦ",
        "description": "Это архетип человека, который приходит в жизнь с импульсом начинать. В вас много внутреннего огня, прямоты и ощущения, что ждать подходящего момента не обязательно — его можно создать самой. Вы быстро загораетесь, чувствуете вкус к новому и часто первой решаетесь на то, на что другим нужно больше времени. Рядом с вами ощущается движение, смелость и живая энергия. Вы пробуждаете в людях желание действовать, рисковать, пробовать и верить в возможность нового сценария.",
        "extra_elements": {
            "Воздух": "Идейный лидер. Ваш огонь проявляется не только через силу воли, но и через яркий ум, острое слово и способность зажигать других своими идеями.",
            "Земля": "Строитель основы. Вы умеете не только стартовать, но и быстро превращать вдохновение в реальный результат, форму и опору.",
            "Вода": "Эмоциональный катализатор. Ваша энергия задевает людей глубже, чем кажется на первый взгляд; вы умеете вдохновлять сердцем, а не только напором.",
        },
        "extra_modalities": {
            "Фиксированный": "Сила выдержки. Вы умеете не только вспыхивать, но и долго держать внутренний огонь, не теряя веры в своё направление.",
            "Мутабельный": "Живой экспериментатор. Вы быстро понимаете, когда пора сменить маршрут, и умеете начинать заново без чувства поражения.",
        },
    },
    ("Фиксированный", "Огонь"): {
        "name": "ИКОНА",
        "description": "Это стабильное, мощное горение, которое невозможно игнорировать. Вы — центр притяжения, обладающий врожденным авторитетом и способностью удерживать внимание аудитории годами. Ваша задача не бежать за трендами, а самой становиться эталоном и транслировать личную харизму. Вы светите ровно и жарко, согревая тех, кто идет на ваш свет.",
        "extra_elements": {
            "Воздух": "Публичный спикер. Ваша харизма обретает голос, позволяя вам мастерски управлять мнением масс через смыслы.",
            "Земля": "Статусный эксперт. Ваше влияние материально; вы создаете продукты, которые подчеркивают престиж и твердое качество.",
            "Вода": "Драматичный творец. Ваше сияние наполнено глубоким психологизмом, что делает ваш образ мистическим и притягательным.",
        },
        "extra_modalities": {
            "Кардинальный": "Агрессивный захват рынка. Вы не просто светите, вы активно расширяете свое влияние, захватывая новые территории своим авторитетом.",
            "Мутабельный": "Гибкий лидер. Вы сохраняете свой стержень, но виртуозно адаптируете свой стиль под запросы времени и аудитории.",
        },
    },
    ("Мутабельный", "Огонь"): {
        "name": "ВДОХНОВИТЕЛЬ",
        "description": "Это архетип живого импульса, который не любит застывать в одной форме. Вы вдохновляетесь идеями, дорогой, людьми, движением и чувством свободы. В вас много внутреннего огня, но он проявляется не как давление, а как заразительная искра, желание расширяться и открывать новые горизонты. Вы умеете оживлять пространство, приносить надежду и напоминать другим, что жизнь больше привычных рамок. Рядом с вами чувствуется воздух свободы, азарт поиска и вкус к переменам.",
        "extra_elements": {
            "Воздух": "Генератор идей. Ваше вдохновение быстро превращается в слова, смыслы и новые перспективы, которые увлекают окружающих.",
            "Земля": "Практичный исследователь. Вы умеете не только мечтать о новых горизонтах, но и находить для них реальную, жизнеспособную форму.",
            "Вода": "Тонкий вдохновитель. Ваша энергия затрагивает не только ум, но и чувства, поэтому вы способны пробуждать в людях глубокий отклик.",
        },
        "extra_modalities": {
            "Кардинальный": "Запускает движение. Вы не только ищете новое, но и умеете первой делать шаг, задавая темп для других.",
            "Фиксированный": "Верность внутреннему огню. Даже меняя маршрут, вы сохраняете глубокую преданность тому, что действительно вас зажигает.",
        },
    },
    ("Кардинальный", "Земля"): {
        "name": "СТРАТЕГ",
        "description": "Это архетип человека, который умеет собирать жизнь в устойчивую форму. В вас есть глубокая потребность в опоре, ясности и ощущении, что всё не хаотично, а имеет структуру и направление. Вы спокойно чувствуете себя там, где можно выстроить систему, увидеть закономерность и шаг за шагом приблизиться к важной цели. Вы не просто мечтаете о результате — вы умеете делать так, чтобы он стал реальностью. Рядом с вами часто ощущается надёжность, взрослая собранность и чувство, что на вас можно опереться.",
        "extra_elements": {
            "Воздух": "Системный мыслитель. Ваша практичность соединяется с интеллектуальной тонкостью, и вы умеете не только строить, но и понимать, почему именно так всё работает.",
            "Огонь": "Амбициозный двигатель. В вашей устойчивости появляется больше напора, смелости и готовности брать на себя ведущую роль.",
            "Вода": "Чуткий организатор. Вы выстраиваете порядок, не отрываясь от чувств людей, и умеете создавать не только систему, но и пространство, в котором безопасно жить.",
        },
        "extra_modalities": {
            "Фиксированный": "Опора надолго. Вы не просто задаёте направление, а умеете удерживать его, не сдаваясь перед внешними трудностями.",
            "Мутабельный": "Гибкая стратегия. Вы сохраняете цель, но не цепляетесь за один-единственный путь, если реальность подсказывает более точное решение.",
        },
    },
    ("Фиксированный", "Земля"): {
        "name": "ХРАНИТЕЛЬ",
        "description": "Это архетип устойчивости, верности и внутреннего спокойствия. В вас есть редкая способность сохранять ценное, не разрушаясь под давлением времени и обстоятельств. Вы не склонны к лишней суете и чаще всего раскрываетесь через основательность, верность своим чувствам, привычкам и тому, что по-настоящему имеет значение. Вы умеете быть опорой, удерживать пространство и создавать вокруг себя ощущение безопасности. Рядом с вами люди часто чувствуют, что жизнь может быть стабильной, тёплой и надёжной.",
        "extra_elements": {
            "Огонь": "Тёплая сила. Ваша устойчивость соединяется с внутренним жаром и делает вас человеком, который не только удерживает, но и вдохновляет.",
            "Воздух": "Осознанная опора. Вы умеете сохранять стабильность не вслепую, а через понимание, смысл и ясную внутреннюю позицию.",
            "Вода": "Глубокая надёжность. За вашей внешней собранностью чувствуется эмоциональная верность и способность очень глубоко привязываться.",
        },
        "extra_modalities": {
            "Кардинальный": "Создатель основы. Вы умеете не только сохранять, но и активно строить то, что со временем станет настоящей опорой.",
            "Мутабельный": "Гибкая устойчивость. Даже сохраняя стержень, вы умеете мягко подстраиваться под перемены и не ломаться под их давлением.",
        },
    },
    ("Мутабельный", "Земля"): {
        "name": "МЕТОДОЛОГ",
        "description": "Это архетип тонкой настройки мира. Вы замечаете детали, улавливаете несовершенства и почти интуитивно понимаете, что можно улучшить, поправить, выстроить точнее. В вас есть талант делать сложное понятнее, хаотичное — чище, а сырое — зрелее. Вы можете быть человеком, который приносит не шумную силу, а реальную пользу. Ваша энергия проявляется в точности, внимательности и умении видеть, как именно сделать лучше. Рядом с вами часто становится спокойнее, яснее и функциональнее.",
        "extra_elements": {
            "Воздух": "Архитектор смыслов. Вы умеете тонко работать с информацией, словами и логикой, делая знания доступными и стройными.",
            "Огонь": "Энергия улучшения. Вы не просто замечаете, что можно доработать, но и чувствуете драйв быстро внедрять изменения.",
            "Вода": "Мягкая точность. Ваша внимательность проявляется через заботу, эмпатию и желание исцелять, а не только исправлять.",
        },
        "extra_modalities": {
            "Кардинальный": "Инициатор порядка. Вы не ждёте, пока кто-то займётся хаосом, а первой включаетесь в наведение ясности.",
            "Фиксированный": "Хранитель качества. Вы умеете удерживать высокий стандарт и не соглашаетесь на поверхностный результат.",
        },
    },
    ("Кардинальный", "Воздух"): {
        "name": "ЭСТЕТ",
        "description": "Это архетип человека, который чувствует красоту отношений, идей и тонких настроек между людьми. В вас много вкуса, интеллекта и стремления к гармонии. Вы умеете создавать атмосферу, находить нужный тон, соединять несовместимое и выстраивать пространство, где важны уважение, стиль и взаимность. Вы редко идёте напролом — ваша сила в дипломатии, чувстве меры и умении сделать контакт красивым и осмысленным. Рядом с вами людям хочется становиться мягче, умнее и деликатнее.",
        "extra_elements": {
            "Огонь": "Яркая убедительность. Ваша мягкость соединяется с темпераментом, и вы умеете зажигать людей не только красотой, но и силой подачи.",
            "Земля": "Осязаемая эстетика. Вы умеете воплощать вкус в форму: в предметы, образы, пространство, стиль жизни и устойчивые решения.",
            "Вода": "Психологическая тонкость. Вы не просто чувствуете красоту, но и улавливаете скрытую эмоциональную динамику между людьми.",
        },
        "extra_modalities": {
            "Фиксированный": "Глубина привязанности. Вы умеете строить связи надолго и не относитесь к близости поверхностно.",
            "Мутабельный": "Социальная гибкость. Вы тонко чувствуете, как говорить с разными людьми, и умеете менять форму общения, сохраняя внутреннюю деликатность.",
        },
    },
    ("Фиксированный", "Воздух"): {
        "name": "ВИЗИОНЕР",
        "description": "Это архетип идеи, у которой есть стержень. Вы не просто думаете — вы формируете собственную картину мира и умеете оставаться ей верны, даже если окружающие не сразу её понимают. В вас сочетаются независимость мышления, внутренняя принципиальность и желание смотреть шире привычного. Вы можете быть человеком, который приносит в пространство новые концепции, неожиданные смыслы и ощущение будущего. Рядом с вами чувствуется интеллектуальная глубина, свобода и право быть собой.",
        "extra_elements": {
            "Огонь": "Искра влияния. Ваши идеи не просто существуют, а зажигают, увлекают и побуждают других смотреть на мир смелее.",
            "Земля": "Практичный концептуалист. Вы умеете придавать своим взглядам форму, превращая идеи в устойчивые проекты и решения.",
            "Вода": "Интуитивный мыслитель. За вашей логикой чувствуется тонкое чутьё, поэтому вы понимаете не только конструкции, но и скрытые мотивы.",
        },
        "extra_modalities": {
            "Кардинальный": "Запускает новую реальность. Вы не только держитесь своих идей, но и умеете вводить их в жизнь, меняя пространство вокруг.",
            "Мутабельный": "Многослойное мышление. Вы сохраняете внутренний стержень, но умеете переупаковывать свои идеи под разные контексты и людей.",
        },
    },
    ("Мутабельный", "Воздух"): {
        "name": "КОММУНИКАТОР",
        "description": "Это архетип движения мысли, слова и связи. В вас много любопытства, гибкости и природной способности чувствовать, как именно нужно говорить с разными людьми. Вы легко подхватываете новые идеи, быстро ориентируетесь в контекстах и умеете соединять разные миры через разговор, текст, обучение или обмен смыслами. Ваша энергия подвижна, жива и многогранна. Рядом с вами людям легче думать, интересоваться, задавать вопросы и замечать, насколько мир на самом деле разнообразен.",
        "extra_elements": {
            "Огонь": "Воодушевляющий голос. Ваше слово не только информирует, но и зажигает, побуждая людей действовать и верить в новые возможности.",
            "Земля": "Практичный посредник. Вы умеете превращать поток информации в понятные решения, инструкции и полезные связи.",
            "Вода": "Чуткий собеседник. Вы чувствуете оттенки настроения и умеете говорить так, что человек ощущает себя услышанным и понятым.",
        },
        "extra_modalities": {
            "Кардинальный": "Запускает диалог. Вы умеете первой создавать контакт, объединять людей и задавать направление разговору или процессу.",
            "Фиксированный": "Смысловой стержень. При всей гибкости вы умеете держаться важных для вас идей и не терять внутреннюю линию.",
        },
    },
    ("Кардинальный", "Вода"): {
        "name": "ПРОВОДНИК",
        "description": "Это архетип человека, который начинает новое, опираясь на чувство, интуицию и внутреннее знание момента. Вы очень тонко считываете атмосферу, улавливаете скрытые эмоциональные процессы и умеете действовать не только из логики, но и из глубинного ощущения «сейчас пора». Ваша сила в мягком, но сильном влиянии: вы можете направлять, поддерживать и открывать перемены через чувствительность, а не через давление. Рядом с вами люди ощущают, что их внутренний мир замечают и понимают.",
        "extra_elements": {
            "Огонь": "Сердце, которое зажигает. Ваша эмоциональная глубина соединяется со смелостью, и вы способны вдохновлять других очень живо и сильно.",
            "Земля": "Чуткая опора. Вы умеете не только чувствовать, но и превращать заботу, интуицию и внутреннее знание в реальную поддержку.",
            "Воздух": "Эмпатичный медиатор. Ваша чувствительность соединяется с ясностью ума, поэтому вы умеете находить слова для самых тонких состояний.",
        },
        "extra_modalities": {
            "Фиксированный": "Глубокая верность чувству. Вы не только открываете перемены, но и умеете удерживать эмоциональную связь и внутреннюю правду.",
            "Мутабельный": "Мягкое течение. Вы тонко чувствуете, когда лучше изменить способ действия, не предавая внутреннего импульса.",
        },
    },
    ("Фиксированный", "Вода"): {
        "name": "ТРАНСФОРМАТОР",
        "description": "Это архетип глубины, внутренней силы и способности проходить через сложное не разрушаясь, а меняясь. В вас много эмоциональной интенсивности, проницательности и умения чувствовать скрытую суть вещей. Вы не склонны жить только на поверхности: вам важно понимать, что на самом деле происходит внутри людей, отношений и жизненных процессов. Ваша энергия может быть сильной, магнетической и немного пугающей для тех, кто привык к простоте. Но именно в этом ваша сила: вы умеете видеть правду, выдерживать напряжение и превращать кризис в точку роста.",
        "extra_elements": {
            "Огонь": "Взрывная трансформация. Ваша глубина соединяется с мощной волей и делает вас человеком, который не просто меняется сам, но и резко меняет пространство вокруг.",
            "Земля": "Практичная глубина. Вы умеете превращать интуицию и внутреннее чутьё в реальные решения, защиту и устойчивость.",
            "Воздух": "Психологическая стратегия. Вы понимаете не только чувства, но и логику процессов, благодаря чему умеете тонко влиять на людей и ситуации.",
        },
        "extra_modalities": {
            "Кардинальный": "Провокатор изменений. Вы не ждёте, пока жизнь подтолкнёт к переменам, а сами чувствуете момент, когда пора запускать обновление.",
            "Мутабельный": "Исследователь глубины. Вы умеете искать новые смыслы, подходы и языки для того, чтобы проживать сложное не жёстко, а живо и многослойно.",
        },
    },
    ("Мутабельный", "Вода"): {
        "name": "МЕДИУМ",
        "description": "Это архетип тонкой чувствительности и глубокой внутренней подвижности. Вы словно живёте сразу в нескольких слоях реальности: чувствуете настроения, улавливаете невысказанное, замечаете то, что ускользает от более рационального взгляда. В вас много мягкости, воображения, эмпатии и способности растворяться в переживании, музыке, образе, любви или смысле. Вы можете быть очень чутким, вдохновляющим и исцеляющим человеком. Рядом с вами мир становится менее грубым и более живым, тонким, душевным.",
        "extra_elements": {
            "Огонь": "Оживляющая чувствительность. Ваш внутренний мир не только глубокий, но и яркий, поэтому вы способны вдохновлять через эмоцию и образ.",
            "Земля": "Заземлённая эмпатия. Вы умеете делать тонкое практичным: превращать интуицию, заботу и вдохновение в реальную помощь.",
            "Воздух": "Образный переводчик. Вы умеете находить слова для самых сложных чувств и превращать тонкие переживания в понятный язык.",
        },
        "extra_modalities": {
            "Кардинальный": "Запускает тонкие перемены. Вы чувствуете, когда пора мягко менять направление жизни, отношений или внутреннего состояния.",
            "Фиксированный": "Глубина переживания. При всей гибкости вы умеете очень преданно и глубоко проживать то, что для вас по-настоящему важно.",
        },
    },
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


def normalize_date_input(raw_text):
    cleaned = raw_text.strip()
    match = re.match(r"^\s*(\d{1,2})[./\-\s](\d{1,2})[./\-\s](\d{2}|\d{4})\s*$", cleaned)
    if not match:
        return None

    day, month, year = match.groups()
    day = int(day)
    month = int(month)
    year = int(year)

    if year < 100:
        year += 1900 if year >= 30 else 2000

    try:
        normalized = datetime.date(year, month, day)
    except ValueError:
        return None

    return normalized.strftime("%d.%m.%Y")


def normalize_time_input(raw_text):
    cleaned = raw_text.strip().lower()
    if cleaned in {"нет", "не знаю", "не помню"}:
        return "12:00"

    cleaned = cleaned.replace(".", ":")
    match = re.match(r"^\s*(\d{1,2})[:\s](\d{2})\s*$", cleaned)
    if not match:
        return None

    hour, minute = map(int, match.groups())
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    return f"{hour:02d}:{minute:02d}"


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


def get_secondary_category(counts, dominant_category):
    secondary = {name: count for name, count in counts.items() if name != dominant_category and count > 0}
    if not secondary:
        return None
    max_count = max(secondary.values())
    return sorted([name for name, count in secondary.items() if count == max_count])[0]


def should_show_secondary_category(counts, dominant_category, secondary_category, personal_signs, category_map):
    if not secondary_category:
        return False

    dominant_count = counts.get(dominant_category, 0)
    secondary_count = counts.get(secondary_category, 0)

    if secondary_count == dominant_count:
        return True
    if dominant_count - secondary_count == 1:
        return True

    personal_counts = {name: 0 for name in counts}
    for sign in personal_signs:
        category = category_map.get(sign)
        if category:
            personal_counts[category] += 1

    dominant_personal = personal_counts.get(dominant_category, 0)
    secondary_personal = personal_counts.get(secondary_category, 0)
    return secondary_personal > dominant_personal


def analyze_archetype(all_signs, personal_signs, priority_signs):
    modality_counts, element_counts = build_category_counts(all_signs)
    dominant_modality, modality_leaders, modality_source = resolve_dominant_category(
        modality_counts, personal_signs, priority_signs, SIGN_MODALITIES
    )
    dominant_element, element_leaders, element_source = resolve_dominant_category(
        element_counts, personal_signs, priority_signs, SIGN_ELEMENTS
    )
    secondary_modality = get_secondary_category(modality_counts, dominant_modality)
    secondary_element = get_secondary_category(element_counts, dominant_element)
    show_secondary_modality = should_show_secondary_category(
        modality_counts, dominant_modality, secondary_modality, personal_signs, SIGN_MODALITIES
    )
    show_secondary_element = should_show_secondary_category(
        element_counts, dominant_element, secondary_element, personal_signs, SIGN_ELEMENTS
    )

    archetype_name = f"{dominant_modality} {dominant_element}"

    return {
        "modality_counts": modality_counts,
        "element_counts": element_counts,
        "dominant_modality": dominant_modality,
        "dominant_element": dominant_element,
        "secondary_modality": secondary_modality,
        "secondary_element": secondary_element,
        "show_secondary_modality": show_secondary_modality,
        "show_secondary_element": show_secondary_element,
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
    secondary_modality = archetype_data["secondary_modality"]
    secondary_element = archetype_data["secondary_element"]
    show_secondary_modality = archetype_data["show_secondary_modality"]
    show_secondary_element = archetype_data["show_secondary_element"]
    archetype_name = archetype_data["archetype_name"]
    archetype_entry = ARCHETYPE_DATABASE.get((dominant_modality, dominant_element))

    if archetype_entry:
        title = archetype_entry["name"]
        description = archetype_entry["description"]
    else:
        title = archetype_name
        description = ARCHETYPE_OPENERS.get((dominant_modality, dominant_element), "Ваш архетип собран по доминирующему кресту и стихии карты.")

    parts = [
        f"✨ Ваш базовый архетип — <b>{title}</b>.",
        description,
    ]

    if secondary_element and show_secondary_element:
        extra_element_text = None
        if archetype_entry:
            extra_element_text = archetype_entry["extra_elements"].get(secondary_element)
        if not extra_element_text:
            extra_element_text = f"Дополнительный акцент стихии {secondary_element.lower()} делает ваш архетип более многослойным и добавляет ему особую манеру проявления."
        parts.append(
            f"Ваша уникальность в том, что к {ELEMENT_DATIVE.get(dominant_element, dominant_element.lower())} добавляется {secondary_element.lower()}. {extra_element_text}"
        )

    if secondary_modality and show_secondary_modality:
        extra_modality_text = None
        if archetype_entry:
            extra_modality_text = archetype_entry["extra_modalities"].get(secondary_modality)
        if not extra_modality_text:
            extra_modality_text = f"Дополнительный акцент креста {secondary_modality.lower()} меняет ваш стиль действий и делает его гибче."
        parts.append(
            f"Ваш микс крестов {dominant_modality} + {secondary_modality} говорит о том, что вы {extra_modality_text[0].lower() + extra_modality_text[1:]}"
        )

    archetype_data["archetype_title"] = title
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

        headers = [[
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
            "planet_summary",
            "planet_signs_json",
            "planet_houses_json",
            "archetype_report",
        ]]
        if worksheet.row_values(1) != headers[0]:
            worksheet.update("A1:T1", headers)
        return worksheet
    except Exception:
        logging.exception("Не удалось подключиться к Google Sheets")
        return None


def get_support_worksheet():
    if not GOOGLE_SHEET_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
        return None

    try:
        credentials_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        credentials = Credentials.from_service_account_info(credentials_info, scopes=GOOGLE_SCOPES)
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        try:
            worksheet = spreadsheet.worksheet(SUPPORT_WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=SUPPORT_WORKSHEET_NAME, rows=1000, cols=10)

        if not worksheet.cell(1, 1).value:
            worksheet.update("A1:G1", [[
                "saved_at",
                "telegram_user_id",
                "username",
                "first_name",
                "archetype_name",
                "message",
                "status",
            ]])
        return worksheet
    except Exception:
        logging.exception("Не удалось подключиться к листу support")
        return None


def normalize_saved_row(row):
    if not row:
        return None
    return {
        "saved_at": row.get("saved_at", ""),
        "telegram_user_id": str(row.get("telegram_user_id", "")).strip(),
        "username": row.get("username", ""),
        "first_name": row.get("first_name", ""),
        "birth_date": row.get("birth_date", ""),
        "birth_time": row.get("birth_time", ""),
        "birth_city": row.get("birth_city", ""),
        "latitude": row.get("latitude", ""),
        "longitude": row.get("longitude", ""),
        "timezone": row.get("timezone", ""),
        "sun_sign": row.get("sun_sign", ""),
        "moon_sign": row.get("moon_sign", ""),
        "asc_sign": row.get("asc_sign", ""),
        "dominant_modalities": row.get("dominant_modalities", ""),
        "dominant_elements": row.get("dominant_elements", ""),
        "archetype_name": row.get("archetype_name", ""),
        "planet_summary": row.get("planet_summary", ""),
        "planet_signs_json": row.get("planet_signs_json", ""),
        "planet_houses_json": row.get("planet_houses_json", ""),
        "archetype_report": row.get("archetype_report", ""),
    }


def find_profile_row(worksheet, user_id):
    records = worksheet.get_all_records()
    user_id = str(user_id)
    for index, row in enumerate(records, start=2):
        if str(row.get("telegram_user_id", "")).strip() == user_id:
            return index, normalize_saved_row(row)
    return None, None


def get_user_profile(user_id):
    worksheet = get_google_worksheet()
    if not worksheet:
        return None
    _, row = find_profile_row(worksheet, user_id)
    return row


def save_support_request(message, profile, complaint_text):
    worksheet = get_support_worksheet()
    if not worksheet:
        return False

    try:
        worksheet.append_row([
            datetime.datetime.utcnow().isoformat(),
            str(message.from_user.id),
            message.from_user.username or "",
            message.from_user.first_name or "",
            (profile or {}).get("archetype_name", ""),
            complaint_text,
            "new",
        ])
        return True
    except Exception:
        logging.exception("Не удалось сохранить обращение в support")
        return False


def save_profile_to_google_sheets(message, profile_data):
    worksheet = get_google_worksheet()
    if not worksheet:
        return

    try:
        row_values = [
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
            profile_data["planet_summary"],
            json.dumps(profile_data["planet_signs"], ensure_ascii=False),
            json.dumps(profile_data["planet_houses"], ensure_ascii=False),
            profile_data["archetype_report"],
        ]
        row_index, _ = find_profile_row(worksheet, message.from_user.id)
        if row_index:
            worksheet.update(f"A{row_index}:T{row_index}", [row_values])
        else:
            worksheet.append_row(row_values)
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


class SupportFlow(StatesGroup):
    waiting_for_message = State()


def build_main_keyboard(profile_exists=False, expanded=False, developer=False):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    if not profile_exists:
        kb.row(types.KeyboardButton("Старт ✨"))
        if developer:
            kb.row(types.KeyboardButton("Сбросить 🌑"))
        return kb

    if not expanded:
        kb.row(types.KeyboardButton("Меню"))
        if developer:
            kb.row(types.KeyboardButton("Сбросить 🌑"))
        return kb

    if MINI_APP_URL:
        kb.row(types.KeyboardButton("Карта дня ✨", web_app=types.WebAppInfo(url=MINI_APP_URL)))
    else:
        kb.row(types.KeyboardButton("Карта дня ✨"))
    kb.row(
        types.KeyboardButton("Карьера"),
        types.KeyboardButton("Бизнес"),
    )
    kb.row(
        types.KeyboardButton("Отношения"),
        types.KeyboardButton("Здоровье"),
    )
    kb.row(
        types.KeyboardButton("Натальная карта"),
        types.KeyboardButton("Вопрос"),
    )
    kb.row(types.KeyboardButton("Саппорт"))
    kb.row(types.KeyboardButton("Скрыть меню"))
    if developer:
        kb.row(types.KeyboardButton("Сбросить 🌑"))
    return kb


FEATURE_TEXTS = {
    "daily": (
        "🌙 <b>Карта дня</b>\n\n"
        "Кнопка mini app доступна в раскрытом меню. Если она не открывается, значит ещё нужно проверить публичный URL приложения."
    ),
    "career": (
        "💼 <b>Архетип и карьера</b>\n\n"
        "Этот раздел подготовим следующим: здесь будет разбор сильных сторон архетипа в работе, деньгах, формате занятости и стиле проявления в профессии."
    ),
    "business": (
        "📈 <b>Архетип и бизнес</b>\n\n"
        "Этот раздел подготовим следующим: здесь будет разбор того, как твой архетип проявляется в предпринимательстве, лидерстве, деньгах и масштабировании."
    ),
    "relations": (
        "💞 <b>Архетип и отношения</b>\n\n"
        "Здесь появится дополнительный разбор того, как архетип проявляется в близости, выборе партнёра, границах и сценариях отношений."
    ),
    "health": (
        "🌿 <b>Архетип и здоровье</b>\n\n"
        "Этот блок станет отдельным мягким разбором ресурса, ритма жизни и точек, где особенно важно бережное отношение к себе."
    ),
    "natal": (
        "🔮 <b>Индивидуальный разбор натальной карты</b>\n\n"
        "Здесь мы позже добавим сценарий записи на персональный разбор: бот задаст несколько уточняющих вопросов и поможет оставить заявку на консультацию."
    ),
    "question": (
        "✨ <b>Свой вопрос</b>\n\n"
        "Этот раздел станет входом для личного запроса: можно будет описать ситуацию, выбрать тему и получить дальнейший сценарий сопровождения или записи."
    ),
    "support": (
        "🛠 <b>Саппорт</b>\n\n"
        "Напиши одним сообщением, что именно не работает или что хотелось бы улучшить. Я сохраню обращение для разбора."
    ),
    "daily_unavailable": (
        "🌙 <b>Карта дня</b>\n\n"
        "Мини-приложение уже подготовлено в коде, осталось только привязать публичный URL. Как только `MINI_APP_URL` или `WEBHOOK_HOST` будут настроены, кнопка начнёт открывать экран карты дня прямо внутри Telegram."
    ),
}


def build_post_archetype_keyboard():
    return None


async def card_of_day_webapp(request):
    return web.FileResponse(BASE_DIR / "webapp" / "card_of_day.html")


async def index_page(request):
    return web.Response(
        text="astro-bot is running",
        content_type="text/plain",
    )


async def healthcheck(request):
    return web.json_response({"ok": True})


def is_developer(user_id):
    return user_id in DEV_TELEGRAM_IDS


def build_saved_profile_text(profile):
    archetype = profile.get("archetype_name") or "не определён"
    birth_date = profile.get("birth_date") or "—"
    birth_time = profile.get("birth_time") or "—"
    birth_city = profile.get("birth_city") or "—"
    return (
        "✨ <b>Твой профиль уже сохранён</b>\n\n"
        f"<b>Архетип:</b> {archetype}\n"
        f"<b>Дата рождения:</b> {birth_date}\n"
        f"<b>Время:</b> {birth_time}\n"
        f"<b>Место:</b> {birth_city}\n\n"
        "Ниже можешь открыть нужный раздел и продолжить работу с ботом."
    )


def build_natal_text(profile):
    archetype_report = profile.get("archetype_report", "").strip()
    planet_summary = profile.get("planet_summary", "").strip()
    parts = ["🔮 <b>Твоя натальная карта</b>"]
    if archetype_report:
        parts.append(archetype_report)
    if planet_summary:
        parts.append(f"<b>Положение планет:</b>\n{planet_summary.replace(' | ', chr(10))}")
    return "\n\n".join(parts)


async def show_profile_home(message):
    profile = get_user_profile(message.from_user.id)
    if not profile:
        await message.reply(
            "Привет! Я помогу тебе узнать твой астрологический архетип. Нажми «Старт ✨», чтобы ввести дату, время и место рождения.",
            reply_markup=build_main_keyboard(profile_exists=False, developer=is_developer(message.from_user.id)),
        )
        return

    await message.reply(
        build_saved_profile_text(profile),
        parse_mode="HTML",
        reply_markup=build_main_keyboard(profile_exists=True, expanded=False, developer=is_developer(message.from_user.id)),
    )


async def send_feature_response(message, feature_key, profile=None):
    if feature_key == "natal" and profile:
        await message.reply(build_natal_text(profile), parse_mode="HTML")
        return
    if feature_key == "support":
        await SupportFlow.waiting_for_message.set()
    text = FEATURE_TEXTS.get(
        feature_key,
        "Этот раздел уже отмечен в боте, но текст-заглушка для него пока не настроен."
    )
    await message.reply(text, parse_mode="HTML")

@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    profile = get_user_profile(message.from_user.id)
    if profile:
        await show_profile_home(message)
        return

    greetings = [
        "Привет! Я помогу тебе узнать твой астрологический архетип. Для начала введи дату рождения в формате ДД.ММ.ГГГГ ✨",
        "Давай определим твой астрологический архетип. Введи дату рождения в формате ДД.ММ.ГГГГ 🌌",
        "Сейчас мы посмотрим, какой архетип заложен в твоей карте. Введи дату рождения: ДД.ММ.ГГГГ 🗓️"
    ]
    kb = build_main_keyboard(profile_exists=False, developer=is_developer(message.from_user.id))
    await message.reply(random.choice(greetings), parse_mode='HTML', reply_markup=kb)
    await BirthData.waiting_for_date.set()

@dp.message_handler(lambda message: message.text == "Сбросить 🌑", state="*")
async def handle_reset_button(message: types.Message, state: FSMContext):
    if is_developer(message.from_user.id):
        await reset_state(message, state)
    else:
        await message.reply("Сброс профиля недоступен в пользовательском режиме. Если есть проблема, напиши в саппорт.")


@dp.message_handler(lambda message: message.text == "Старт ✨", state="*")
async def handle_start_button(message: types.Message, state: FSMContext):
    if get_user_profile(message.from_user.id) and not is_developer(message.from_user.id):
        await show_profile_home(message)
        return
    await state.finish()
    await start(message)


@dp.message_handler(lambda message: message.text == "Меню", state="*")
async def handle_menu_button(message: types.Message, state: FSMContext):
    profile = get_user_profile(message.from_user.id)
    if not profile:
        await message.reply(
            "Сначала нужно нажать «Старт ✨» и заполнить дату, время и место рождения.",
            reply_markup=build_main_keyboard(profile_exists=False, developer=is_developer(message.from_user.id)),
        )
        return
    await state.finish()
    await message.reply(
        "Выбери нужный раздел.",
        reply_markup=build_main_keyboard(profile_exists=True, expanded=True, developer=is_developer(message.from_user.id)),
    )


@dp.message_handler(lambda message: message.text == "Скрыть меню", state="*")
async def handle_hide_menu_button(message: types.Message, state: FSMContext):
    await state.finish()
    await message.reply(
        "Меню свернуто.",
        reply_markup=build_main_keyboard(profile_exists=bool(get_user_profile(message.from_user.id)), expanded=False, developer=is_developer(message.from_user.id)),
    )

@dp.message_handler(state=BirthData.waiting_for_date)
async def process_date(message: types.Message, state: FSMContext):
    try:
        if get_user_profile(message.from_user.id) and not is_developer(message.from_user.id):
            await state.finish()
            profile = get_user_profile(message.from_user.id)
            await message.reply(
                build_saved_profile_text(profile),
                parse_mode="HTML",
                reply_markup=build_main_keyboard(profile_exists=True, expanded=False, developer=is_developer(message.from_user.id)),
            )
            return
        normalized_date = normalize_date_input(message.text)
        if not normalized_date:
            await message.reply("Я могу принять дату в форматах 25.06.1992, 25/06/1992, 25 06 1992 или 25/06/92.")
            return
        await state.update_data(date=normalized_date)
        await BirthData.waiting_for_time.set()
        await message.reply("Теперь введи время рождения в формате ЧЧ:ММ, ЧЧ ММ или напиши «нет», если не знаешь. Тогда я подставлю 12:00 🕰")
    except Exception as e:
        logging.exception("Ошибка при обработке даты:")
        await message.reply(f"Ошибка: {e}")

@dp.message_handler(state=BirthData.waiting_for_time)
async def process_time(message: types.Message, state: FSMContext):
    try:
        normalized_time = normalize_time_input(message.text)
        if not normalized_time:
            await message.reply("Я могу принять время в форматах 19:45, 19 45 или «нет», если время неизвестно.")
            return
        await state.update_data(time=normalized_time)
        await BirthData.waiting_for_place.set()
        await message.reply("И наконец, введи город рождения 🌍")
    except Exception as e:
        logging.exception("Ошибка при обработке времени:")
        await message.reply(f"Ошибка: {e}")

@dp.message_handler(state=BirthData.waiting_for_place)
async def process_place(message: types.Message, state: FSMContext):
    try:
        if get_user_profile(message.from_user.id) and not is_developer(message.from_user.id):
            await state.finish()
            profile = get_user_profile(message.from_user.id)
            await message.reply(
                build_saved_profile_text(profile),
                parse_mode="HTML",
                reply_markup=build_main_keyboard(profile_exists=True, expanded=False, developer=is_developer(message.from_user.id)),
            )
            return
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
        planet_summary_rows = []
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

                if house:
                    planet_summary_rows.append(f"{point_name} — {sign_ru} в доме {house}")
                else:
                    planet_summary_rows.append(f"{point_name} — {sign_ru} (дом не найден)")

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
        planet_summary_rows.append(f"Ascendant (ASC) — {asc_sign_ru}")

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
            "archetype_name": archetype_data["archetype_title"],
            "archetype_report": archetype_report,
            "planet_summary": " | ".join(planet_summary_rows),
            "planet_signs": planet_signs,
            "planet_houses": planet_houses,
        })
        await message.reply(archetype_report, parse_mode="HTML")
        await message.reply(
            "Профиль сохранён. Теперь внизу у тебя есть кнопка «Меню» со всеми разделами.",
            reply_markup=build_main_keyboard(profile_exists=True, expanded=False, developer=is_developer(message.from_user.id)),
        )
        await state.finish()

    except Exception as e:
        logging.exception("Ошибка при расчёте карты:")
        await message.reply(f"Что-то пошло не так 😕 Ошибка: {e}")
        await state.finish()

@dp.message_handler(commands=['reset'])
async def cmd_reset(message: types.Message, state: FSMContext):
    if not is_developer(message.from_user.id):
        await message.reply("Эта команда доступна только разработчику.")
        return
    await reset_state(message, state)

@dp.message_handler(commands=['ping'])
async def cmd_ping(message: types.Message):
    await message.reply("✅ Я на связи!")


async def handle_section_command(message: types.Message, state: FSMContext, feature_key: str):
    profile = get_user_profile(message.from_user.id)
    if not profile:
        await state.finish()
        await message.reply(
            "Сначала нажми «Старт ✨» и заполни дату, время и место рождения, чтобы я сохранил твой профиль.",
            reply_markup=build_main_keyboard(profile_exists=False, developer=is_developer(message.from_user.id)),
        )
        return

    if feature_key != "support":
        await state.finish()
    await send_feature_response(message, feature_key, profile=profile)


@dp.message_handler(commands=['career'])
async def cmd_career(message: types.Message, state: FSMContext):
    await handle_section_command(message, state, "career")


@dp.message_handler(commands=['business'])
async def cmd_business(message: types.Message, state: FSMContext):
    await handle_section_command(message, state, "business")


@dp.message_handler(commands=['relations'])
async def cmd_relations(message: types.Message, state: FSMContext):
    await handle_section_command(message, state, "relations")


@dp.message_handler(commands=['health'])
async def cmd_health(message: types.Message, state: FSMContext):
    await handle_section_command(message, state, "health")


@dp.message_handler(commands=['natal'])
async def cmd_natal(message: types.Message, state: FSMContext):
    await handle_section_command(message, state, "natal")


@dp.message_handler(commands=['question'])
async def cmd_question(message: types.Message, state: FSMContext):
    await handle_section_command(message, state, "question")


@dp.message_handler(commands=['support'])
async def cmd_support(message: types.Message, state: FSMContext):
    await handle_section_command(message, state, "support")


@dp.message_handler(commands=['daily'])
async def cmd_daily(message: types.Message, state: FSMContext):
    profile = get_user_profile(message.from_user.id)
    if not profile:
        await state.finish()
        await message.reply(
            "Сначала нажми «Старт ✨» и заполни дату, время и место рождения, чтобы я сохранил твой профиль.",
            reply_markup=build_main_keyboard(profile_exists=False, developer=is_developer(message.from_user.id)),
        )
        return

    if MINI_APP_URL:
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("Открыть карту дня ✨", web_app=types.WebAppInfo(url=MINI_APP_URL)))
        await message.reply("Открой карту дня в mini app.", reply_markup=kb)
        return

    await handle_section_command(message, state, "daily")


@dp.message_handler(commands=['dev_me'])
async def cmd_dev_me(message: types.Message):
    if not is_developer(message.from_user.id):
        return
    await message.reply(f"Developer mode\nuser_id={message.from_user.id}")


@dp.message_handler(commands=['dev_reset_profile'])
async def cmd_dev_reset_profile(message: types.Message, state: FSMContext):
    if not is_developer(message.from_user.id):
        return
    await reset_state(message, state)


@dp.callback_query_handler(lambda call: call.data and call.data.startswith("feature:"))
async def handle_feature_callbacks(call: types.CallbackQuery):
    feature_key = call.data.split(":", 1)[1]
    await call.answer()
    profile = get_user_profile(call.from_user.id)
    await send_feature_response(call.message, feature_key, profile=profile)


@dp.message_handler(content_types=types.ContentType.WEB_APP_DATA)
async def handle_web_app_data(message: types.Message):
    try:
        payload = json.loads(message.web_app_data.data)
        card_title = payload.get("title", "Карта дня")
        card_symbol = payload.get("symbol", "✨")
        card_message = payload.get("message", "")
        focus = payload.get("focus", "")
        mantra = payload.get("mantra", "")
        date_label = payload.get("date_label", "")

        parts = [f"{card_symbol} <b>{card_title}</b>"]
        if date_label:
            parts.append(f"<i>{date_label}</i>")
        if card_message:
            parts.append(card_message)
        if focus:
            parts.append(f"<b>Фокус дня:</b> {focus}")
        if mantra:
            parts.append(f"<b>Мантра:</b> {mantra}")

        await message.reply("\n\n".join(parts), parse_mode="HTML")
    except Exception:
        logging.exception("Ошибка при обработке данных из mini app")
        await message.reply("Не удалось прочитать данные из мини-приложения. Попробуй открыть карту дня ещё раз.")


MENU_ACTIONS = {
    "Карта дня ✨": "daily",
    "Карьера": "career",
    "Бизнес": "business",
    "Отношения": "relations",
    "Здоровье": "health",
    "Натальная карта": "natal",
    "Вопрос": "question",
    "Саппорт": "support",
}


@dp.message_handler(lambda message: message.text in MENU_ACTIONS, state="*")
async def handle_menu_buttons(message: types.Message, state: FSMContext):
    profile = get_user_profile(message.from_user.id)
    if not profile:
        await message.reply("Сначала нужно один раз заполнить дату, время и место рождения, чтобы я сохранил твой профиль.")
        await BirthData.waiting_for_date.set()
        return

    if MENU_ACTIONS[message.text] != "support":
        await state.finish()
    await send_feature_response(message, MENU_ACTIONS[message.text], profile=profile)


@dp.message_handler(state=SupportFlow.waiting_for_message, content_types=types.ContentTypes.TEXT)
async def handle_support_message(message: types.Message, state: FSMContext):
    complaint_text = message.text.strip()
    if len(complaint_text) < 5:
        await message.reply("Опиши проблему чуть подробнее, чтобы я мог сохранить обращение.")
        return

    profile = get_user_profile(message.from_user.id)
    success = save_support_request(message, profile, complaint_text)
    await state.finish()
    if success:
        await message.reply(
            "Спасибо, я сохранил обращение в саппорт. Можешь продолжать пользоваться ботом.",
            reply_markup=build_main_keyboard(profile_exists=True, expanded=False, developer=is_developer(message.from_user.id)),
        )
    else:
        await message.reply(
            "Не удалось сохранить обращение в таблицу, но я уже знаю, что этот блок нужно проверить.",
            reply_markup=build_main_keyboard(profile_exists=True, expanded=False, developer=is_developer(message.from_user.id)),
        )

async def reset_state(message: types.Message, state: FSMContext):
    await state.finish()
    kb = build_main_keyboard(profile_exists=False, developer=is_developer(message.from_user.id))
    await message.reply("🔄 Всё сброшено. Начнём заново!\nВведи дату рождения в формате ДД.ММ.ГГГГ ✨", reply_markup=kb)
    await BirthData.waiting_for_date.set()

@dp.message_handler()
async def fallback_handler(message: types.Message):
    if get_user_profile(message.from_user.id):
        await message.reply(
            "Твой профиль уже сохранён. Нажми «Меню», чтобы открыть разделы.",
            reply_markup=build_main_keyboard(profile_exists=True, expanded=False, developer=is_developer(message.from_user.id)),
        )
    else:
        await message.reply(
            "Нажми «Старт ✨», чтобы ввести дату, время и место рождения и получить архетип.",
            reply_markup=build_main_keyboard(profile_exists=False, developer=is_developer(message.from_user.id)),
        )

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
