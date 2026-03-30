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

ARCHETYPE_DATABASE = {
    ("Кардинальный", "Огонь"): {
        "name": "ПЕРВОПРОХОДЕЦ",
        "description": "Это энергия чистого импульса и дерзости. Вы рождены, чтобы открывать новые двери, запускать проекты с нуля и вести людей за собой туда, где еще никто не был. Ваша сила в скорости принятия решений и бесстрашии перед неизвестностью. Вы не ждете разрешения — вы создаете инфоповод сами.",
        "extra_elements": {
            "Воздух": "Идейный лидер. Ваша энергия подкрепляется мощным интеллектом, превращая импульс в аргументированную концепцию.",
            "Земля": "Строитель бизнеса. Вы не просто зажигаете искру, а сразу закладываете фундамент, превращая драйв в реальный доход.",
            "Вода": "Эмоциональный триггер. Ваша проявленность глубоко цепляет чувства людей, создавая вокруг вас преданное комьюнити.",
        },
        "extra_modalities": {
            "Фиксированный": "Доводит начатое до конца. Вы обладаете редким даром не перегорать после старта, а методично превращать вспышку в вечное пламя.",
            "Мутабельный": "Постоянно меняет ниши. Ваша сила в многозадачности: вы мастер серийных запусков и умеете вовремя переключиться на более перспективное направление.",
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
        "description": "Вы обладаете талантом превращать хаос в четкую иерархию и рабочую систему. Ваша цель — не просто результат, а создание масштабной структуры или империи, которая будет стоять вечно. Вы видите вершину и прокладываете к ней самый эффективный маршрут, опираясь на логику и дисциплину. Ваша суперсила — умение материализовать амбиции в конкретные цифры.",
        "extra_elements": {
            "Воздух": "Системный аналитик. Ваши решения основаны на безупречной логике и анализе данных, что делает ваши стратегии непобедимыми.",
            "Огонь": "Амбициозный CEO. В вашей прагматичности горит огонь достижений, заставляя систему работать на сверхскоростях.",
            "Вода": "Мудрый руководитель. Вы строите систему, учитывая человеческий фактор, создавая экологичную и лояльную среду.",
        },
        "extra_modalities": {
            "Фиксированный": "Железная стабильность. Вы не только строите систему, но и гарантируете её неуязвимость перед любыми внешними кризисами.",
            "Мутабельный": "Адаптивный менеджер. Ваша стратегия гибка: вы мгновенно перестраиваете структуру под меняющиеся условия рынка.",
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
        "description": "Ювелирная работа с реальностью, где важна каждая деталь и шестеренка. Вы обладаете уникальным аналитическим умом, способным оптимизировать любой процесс до идеального состояния. Ваша суперсила — приносить конкретную пользу здесь и сейчас, делая мир вокруг понятнее и эффективнее. Вы мастер инструментов, который знает, как улучшить абсолютно всё.",
        "extra_elements": {
            "Воздух": "Редактор смыслов. Вы виртуозно работаете с информацией, превращая сложные данные в кристально понятные инструкции.",
            "Огонь": "Технический директор. Вы внедряете инновации с горящими глазами, заставляя механизмы работать быстрее и мощнее.",
            "Вода": "Целитель. Ваше внимание к деталям направлено на заботу; вы исправляете несовершенства так мягко, что это ощущается как терапия.",
        },
        "extra_modalities": {
            "Кардинальный": "Организатор хаоса. Вы активно берете на себя инициативу по наведению порядка там, где другие сдались перед сложностью.",
            "Фиксированный": "Хранитель стандартов. Вы создаете эталон качества один раз и навсегда, становясь гарантом безупречного результата.",
        },
    },
    ("Кардинальный", "Воздух"): {
        "name": "ЭСТЕТ",
        "description": "Вы создаете правила игры в социуме, опираясь на принципы гармонии, красоты и этики. Ваша сила в дипломатии, умении договариваться и создавать безупречный интеллектуальный контекст. Вы — мост между людьми, который объединяет противоположности ради общей цели. Вы задаете стандарты вкуса, превращая любое общение в искусство.",
        "extra_elements": {
            "Огонь": "Яркий дипломат. Ваши переговоры — это шоу; вы убеждаете людей не только логикой, но и неудержимой энергией.",
            "Земля": "Создатель красоты. Вы приземляете эстетику, создавая реальные красивые объекты, интерьеры или бренды.",
            "Вода": "Тонкий психолог. Вы чувствуете нюансы отношений и управляете атмосферой в коллективе на уровне интуиции.",
        },
        "extra_modalities": {
            "Фиксированный": "Верный партнер. Если вы выбрали сторону или человека, вы будете выстраивать эти отношения десятилетиями, создавая прочный союз.",
            "Мутабельный": "Социальный хамелеон. Вы обладаете невероятной гибкостью, подбирая ключик к любому собеседнику за считанные секунды.",
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
        "description": "Глубинная, магнетическая энергия, способная видеть скрытую суть вещей и управлять кризисами. Вы не боитесь идти в самые сложные темы, разрушая старое ради рождения чего-то по-настоящему живого. Ваша сила в проницательности и способности влиять на подсознание других. Вы — алхимик чувств, который превращает боль в ресурс, а страх в силу.",
        "extra_elements": {
            "Огонь": "Роковая энергия. Ваша способность к трансформации взрывоопасна; вы меняете жизни людей ярко и бесповоротно.",
            "Земля": "Кризис-менеджер. Вы используете свою интуицию для спасения реальных активов, видя дыры в системе там, где другие ослепли.",
            "Воздух": "Стратег-манипулятор. Вы понимаете психологию масс и умеете тонко направлять потоки информации в нужное вам русло.",
        },
        "extra_modalities": {
            "Кардинальный": "Инициатор перемен. Вы не ждете кризиса, вы сами провоцируете трансформацию, чтобы ускорить эволюцию вокруг себя.",
            "Мутабельный": "Исследователь тайн. Ваша сила в поиске; вы постоянно адаптируете свои знания о глубинах психики под новые реалии.",
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
            f"Ваша уникальность в том, что к {dominant_element.lower()} добавляется {secondary_element.lower()}. {extra_element_text}"
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
                "planet_summary",
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
            profile_data["planet_summary"],
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
            "archetype_name": archetype_data["archetype_name"],
            "planet_summary": " | ".join(planet_summary_rows),
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
