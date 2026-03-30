FROM python:3.10-slim

# Обновление и установка системных библиотек
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    git \
    pkg-config \
    libc-dev \
    libffi-dev \
    libssl-dev

# Установка рабочей директории и копирование файлов
WORKDIR /app
COPY . .

# Установка зависимостей Python
RUN pip install --no-cache-dir -r requirements.txt

# Запуск бота
CMD ["python", "main.py"]

