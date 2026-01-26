# 🚀 Quick Start Guide

## Локальный запуск за 5 минут

### 1. Подготовка

```bash
# Создайте .env файл
cp .env.example .env
```

Заполните `.env`:
```env
DATABASE_URL=postgres://postgres:postgres@localhost:5432/millionaire_bot?sslmode=disable
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
TELEGRAM_CHAT_ID=your_chat_id
```

### 2. Запуск PostgreSQL

```bash
# Используя Docker Compose (рекомендуется)
docker-compose up -d postgres

# Или через Makefile
make postgres
```

### 3. Запуск бота

```bash
# Способ 1: Через go run
go run cmd/bot/main.go

# Способ 2: Собрать и запустить
make build
./bot

# Способ 3: Через Docker Compose (полный стек)
docker-compose up
```

## Деплой на Railway за 2 минуты

### 1. Установите Railway CLI

```bash
npm install -g @railway/cli
railway login
```

### 2. Создайте проект

```bash
railway init
```

### 3. Добавьте PostgreSQL

В Railway Dashboard:
- New → Database → PostgreSQL

### 4. Добавьте переменные окружения

В Variables:
```
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
```

### 5. Задеплойте

```bash
railway up
```

Готово! Бот работает. 🎉

## Проверка работы

После запуска вы должны увидеть логи:

```
{"level":"info","message":"🚀 Starting Millionaire Bot"}
{"level":"info","message":"configuration loaded successfully"}
{"level":"info","message":"database connection established"}
{"level":"info","count":100,"message":"loaded top USDT futures"}
{"level":"info","message":"WebSocket connected successfully"}
{"level":"info","message":"✅ Bot is running and monitoring markets"}
```

Когда бот найдет сигнал, вы получите уведомление в Telegram! 📱

## Полезные команды

```bash
make help          # Показать все доступные команды
make build         # Собрать бота
make run           # Запустить бота
make test          # Запустить тесты
make fmt           # Отформатировать код
make clean         # Очистить артефакты сборки
```

## Troubleshooting

**Проблема:** `failed to connect to database`
**Решение:** Проверьте что PostgreSQL запущен и DATABASE_URL корректен

**Проблема:** `TELEGRAM_BOT_TOKEN is required`
**Решение:** Создайте бота через @BotFather и добавьте токен в .env

**Проблема:** `failed to fetch top 100 USDT futures`
**Решение:** Проверьте интернет соединение, ByBit может быть недоступен в вашем регионе

## Дальнейшие шаги

1. Прочитайте [README.md](README.md) для детальной документации
2. Изучите [spec.md](spec.md) для понимания стратегии
3. Настройте мониторинг логов на Railway
4. Подключите оповещения о критичных ошибках

Удачи в трейдинге! 💰
