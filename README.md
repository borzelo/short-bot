# Millionaire Bot - Weak Asset Breakdown Detector

Высокопроизводительный бот для мониторинга крипто-фьючерсов на ByBit и детектирования сигналов "Пробоя слабых активов".

## 🎯 Стратегия

Бот автоматически:
1. **Определяет слабость** - находит активы, которые слабее BTC (Relative Strength < 0)
2. **Находит уровни поддержки** - определяет локальные минимумы на 1-минутном таймфрейме
3. **Сигнализирует пробой** - триггерит алерт при пробое поддержки с высоким объемом
4. **Оценивает сигнал** - присваивает скор от 0 до 100 на основе множества факторов

## 📊 Скоринг (0-100)

- **Слабость актива** (до 40 баллов)
  - RS < -3%: +30 баллов
  - RS < -5%: +10 дополнительно

- **Объем пробоя** (до 30 баллов)
  - Объем > 2x среднего: +20 баллов
  - Объем > 3x среднего: +10 дополнительно

- **Возраст уровня** (20 баллов)
  - Уровень существует > 30 минут: +20 баллов

- **Дивергенция с BTC** (10 баллов)
  - BTC растет, актив падает: +10 баллов

## 🏗 Архитектура

```
millionaire-bot/
├── cmd/bot/              # Точка входа
├── internal/
│   ├── config/          # Конфигурация из ENV
│   ├── models/          # Модели данных
│   ├── db/              # PostgreSQL слой
│   ├── bybit/           # WebSocket + REST API клиент
│   ├── strategy/        # Логика стратегии и скоринга
│   └── telegram/        # Telegram уведомления
├── Dockerfile
├── railway.toml
└── go.mod
```

## 🚀 Деплой на Railway

### 1. Подготовка

1. Создайте Telegram бота через [@BotFather](https://t.me/botfather)
2. Получите Chat ID вашего чата (можно через [@userinfobot](https://t.me/userinfobot))

### 2. Создайте проект на Railway

```bash
# Установите Railway CLI (если не установлен)
npm install -g @railway/cli

# Залогиньтесь
railway login

# Создайте новый проект
railway init
```

### 3. Добавьте PostgreSQL

В Railway Dashboard:
- Click "New" → "Database" → "Add PostgreSQL"
- Railway автоматически создаст переменную `DATABASE_URL`

### 4. Настройте переменные окружения

В Railway Dashboard → Variables:

```env
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
TELEGRAM_CHAT_ID=your_chat_id
LOG_LEVEL=info
```

`DATABASE_URL` уже будет создан автоматически при добавлении PostgreSQL.

### 5. Деплой

```bash
# Подключите репозиторий к Railway
railway link

# Задеплойте
railway up
```

Или подключите GitHub репозиторий через Railway Dashboard для автоматических деплоев.

## 💻 Локальная разработка

### Требования

- Go 1.22+
- PostgreSQL 15+
- Telegram Bot Token

### Установка

```bash
# Клонируйте репозиторий
git clone <your-repo>
cd millionaire-bot

# Скопируйте .env.example в .env и заполните значения
cp .env.example .env

# Установите зависимости
go mod download

# Запустите PostgreSQL (через Docker)
docker run --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=millionaire_bot -p 5432:5432 -d postgres:15

# Обновите DATABASE_URL в .env
# DATABASE_URL=postgres://postgres:postgres@localhost:5432/millionaire_bot?sslmode=disable

# Запустите бота
go run cmd/bot/main.go
```

### Запуск тестов

```bash
go test ./...
```

### Сборка

```bash
go build -o bot cmd/bot/main.go
./bot
```

## 📱 Формат уведомлений в Telegram

```
🚨 СИГНАЛ: ПРОБОЙ УРОВНЯ 🚨

Тикер: #SOLUSDT
Цена: 142.10 (Пробит уровень: 142.50)

📉 Факторы слабости:
• Относит. сила (RS): -4.2% (Слабее рынка)
• Объем при пробое: x2.4 от среднего

🛠 Технический анализ:
• Время жизни уровня: 45 мин
• Объем пробоя: 123456.78

🏆 ОБЩИЙ БАЛЛ: 85/100
(Высокая вероятность падения)
```

## 🔧 Конфигурация

### Переменные окружения

| Переменная | Описание | По умолчанию |
|-----------|----------|--------------|
| `DATABASE_URL` | PostgreSQL connection string | - (обязательно) |
| `TELEGRAM_BOT_TOKEN` | Токен Telegram бота | - (обязательно) |
| `TELEGRAM_CHAT_ID` | ID чата для уведомлений | - (обязательно) |
| `BYBIT_WS_URL` | WebSocket URL ByBit | `wss://stream.bybit.com/v5/public/linear` |
| `BYBIT_API_URL` | REST API URL ByBit | `https://api.bybit.com` |
| `BYBIT_API_KEY` | API ключ (опционально) | - |
| `BYBIT_API_SECRET` | API секрет (опционально) | - |
| `LOG_LEVEL` | Уровень логирования | `info` |

## 📈 Мониторинг

Бот логирует следующие события:

- Подключение к WebSocket
- Получение свечей
- Генерация сигналов
- Отправка уведомлений
- Статистика каждые 5 минут

Логи в Railway доступны в разделе "Deployments" → "View Logs".

## 🛡 Обработка ошибок

- **WebSocket переподключение**: Автоматическое переподключение с exponential backoff
- **Database retry**: Graceful handling с логированием ошибок
- **Telegram retry**: Логирование неудачных отправок без остановки бота

## 📊 База данных

### Таблица `assets`
Хранит информацию о торговых инструментах

### Таблица `signals`
Хранит все сгенерированные сигналы с метаданными

Индексы оптимизированы для быстрого поиска по символу и времени.

## 🔐 Безопасность

- Все секреты через переменные окружения
- Нет хардкода ключей в коде
- `.env` файл в `.gitignore`
- PostgreSQL соединение через SSL на production

## 📝 Лицензия

MIT

## 🤝 Вклад

Приветствуются Pull Requests и Issues!

## 📮 Контакты

Вопросы и предложения: [создайте Issue](https://github.com/your-username/millionaire-bot/issues)
