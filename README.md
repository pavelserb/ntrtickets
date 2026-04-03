# TicketBot — Ticket Sales Collector

Сбор данных о продажах билетов из нескольких билетных сервисов с единым хранилищем.

## Архитектура

```
TicketBot/
├── collector.py              # оркестратор: конфиг → сбор → SQLite
├── config.yaml               # ивенты, источники, цели продаж
├── sources/
│   ├── bilesu_serviss.py     # Biļešu Serviss (JSON API)
│   └── mticket.py            # mticket (MySQL)
├── sales.db                  # локальная БД (создаётся автоматически)
├── .env                      # секреты подключения (не коммитить!)
└── logs/
```

## Быстрый старт

```bash
pip3 install -r requirements.txt
cp .env.example .env           # заполнить секреты
# отредактировать config.yaml  # настроить ивенты

python3 collector.py               # все ивенты
python3 collector.py -e prodigy-2026   # конкретный ивент
python3 collector.py -v            # подробный вывод
```

## Конфигурация ивентов (`config.yaml`)

```yaml
events:
  - name: "My Event"
    slug: "my-event-2026"
    enabled: true                # false => ивент полностью исключается из обработки
    telegram_enabled: true       # false => сбор + дашборд без отправки в Telegram
    currency_code: "EUR"         # код валюты для подписей и таблиц
    currency_symbol: "€"         # символ валюты для карточек/Telegram
    sales_target:              # опционально
      tickets: 1000
      revenue: 65000
    sources:
      - type: bilesu_serviss
        provider_name: "Biļešu Serviss"
        provider_link: "https://www.bilesuserviss.lv"
        event_page_url: "https://www.bilesuserviss.lv/..."
        params:
          event_id: "123456"
          legal_person_id: "4003"
          sale_start: "2026-05-01"
      - type: mticket
        provider_name: "mticket"
        provider_link: "https://www.mticket.eu"
        event_page_url: ""
        params:
          event_id: "789012"
```

Флаги `enabled` и `telegram_enabled` опциональны. По умолчанию оба считаются `true`.
Поля `currency_code` и `currency_symbol` тоже опциональны (по умолчанию `EUR` и `€`).

## Добавление нового источника

1. Создать `sources/my_source.py` с функцией `collect(params) -> list[dict]`
2. Добавить в `SOURCE_MODULES` в `collector.py`
3. Использовать `type: my_source` в `config.yaml`

## Cron

```bash
0 6 * * * cd /path/to/TicketBot && python3 collector.py >> logs/cron.log 2>&1
```
