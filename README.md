# MOEX Stock SQLite Project

Небольшой ETL/BI-проект для загрузки дневных баров MOEX в SQLite, подготовки витрин и анализа в SQL и Power BI.

## Цель проекта

Цель проекта - собрать понятный локальный пайплайн для акций MOEX, который:

- загружает рыночные данные в SQLite
- приводит их к нормальной аналитической структуре
- считает основные рыночные метрики
- даёт готовую базу для SQL-анализа и Power BI

## Что делает проект

В проекте три слоя данных:

- `raw_bars_daily` - сырые дневные OHLCV-данные
- `stg_instruments`, `stg_bars_daily` - staging-слой
- `mart_market_data`, `mart_market_metrics` - аналитические витрины

Источник данных - MOEX API. Загрузка выполняется скриптом [`load_moex_to_sqlite.py`](load_moex_to_sqlite.py).

## Структура проекта

- [`schema.sql`](schema.sql) - создание таблиц и индексов
- [`drop_all.sql`](drop_all.sql) - удаление всех таблиц
- [`raw.sql`](raw.sql) - проверки raw-слоя
- [`staging.sql`](staging.sql) - загрузка staging-слоя и проверки данных
- [`mart.sql`](mart.sql) - построение витрин и расчёт метрик
- [`analysis.sql`](analysis.sql) - примеры аналитических SQL-запросов
- [`load_moex_to_sqlite.py`](load_moex_to_sqlite.py) - загрузка данных из MOEX в raw-слой
- [`run_all.ps1`](run_all.ps1) - полный запуск пайплайна одной командой
- [`BI_stock.pbix`](BI_stock.pbix) - Power BI-дашборд
- `moex_stock.db` - SQLite-база проекта

## Архитектура данных

Проект построен по обычной схеме `raw -> staging -> mart`.

- `raw` - слой сырой загрузки
- `staging` - слой очистки и нормализации
- `mart` - слой готовых метрик для анализа и BI

Поток данных:

```text
MOEX API
  -> raw_bars_daily
  -> stg_instruments
  -> stg_bars_daily
  -> mart_market_data
  -> mart_market_metrics
  -> SQL analysis / Power BI dashboard
```

Назначение таблиц:

- `raw_bars_daily` хранит исходные дневные OHLCV-данные
- `stg_instruments` хранит справочник инструментов
- `stg_bars_daily` хранит котировки с привязкой к `instrument_id`
- `mart_market_data` хранит рыночные данные и `prev_close`
- `mart_market_metrics` хранит рассчитанные аналитические показатели

## Метрики в `mart_market_metrics`

В витрине считаются:

- `return` - дневная доходность
- `log_return` - логарифмическая доходность `ln(close / prev_close)`
- `volatility_20`, `volatility_50` - скользящая волатильность по дневной доходности за 20 и 50 дней
- `cumulative_return` - накопленная доходность от первой цены инструмента
- `ma_20`, `ma_50` - скользящие средние цены закрытия
- `avg_volume_20` - средний объём за 20 дней
- `volume_ratio_20` - отношение текущего объёма к среднему объёму за 20 дней
- `rolling_max_close` - накопительный максимум цены закрытия
- `drawdown` - просадка относительно исторического максимума

## Используемые технологии

- `Python 3` - загрузка данных
- `PowerShell` - запуск полного пайплайна
- `SQLite` - локальное хранилище данных
- `sqlite3.exe` - выполнение SQL-скриптов из командной строки
- `SQL` - построение слоёв и аналитических запросов
- `aiohttp` - HTTP-клиент для асинхронной загрузки данных
- `aiomoex` - получение рыночных данных с MOEX
- `pandas` - подготовка данных перед записью в SQLite
- `Power BI Desktop` - визуализация итоговых витрин

## Требования

- Windows PowerShell
- Python 3
- SQLite CLI (`sqlite3.exe` уже лежит в проекте)
- Python-пакеты:

```powershell
py -3 -m pip install aiohttp aiomoex pandas
```

Если `py` не работает, можно использовать:

```powershell
python -m pip install aiohttp aiomoex pandas
```

## Анализ требований

Что проект должен уметь:

- загружать дневные данные по одному или нескольким тикерам MOEX
- хранить данные в локальной SQLite-базе
- разделять данные на raw, staging и mart-слои
- считать рыночные метрики для анализа
- позволять пересчитывать витрины
- подключаться к Power BI без дополнительного сервера

Технические требования к решению:

- простой локальный запуск
- понятная структура SQL-файлов
- возможность полной пересборки проекта
- пригодность для ручной проверки данных
- совместимость с PowerShell, SQLite и Power BI Desktop

Как это закрыто в проекте:

- полная пересборка через [`run_all.ps1`](run_all.ps1)
- отдельные SQL-файлы по слоям и этапам обработки
- Python-загрузчик с параметрами по базе, тикерам и датам
- витрина `mart_market_metrics` с ключевыми ценовыми, объёмными и риск-метриками
- набор аналитических SQL-запросов в [`analysis.sql`](analysis.sql)

## Быстрый запуск

Полная сборка проекта одной командой:

```powershell
.\run_all.ps1
```

По умолчанию скрипт:

- создаёт новую или пересоздаёт существующую `moex_stock.db`
- удаляет старые таблицы через `drop_all.sql`
- выполняет `schema.sql`
- загружает данные MOEX по тикеру `GAZP` с `2010-01-01`
- заполняет staging и mart

Запуск с параметрами:

```powershell
.\run_all.ps1 -Database moex_stock.db -Tickers "GAZP,SBER,LKOH" -Start "2015-01-01" -End "2026-03-16"
```

Если PowerShell блокирует запуск скриптов:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_all.ps1 -Tickers "GAZP,SBER"
```

## Важно

`run_all.ps1` делает полную пересборку и начинает с [`drop_all.sql`](drop_all.sql). Если в базе были нужные данные, они будут удалены.

Если задача не в полной пересборке, а только в добавлении новых тикеров в уже существующую базу, `run_all.ps1` использовать не нужно.

Для добавления новых тикеров лучше такой порядок:

1. Запустить [`load_moex_to_sqlite.py`](load_moex_to_sqlite.py) с нужными тикерами
2. Выполнить [`staging.sql`](staging.sql)
3. Выполнить [`mart.sql`](mart.sql)

## Ручной запуск по шагам

1. Создать таблицы:

```powershell
.\sqlite3.exe moex_stock.db ".read schema.sql"
```

2. Загрузить raw-данные:

```powershell
py -3 .\load_moex_to_sqlite.py --db moex_stock.db --tickers "GAZP,SBER" --start "2015-01-01" --end "2026-03-16"
```

3. Построить staging:

```powershell
.\sqlite3.exe moex_stock.db ".read staging.sql"
```

4. Построить mart:

```powershell
.\sqlite3.exe moex_stock.db ".read mart.sql"
```

5. Выполнить анализ:

```powershell
.\sqlite3.exe moex_stock.db ".read analysis.sql"
```

## Параметры `load_moex_to_sqlite.py`

При запуске Python-скрипта можно задавать параметры вручную:

- `--db` - имя или путь к базе данных
- `--tickers` - тикеры через запятую, например `GAZP,SBER,LKOH`
- `--start` - дата начала загрузки в формате `YYYY-MM-DD`
- `--end` - дата конца загрузки в формате `YYYY-MM-DD`

Если `--end` не указан, скрипт берёт текущую дату.

Пример:

```powershell
py -3 .\load_moex_to_sqlite.py --db my_stock.db --tickers "GAZP,MGNT,SBER" --start "2020-01-01" --end "2026-03-16"
```

Если нужно просто добавить новые тикеры в уже существующую базу, после загрузки raw-данных нужно заново выполнить:

```powershell
.\sqlite3.exe moex_stock.db ".read staging.sql"
.\sqlite3.exe moex_stock.db ".read mart.sql"
```

## Проверки и анализ

- [`raw.sql`](raw.sql) проверяет дубли, пропуски и базовые аномалии в raw
- [`staging.sql`](staging.sql) загружает staging и содержит валидационные запросы
- [`mart.sql`](mart.sql) пересчитывает витрины
- [`analysis.sql`](analysis.sql) содержит готовые выборки по доходности, волатильности, просадкам и экстремальным движениям

## Дашборд Power BI

Файл [`BI_stock.pbix`](BI_stock.pbix) использует SQLite-базу проекта как источник данных.

В дашборде можно смотреть:

- динамику цены закрытия по датам
- дневную доходность `return`
- накопленную доходность `cumulative_return`
- волатильность `volatility_20` и `volatility_50`
- просадку `drawdown`
- всплески объёма через `volume_ratio_20`

После обновления базы:

1. Открой `BI_stock.pbix`
2. Нажми `Refresh`
3. Проверь, что Power BI подключён к актуальному файлу `moex_stock.db`

## Результат

В результате проект даёт локальную SQLite-базу с витринами для:

- SQL-анализа
- визуализаций в Power BI
- оценки доходности, волатильности, объёмов и просадок по тикерам MOEX
