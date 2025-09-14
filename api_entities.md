API Entities → DWH (для витрины выплат курьерам)
Цель витрины

Построить cdm.dm_courier_ledger — ежемесячный отчёт по выплатам курьерам:

courier_id, courier_name

settlement_year, settlement_month

orders_count

orders_total_sum

rate_avg

order_processing_fee = orders_total_sum * 0.25

courier_order_sum — сумма по заказам (зависит от рейтинга)

courier_tips_sum

courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95

Группировка и отбор по дате заказа, учитываются только заказы со статусом CLOSED.

1) Поля, нужные для витрины
Показатель	Откуда берём	Поля-источники
courier_id	DDS	dds.dm_couriers.id (surrogate)
courier_name	DDS	dds.dm_couriers.courier_name
settlement_year / month	DDS	из dds.dm_timestamps.ts заказа
orders_count	DDS	количество заказов курьера в месяце (по dds.fct_deliveries + dds.dm_orders)
orders_total_sum	DDS	сумма из dds.fct_product_sales.total_sum агрегированная по заказу и далее по курьеру
rate_avg	DDS	средний rate из dds.fct_deliveries за месяц по курьеру
order_processing_fee	Расчёт	orders_total_sum * 0.25
courier_order_sum	Расчёт	для каждого заказа: max(order_total * pct, min_per_order); параметры из средней оценки курьера в месяце
courier_tips_sum	DDS	сумма tip_sum из dds.fct_deliveries
courier_reward_sum	Расчёт	courier_order_sum + courier_tips_sum * 0.95

Шкала выплат по рейтингу (r — среднемесячный рейтинг курьера):

r < 4 → 5% от заказа, минимум 100₽

4 ≤ r < 4.5 → 7% от заказа, минимум 150₽

4.5 ≤ r < 4.9 → 8% от заказа, минимум 175₽

r ≥ 4.9 → 10% от заказа, минимум 200₽

2) Таблицы DDS, откуда берём поля
Таблица DDS	Нужные поля	Статус
dds.dm_orders	id (surrogate), order_key, timestamp_id, order_status, courier_id	есть (колонка courier_id — добавляется при интеграции)
dds.dm_timestamps	id, ts	есть
dds.fct_product_sales	order_id, total_sum (и др.)	есть
dds.dm_couriers	id (surrogate), courier_id (натуральный из API), courier_name	новая
dds.fct_deliveries	delivery_key (натуральный из API), order_id (FK → dm_orders), courier_id (FK → dm_couriers), order_ts, delivery_ts, rate, tip_sum	новая

orders_total_sum считаем из dds.fct_product_sales, а не из поля sum API-доставок (оно нам не нужно).

3) Что грузим из API и какие поля
3.1. Couriers (GET /couriers)

Нужно: _id, name

Использование:
_id → dds.dm_couriers.courier_id (натуральный ключ)
name → dds.dm_couriers.courier_name

3.2. Deliveries (GET /deliveries)

Нужно: delivery_id, order_id, courier_id, order_ts, delivery_ts, rate, tip_sum, address (не используется в витрине)

Использование:
delivery_id → dds.fct_deliveries.delivery_key (натуральный)
order_id → мэппинг на dds.dm_orders.order_key → dds.fct_deliveries.order_id (surrogate)
courier_id → мэппинг на dds.dm_couriers.courier_id → dds.fct_deliveries.courier_id (surrogate)
order_ts, delivery_ts, rate, tip_sum → прямой перенос в факт

В ответах API поле sum присутствует, но не используется, т.к. сумму заказа берём из подсистемы заказов → dds.fct_product_sales.

3.3. Restaurants (GET /restaurants)

Нужно? Для данной витрины — необязательно. Можно грузить «на будущее» для консистентности, но расчёты не используют.

4) STG-слой (сырые данные из API)

Отражаем JSON as is + ключевые поля для идемпотентных upsert’ов и инкрементальности.

Таблицы STG

stg.courier_api_couriers

object_id text pk ← _id

object_value jsonb ← полный JSON

load_ts timestamptz

stg.courier_api_deliveries

delivery_id text pk ← delivery_id

order_id text ← order_id

courier_id text ← courier_id

order_ts timestamptz ← order_ts

delivery_ts timestamptz ← delivery_ts

rate int ← rate

tip_sum numeric(14,2) ← tip_sum (0, если null)

object_value jsonb ← полный JSON

load_ts timestamptz

stg.courier_api_restaurants (опционально)

object_id text pk ← _id

object_value jsonb

load_ts timestamptz

Инкрементальность и идемпотентность

/couriers, /restaurants: постраничная загрузка, upsert по _id (естественный ключ).

/deliveries: окно последние 7 дней + сортировка и пагинация, upsert по delivery_id.

При повторах: данные перезаписываются по натуральному ключу (идемпотентно).

5) Мэппинг STG → DDS

stg.courier_api_couriers.object_id → dds.dm_couriers.courier_id (натуральный)
dds.dm_couriers.id (surrogate) генерится при вставке.

stg.courier_api_deliveries.delivery_id → dds.fct_deliveries.delivery_key
stg…order_id → join на dds.dm_orders.order_key → берём dds.dm_orders.id в dds.fct_deliveries.order_id
stg…courier_id → join на dds.dm_couriers.courier_id → берём dds.dm_couriers.id в dds.fct_deliveries.courier_id
Прочие поля (order_ts, delivery_ts, rate, tip_sum) — прямой перенос.

Обратная связь: dds.dm_orders.courier_id наполняем из dds.fct_deliveries:

UPDATE dds.dm_orders o
SET courier_id = d.courier_id
FROM dds.fct_deliveries d
WHERE d.order_id = o.id
  AND (o.courier_id IS DISTINCT FROM d.courier_id);

6) Расчёт витрины (DDS → CDM)

Заказы берём по дате заказа: dds.dm_orders.timestamp_id → dds.dm_timestamps.ts::date.
Отбираем месяц (предыдущий) и только o.order_status = 'CLOSED'.

Сумму заказа получаем так:

order_total = SUM(fps.total_sum)   -- fps = dds.fct_product_sales
GROUP BY order_id


База для агрегации:

JOIN dds.fct_deliveries d       -- содержит courier_id (surrogate), rate, tip_sum
JOIN dds.dm_orders o
JOIN dds.dm_timestamps t
JOIN order_totals ot


rate_avg — AVG(rate) за месяц по курьеру.

courier_order_sum — сумма по всем закрытым заказам:

SUM( GREATEST(order_total * pct, min_per_order) )


где pct и min_per_order по шкале рейтинга (см. выше).

order_processing_fee = orders_total_sum * 0.25
courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95

UPSERT в cdm.dm_courier_ledger по (courier_id, settlement_year, settlement_month).

7) Ключи, сортировки, пагинация в API

Всегда используем limit и offset (до 50).

Для стабильности страниц — сортировка:

/couriers: sort_field=id, sort_direction=asc

/deliveries: sort_field=_id, sort_direction=asc, плюс фильтры from, to (окно последних 7 дней)

8) Контроль качества и допущения

В dds.fct_deliveries возможны «висящие» доставки, если соответствующий order_key ещё не в dds.dm_orders → их обрабатываем при следующем прогоне (UPSERT).

tip_sum может быть null → приводим к 0.

Поле sum из /deliveries игнорируем — источник истины по сумме заказа: dds.fct_product_sales.

Идемпотентность обеспечивается ON CONFLICT по натуральным ключам (_id/delivery_id) и по композитному ключу в витрине.

9) Краткая зависимость слоёв
STG: courier_api_couriers, courier_api_deliveries ( + опц. restaurants )
   → DDS: dm_couriers, fct_deliveries (+ update dm_orders.courier_id)
      → join с уже существующими dm_orders, dm_timestamps, fct_product_sales
         → CDM: dm_courier_ledger (ежедневный пересчёт предыдущего месяца)