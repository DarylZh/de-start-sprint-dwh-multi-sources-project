# examples/stg/courier_api_dag.py
import logging
import requests
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from lib import ConnectionBuilder

# ВАЖНО для jsonb в psycopg3:
from psycopg.types.json import Json

log = logging.getLogger(__name__)

API_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
PAGE_LIMIT = 50
REQ_TIMEOUT = 30  # seconds


def _headers() -> dict:
    return {
        "X-Nickname": Variable.get("COURIER_API_NICKNAME"),
        "X-Cohort": Variable.get("COURIER_API_COHORT"),
        "X-API-KEY": Variable.get("COURIER_API_KEY"),
    }


def _paged_get(path: str, params: dict):
    """Итеративно получает страницы (limit/offset) и yield'ит элементы списка."""
    limit = params.get("limit", PAGE_LIMIT)
    offset = 0
    while True:
        q = dict(params)
        q["limit"] = limit
        q["offset"] = offset
        url = f"{API_URL}{path}"

        r = requests.get(url, headers=_headers(), params=q, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        # Ответы — списки. На всякий случай поддержим вариант с ключами.
        if not isinstance(data, list):
            data = (
                data.get("restaurants")
                or data.get("couriers")
                or data.get("deliveries")
                or data
            )

        yielded = 0
        for item in data:
            yielded += 1
            yield item

        if yielded < limit:
            break
        offset += limit


@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["stg", "courier_api"],
    is_paused_upon_creation=False,
)
def stg_courier_api_dag():
    """Грузим /restaurants, /couriers, /deliveries (за прошлый месяц) в STG."""

    dwh_pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def ensure_tables():
        """Опционально создаём STG-таблицы, если их ещё нет."""
        ddl = """
        CREATE SCHEMA IF NOT EXISTS stg;

        CREATE TABLE IF NOT EXISTS stg.courier_api_restaurants (
          object_id    text PRIMARY KEY,
          object_value jsonb NOT NULL,
          load_ts      timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS stg.courier_api_couriers (
          object_id    text PRIMARY KEY,
          object_value jsonb NOT NULL,
          load_ts      timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS stg.courier_api_deliveries (
          delivery_id   text PRIMARY KEY,
          order_id      text NOT NULL,
          courier_id    text NOT NULL,
          restaurant_id text,
          order_ts      timestamptz,
          delivery_ts   timestamptz,
          rate          int2,
          tip_sum       numeric(14,2),
          object_value  jsonb NOT NULL,
          load_ts       timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS courier_api_deliveries_order_id_idx
          ON stg.courier_api_deliveries(order_id);
        CREATE INDEX IF NOT EXISTS courier_api_deliveries_courier_id_idx
          ON stg.courier_api_deliveries(courier_id);
        """
        with dwh_pg.connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)

    @task()
    def load_restaurants():
        with dwh_pg.connection() as conn, conn.cursor() as cur:
            rows = 0
            for row in _paged_get(
                "/restaurants",
                # для /restaurants допустимы sort_field=id|name; используем id
                {"sort_field": "id", "sort_direction": "asc", "limit": PAGE_LIMIT},
            ):
                object_id = row.get("_id")
                if not object_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO stg.courier_api_restaurants(object_id, object_value)
                    VALUES (%s, %s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value,
                        load_ts     = now();
                    """,
                    (object_id, Json(row)),
                )
                rows += 1
            log.info("Restaurants upserted: %s", rows)

    @task()
    def load_couriers():
        with dwh_pg.connection() as conn, conn.cursor() as cur:
            rows = 0
            for row in _paged_get(
                "/couriers",
                # для /couriers аналогично используем sort_field=id
                {"sort_field": "id", "sort_direction": "asc", "limit": PAGE_LIMIT},
            ):
                object_id = row.get("_id")
                if not object_id:
                    continue
                cur.execute(
                    """
                    INSERT INTO stg.courier_api_couriers(object_id, object_value)
                    VALUES (%s, %s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value,
                        load_ts     = now();
                    """,
                    (object_id, Json(row)),
                )
                rows += 1
            log.info("Couriers upserted: %s", rows)

    @task()
    def load_deliveries_prev_month():
        # окно ровно прошлый месяц в UTC
        month_start = (
            pendulum.now("UTC").subtract(months=1).start_of("month").format("YYYY-MM-DD HH:mm:ss")
        )
        month_end = pendulum.now("UTC").start_of("month").format("YYYY-MM-DD HH:mm:ss")

        with dwh_pg.connection() as conn, conn.cursor() as cur:
            rows = 0
            for row in _paged_get(
                "/deliveries",
                {
                    "from": month_start,
                    "to": month_end,
                    # согласно спецификации /deliveries: sort_field=_id|date
                    "sort_field": "_id",
                    "sort_direction": "asc",
                    "limit": PAGE_LIMIT,
                },
            ):
                # Поля из API
                delivery_id = row.get("delivery_id")
                order_id = row.get("order_id")
                courier_id = row.get("courier_id")
                restaurant_id = row.get("restaurant_id")
                order_ts = row.get("order_ts")
                delivery_ts = row.get("delivery_ts")
                rate = row.get("rate")
                tip_sum = row.get("tip_sum", 0)

                if not delivery_id or not order_id or not courier_id:
                    # пропускаем сломанные записи
                    continue

                cur.execute(
                    """
                    INSERT INTO stg.courier_api_deliveries(
                      delivery_id, order_id, courier_id, restaurant_id,
                      order_ts, delivery_ts, rate, tip_sum, object_value
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (delivery_id) DO UPDATE SET
                      order_id      = EXCLUDED.order_id,
                      courier_id    = EXCLUDED.courier_id,
                      restaurant_id = EXCLUDED.restaurant_id,
                      order_ts      = EXCLUDED.order_ts,
                      delivery_ts   = EXCLUDED.delivery_ts,
                      rate          = EXCLUDED.rate,
                      tip_sum       = EXCLUDED.tip_sum,
                      object_value  = EXCLUDED.object_value,
                      load_ts       = now();
                    """,
                    (
                        delivery_id,
                        order_id,
                        courier_id,
                        restaurant_id,
                        order_ts,
                        delivery_ts,
                        rate,
                        tip_sum,
                        Json(row),
                    ),
                )
                rows += 1
            log.info("Deliveries upserted (prev month): %s", rows)

    t0 = ensure_tables()
    t1 = load_restaurants()
    t2 = load_couriers()
    t3 = load_deliveries_prev_month()

    t0 >> [t1, t2] >> t3


stg_courier_api = stg_courier_api_dag()

