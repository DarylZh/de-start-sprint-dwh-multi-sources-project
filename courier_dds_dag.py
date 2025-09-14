# examples/dds/courier_dds_dag.py
import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dds", "courier"],
    is_paused_upon_creation=False,
)
def dds_courier_dag():
    """
    DDS загрузка из STG:
      1) dm_couriers (апсерт по натуральному ключу courier_id из API)
      2) fct_deliveries за прошлый месяц (order_id = SURROGATE dds.dm_orders.id, courier_id = SURROGATE dds.dm_couriers.id)
      3) update dm_orders.courier_id (ссылка на справочник курьеров)
    """
    dwh_pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def upsert_dm_couriers():
        sql = """
        INSERT INTO dds.dm_couriers (courier_id, courier_name)
        SELECT s.object_id, s.object_value->>'name'
        FROM stg.courier_api_couriers s
        ON CONFLICT (courier_id)
        DO UPDATE SET courier_name = EXCLUDED.courier_name;
        """
        with dwh_pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
        log.info("dm_couriers upsert done.")

    @task()
    def upsert_fct_deliveries_prev_month():
        month_start = (
            pendulum.now("UTC").subtract(months=1).start_of("month").format("YYYY-MM-DD HH:mm:ss")
        )
        month_end = pendulum.now("UTC").start_of("month").format("YYYY-MM-DD HH:mm:ss")

        sql = """
        INSERT INTO dds.fct_deliveries (
          delivery_key, order_id, courier_id, order_ts, delivery_ts, rate, tip_sum
        )
        SELECT
          s.delivery_id                                        AS delivery_key,
          o.id                                                 AS order_id,     -- SURROGATE из dds.dm_orders
          c.id                                                 AS courier_id,   -- SURROGATE из dds.dm_couriers
          s.order_ts::timestamptz                              AS order_ts,
          s.delivery_ts::timestamptz                           AS delivery_ts,
          NULLIF(s.rate,0)::int2                               AS rate,
          COALESCE(s.tip_sum,0)::numeric(14,2)                 AS tip_sum
        FROM stg.courier_api_deliveries s
        JOIN dds.dm_orders   o ON o.order_key  = s.order_id
        JOIN dds.dm_couriers c ON c.courier_id = s.courier_id
        WHERE s.order_ts >= %(ms)s::timestamptz AND s.order_ts < %(me)s::timestamptz
        ON CONFLICT (delivery_key) DO UPDATE SET
          order_id    = EXCLUDED.order_id,
          courier_id  = EXCLUDED.courier_id,
          order_ts    = EXCLUDED.order_ts,
          delivery_ts = EXCLUDED.delivery_ts,
          rate        = EXCLUDED.rate,
          tip_sum     = EXCLUDED.tip_sum;
        """
        with dwh_pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, {"ms": month_start, "me": month_end})
        log.info("fct_deliveries upsert (prev month) done.")

    @task()
    def update_orders_courier_id():
        sql = """
        UPDATE dds.dm_orders o
        SET courier_id = fd.courier_id
        FROM dds.fct_deliveries fd
        WHERE fd.order_id = o.id
          AND (o.courier_id IS DISTINCT FROM fd.courier_id);
        """
        with dwh_pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
        log.info("dm_orders.courier_id updated from fct_deliveries.")

    c = upsert_dm_couriers()
    f = upsert_fct_deliveries_prev_month()
    u = update_orders_courier_id()
    c >> f >> u

dds_courier = dds_courier_dag()
