# examples/cdm/courier_ledger_dag.py
import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["cdm", "courier", "ledger"],
    is_paused_upon_creation=False,
)
def cdm_courier_ledger_dag():
    """
    Ежедневно пересчитываем ПОЗАПРОШЛЫЙ месяц? (требование — прошлый)
    Здесь считаем ИМЕННО предыдущий месяц относительно сегодняшней даты.
    """
    dwh_pg = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def upsert_previous_month():
        sql = """
        WITH m AS (
          SELECT
            date_trunc('month', (now() AT TIME ZONE 'utc')::date - interval '1 month')::date AS month_start,
            date_trunc('month', (now() AT TIME ZONE 'utc')::date)::date                   AS month_end
        ),
        order_totals AS (
          -- сумма заказа = сумма позиций
          SELECT order_id, SUM(total_sum) AS order_total
          FROM dds.fct_product_sales
          GROUP BY order_id
        ),
        base AS (
          SELECT
            d.courier_id,       -- surrogate в dm_couriers
            o.id AS order_id,   -- surrogate заказа
            t.ts::date AS order_date,
            ot.order_total,
            d.rate,
            COALESCE(d.tip_sum,0) AS tip_sum
          FROM dds.fct_deliveries d
          JOIN dds.dm_orders     o  ON o.id = d.order_id
          JOIN dds.dm_timestamps t  ON t.id = o.timestamp_id
          JOIN order_totals      ot ON ot.order_id = o.id
          JOIN m ON t.ts::date >= m.month_start AND t.ts::date < m.month_end
          WHERE o.order_status = 'CLOSED'
        ),
        rate_by_courier AS (
          SELECT courier_id, ROUND(AVG(rate)::numeric, 2) AS rate_avg
          FROM base
          GROUP BY courier_id
        ),
        params AS (
          SELECT
            r.courier_id,
            r.rate_avg,
            CASE
              WHEN r.rate_avg < 4      THEN 0.05
              WHEN r.rate_avg < 4.5    THEN 0.07
              WHEN r.rate_avg < 4.9    THEN 0.08
              ELSE                          0.10
            END AS pct,
            CASE
              WHEN r.rate_avg < 4      THEN 100
              WHEN r.rate_avg < 4.5    THEN 150
              WHEN r.rate_avg < 4.9    THEN 175
              ELSE                          200
            END AS min_per_order
          FROM rate_by_courier r
        ),
        agg AS (
          SELECT
            b.courier_id,
            COUNT(*)                              AS orders_count,
            ROUND(SUM(b.order_total)::numeric, 2) AS orders_total_sum,
            ROUND(SUM(b.tip_sum)::numeric, 2)     AS courier_tips_sum,
            -- сумма курьеру: max(%, минимум) по каждому заказу
            ROUND(SUM(GREATEST(b.order_total * p.pct, p.min_per_order))::numeric, 2) AS courier_order_sum
          FROM base b
          JOIN params p ON p.courier_id = b.courier_id
          GROUP BY b.courier_id
        ),
        final AS (
          SELECT
            a.courier_id,
            dc.courier_name,
            EXTRACT(YEAR  FROM (SELECT month_start FROM m))::int2 AS settlement_year,
            EXTRACT(MONTH FROM (SELECT month_start FROM m))::int2 AS settlement_month,
            a.orders_count,
            a.orders_total_sum,
            r.rate_avg,
            ROUND(a.orders_total_sum * 0.25, 2)                   AS order_processing_fee,
            a.courier_order_sum,
            a.courier_tips_sum,
            ROUND(a.courier_order_sum + a.courier_tips_sum * 0.95, 2) AS courier_reward_sum
          FROM agg a
          JOIN rate_by_courier r ON r.courier_id = a.courier_id
          JOIN dds.dm_couriers dc ON dc.id = a.courier_id
        )
        INSERT INTO cdm.dm_courier_ledger (
          courier_id, courier_name, settlement_year, settlement_month,
          orders_count, orders_total_sum, rate_avg,
          order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum
        )
        SELECT
          courier_id, courier_name, settlement_year, settlement_month,
          orders_count, orders_total_sum, rate_avg,
          order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum
        FROM final
        ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
        SET
          courier_name         = EXCLUDED.courier_name,
          orders_count         = EXCLUDED.orders_count,
          orders_total_sum     = EXCLUDED.orders_total_sum,
          rate_avg             = EXCLUDED.rate_avg,
          order_processing_fee = EXCLUDED.order_processing_fee,
          courier_order_sum    = EXCLUDED.courier_order_sum,
          courier_tips_sum     = EXCLUDED.courier_tips_sum,
          courier_reward_sum   = EXCLUDED.courier_reward_sum;
        """
        with dwh_pg.connection() as conn, conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
        log.info("cdm.dm_courier_ledger upserted for previous month.")

    upsert_previous_month()

cdm_courier_ledger = cdm_courier_ledger_dag()
