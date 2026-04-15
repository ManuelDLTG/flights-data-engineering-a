import argparse
import logging
import sys
import os

os.environ["WR_RAY_ENABLED"] = "false"

import awswrangler as wr
import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

CTAS_SQL = """
CREATE TABLE flights_gold.vuelos_analitica AS (
SELECT
    f.year,
    f.month,
    f.day,
    f.origin_airport,
    ap_orig.airport AS origin_airport_name,
    ap_orig.city    AS origin_city,
    ap_orig.state   AS origin_state,
    f.destination_airport,
    ap_dest.airport AS destination_airport_name,
    al.airline      AS airline_name,
    f.departure_delay,
    f.arrival_delay,
    f.cancelled,
    f.cancellation_reason,
    f.distance,
    f.air_system_delay,
    f.airline_delay,
    f.weather_delay,
    f.late_aircraft_delay,
    f.security_delay
FROM flights_bronze.flights f
LEFT JOIN flights_bronze.airlines al
    ON f.airline = al.iata_code
LEFT JOIN flights_bronze.airports ap_orig
    ON f.origin_airport = ap_orig.iata_code
LEFT JOIN flights_bronze.airports ap_dest
    ON f.destination_airport = ap_dest.iata_code
)
"""

def create_database() -> None:
    try:
        wr.catalog.create_database("flights_gold", exist_ok=True)
        logger.info("Base de datos 'flights_gold' lista en Glue Catalog")
    except Exception:
        logger.exception("Error creando flights_gold en Glue")
        sys.exit(1)


def drop_table_if_exists() -> None:
    try:
        wr.catalog.delete_table_if_exists(database="flights_gold", table="vuelos_analitica")
        logger.info("Tabla vuelos_analitica eliminada (si existía)")
    except Exception:
        logger.exception("Error eliminando tabla existente")
        sys.exit(1)


def run_ctas(bucket: str) -> None:
    s3_output = f"s3://{bucket}/flights/gold/vuelos_analitica/"
    logger.info(f"Ejecutando CTAS → {s3_output}")
    try:
        wr.athena.start_query_execution(
            sql=CTAS_SQL,
            database="flights_gold",
            s3_output=s3_output,
            wait=True,
        )
        logger.info("  ✓ CTAS ejecutado correctamente")
    except Exception:
        logger.exception("Error ejecutando CTAS en Athena")
        sys.exit(1)


def verify(bucket: str) -> None:
    logger.info("Verificando resultado con SELECT LIMIT 5 ...")
    try:
        df = wr.athena.read_sql_query(
            sql="SELECT * FROM flights_gold.vuelos_analitica LIMIT 5",
            database="flights_gold",
            s3_output=f"s3://{bucket}/flights/gold/tmp/",
        )
        logger.info(f"  ✓ Verificación exitosa — {len(df)} filas de muestra")
        logger.info(f"\n{df.to_string()}")
    except Exception:
        logger.exception("Error en verificación")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Gold ETL — CTAS en Athena")
    parser.add_argument("--bucket", required=True, help="Nombre del bucket S3")
    args = parser.parse_args()

    logger.info("=== INICIO Gold ETL ===")
    create_database()
    drop_table_if_exists()
    run_ctas(args.bucket)
    verify(args.bucket)
    logger.info("=== FIN Gold ETL — éxito ===")


if __name__ == "__main__":
    main()
