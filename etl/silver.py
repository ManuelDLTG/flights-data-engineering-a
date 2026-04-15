import argparse
import logging
import sys
import os

os.environ["WR_RAY_ENABLED"] = "false"

import awswrangler as wr
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_files(bucket: str) -> list:
    s3_path = f"s3://{bucket}/flights/bronze/flights/"
    files = wr.s3.list_objects(s3_path)
    logger.info(f"  → {len(files)} archivos en Bronze")
    return files


def build_aggregations(files: list) -> tuple:
    """Lee archivo por archivo y acumula agregaciones sin juntar todo en RAM."""
    daily_parts = []
    monthly_parts = []
    airport_parts = []

    for i, f in enumerate(files):
        logger.info(f"Procesando archivo {i+1}/{len(files)} ...")
        df = wr.s3.read_parquet(path=f, use_threads=False)
        df.columns = [c.upper() for c in df.columns]
        logger.info(f"  → {len(df):,} filas")

        df_active = df[df["CANCELLED"] == 0].copy()

        # --- flights_daily ---
        agg = df.groupby(["YEAR", "MONTH", "DAY"]).agg(
            total_flights=("AIRLINE", "count"),
            total_delayed=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            total_cancelled=("CANCELLED", "sum"),
        ).reset_index()

        avg = df_active.groupby(["YEAR", "MONTH", "DAY"]).agg(
            sum_dep_delay=("DEPARTURE_DELAY", lambda x: x[x > 0].sum()),
            cnt_dep_delay=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            sum_arr_delay=("ARRIVAL_DELAY", lambda x: x.dropna().sum()),
            cnt_arr_delay=("ARRIVAL_DELAY", lambda x: x.dropna().count()),
        ).reset_index()

        daily_chunk = agg.merge(avg, on=["YEAR", "MONTH", "DAY"], how="left")
        daily_parts.append(daily_chunk)

        # --- flights_monthly ---
        agg_m = df.groupby(["MONTH", "AIRLINE"]).agg(
            total_flights=("CANCELLED", "count"),
            total_delayed=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            total_cancelled=("CANCELLED", "sum"),
        ).reset_index()

        avg_m = df_active.groupby(["MONTH", "AIRLINE"]).agg(
            sum_arr_delay=("ARRIVAL_DELAY", lambda x: x.dropna().sum()),
            cnt_arr_delay=("ARRIVAL_DELAY", lambda x: x.dropna().count()),
            on_time_count=("ARRIVAL_DELAY", lambda x: (x <= 15).sum()),
            active_count=("ARRIVAL_DELAY", lambda x: x.dropna().count()),
        ).reset_index()

        monthly_chunk = agg_m.merge(avg_m, on=["MONTH", "AIRLINE"], how="left")
        monthly_parts.append(monthly_chunk)

        # --- flights_by_airport ---
        agg_a = df.groupby("ORIGIN_AIRPORT").agg(
            total_departures=("CANCELLED", "count"),
            total_delayed=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            total_cancelled=("CANCELLED", "sum"),
            sum_dep_delay=("DEPARTURE_DELAY", lambda x: x[x > 0].sum()),
            cnt_dep_delay=("DEPARTURE_DELAY", lambda x: (x > 0).sum()),
            total_weather_delay=("WEATHER_DELAY", lambda x: x.fillna(0).sum()),
            total_delay_minutes=("DEPARTURE_DELAY", lambda x: x[x > 0].sum()),
        ).reset_index()
        airport_parts.append(agg_a)

        del df, df_active
        logger.info(f"  ✓ archivo {i+1} procesado")

    return daily_parts, monthly_parts, airport_parts


def consolidate_daily(parts: list) -> pd.DataFrame:
    logger.info("Consolidando flights_daily ...")
    df = pd.concat(parts, ignore_index=True)
    agg = df.groupby(["YEAR", "MONTH", "DAY"]).agg(
        total_flights=("total_flights", "sum"),
        total_delayed=("total_delayed", "sum"),
        total_cancelled=("total_cancelled", "sum"),
        sum_dep_delay=("sum_dep_delay", "sum"),
        cnt_dep_delay=("cnt_dep_delay", "sum"),
        sum_arr_delay=("sum_arr_delay", "sum"),
        cnt_arr_delay=("cnt_arr_delay", "sum"),
    ).reset_index()
    agg["avg_departure_delay"] = agg["sum_dep_delay"] / agg["cnt_dep_delay"].replace(0, np.nan)
    agg["avg_arrival_delay"]   = agg["sum_arr_delay"] / agg["cnt_arr_delay"].replace(0, np.nan)
    agg = agg.drop(columns=["sum_dep_delay","cnt_dep_delay","sum_arr_delay","cnt_arr_delay"])
    logger.info(f"  → {len(agg):,} filas")
    return agg


def consolidate_monthly(parts: list) -> pd.DataFrame:
    logger.info("Consolidando flights_monthly ...")
    df = pd.concat(parts, ignore_index=True)
    agg = df.groupby(["MONTH", "AIRLINE"]).agg(
        total_flights=("total_flights", "sum"),
        total_delayed=("total_delayed", "sum"),
        total_cancelled=("total_cancelled", "sum"),
        sum_arr_delay=("sum_arr_delay", "sum"),
        cnt_arr_delay=("cnt_arr_delay", "sum"),
        on_time_count=("on_time_count", "sum"),
        active_count=("active_count", "sum"),
    ).reset_index()
    agg["avg_arrival_delay"] = agg["sum_arr_delay"] / agg["cnt_arr_delay"].replace(0, np.nan)
    agg["on_time_pct"]       = agg["on_time_count"] / agg["active_count"].replace(0, np.nan) * 100
    agg = agg.drop(columns=["sum_arr_delay","cnt_arr_delay","on_time_count","active_count"])
    logger.info(f"  → {len(agg):,} filas")
    return agg


def consolidate_airport(parts: list) -> pd.DataFrame:
    logger.info("Consolidando flights_by_airport ...")
    df = pd.concat(parts, ignore_index=True)
    agg = df.groupby("ORIGIN_AIRPORT").agg(
        total_departures=("total_departures", "sum"),
        total_delayed=("total_delayed", "sum"),
        total_cancelled=("total_cancelled", "sum"),
        sum_dep_delay=("sum_dep_delay", "sum"),
        cnt_dep_delay=("cnt_dep_delay", "sum"),
        total_weather_delay=("total_weather_delay", "sum"),
        total_delay_minutes=("total_delay_minutes", "sum"),
    ).reset_index()
    agg["avg_departure_delay"] = agg["sum_dep_delay"] / agg["cnt_dep_delay"].replace(0, np.nan)
    agg["pct_weather_delay"]   = agg["total_weather_delay"] / agg["total_delay_minutes"].replace(0, np.nan) * 100
    agg = agg.drop(columns=["sum_dep_delay","cnt_dep_delay","total_weather_delay","total_delay_minutes"])
    logger.info(f"  → {len(agg):,} filas")
    return agg


def validate(df: pd.DataFrame, name: str) -> None:
    try:
        assert not df.empty, f"{name} está vacío"
        logger.info(f"  ✓ {name} validado: {len(df):,} filas")
    except AssertionError:
        logger.exception(f"Validación fallida para {name}")
        sys.exit(1)


def write_table(df: pd.DataFrame, bucket: str, database: str,
                table: str, partition_cols: list = None) -> None:
    s3_path = f"s3://{bucket}/flights/silver/{table}/"
    logger.info(f"Escribiendo {table} → {s3_path}")
    try:
        kwargs = dict(
            df=df,
            path=s3_path,
            dataset=True,
            database=database,
            table=table,
            compression="snappy",
            use_threads=False,
        )
        if partition_cols:
            kwargs["partition_cols"] = partition_cols
            kwargs["mode"] = "overwrite_partitions"
        else:
            kwargs["mode"] = "overwrite"
        wr.s3.to_parquet(**kwargs)
        logger.info(f"  ✓ {table} escrito correctamente")
    except Exception:
        logger.exception(f"Error escribiendo {table}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Silver ETL — transformaciones y agregaciones")
    parser.add_argument("--bucket", required=True, help="Nombre del bucket S3")
    args = parser.parse_args()

    logger.info("=== INICIO Silver ETL ===")

    database = "flights_silver"
    try:
        wr.catalog.create_database(database, exist_ok=True)
        logger.info(f"Base de datos '{database}' lista en Glue Catalog")
    except Exception:
        logger.exception("Error creando base de datos en Glue")
        sys.exit(1)

    files = get_files(args.bucket)
    daily_parts, monthly_parts, airport_parts = build_aggregations(files)

    daily    = consolidate_daily(daily_parts)
    monthly  = consolidate_monthly(monthly_parts)
    airports = consolidate_airport(airport_parts)

    validate(daily,    "flights_daily")
    validate(monthly,  "flights_monthly")
    validate(airports, "flights_by_airport")

    write_table(daily,    args.bucket, database, "flights_daily", partition_cols=["MONTH"])
    write_table(monthly,  args.bucket, database, "flights_monthly")
    write_table(airports, args.bucket, database, "flights_by_airport")

    logger.info("=== FIN Silver ETL — éxito ===")


if __name__ == "__main__":
    main()