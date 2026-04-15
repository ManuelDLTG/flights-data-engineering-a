import argparse
import logging
import sys
import awswrangler as wr
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_small(name: str, path: str, bucket: str, database: str) -> None:
    logger.info(f"Leyendo {path} ...")
    try:
        df = pd.read_csv(path)
        logger.info(f"  → {name}: {len(df):,} filas")
        assert not df.empty, f"{name} está vacío"
    except AssertionError:
        logger.exception(f"Validación fallida para {name}")
        sys.exit(1)
    except Exception:
        logger.exception(f"Error leyendo {path}")
        sys.exit(1)

    s3_path = f"s3://{bucket}/flights/bronze/{name}/"
    logger.info(f"Escribiendo {name} → {s3_path}")
    try:
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            database=database,
            table=name,
            mode="overwrite",
        )
        logger.info(f"  ✓ {name}: {len(df):,} filas escritas")
    except Exception:
        logger.exception(f"Error escribiendo {name} a S3")
        sys.exit(1)


def load_flights_chunked(path: str, bucket: str, database: str) -> None:
    s3_path = f"s3://{bucket}/flights/bronze/flights/"
    logger.info(f"Leyendo {path} en chunks y escribiendo → {s3_path}")

    total = 0
    try:
        for i, chunk in enumerate(pd.read_csv(path, chunksize=500_000)):
            assert not chunk.empty, f"chunk {i+1} vacío"
            mode = "overwrite" if i == 0 else "append"
            wr.s3.to_parquet(
                df=chunk,
                path=s3_path,
                dataset=True,
                database=database,
                table="flights",
                mode=mode,
            )
            total += len(chunk)
            logger.info(f"  → chunk {i+1} escrito: {len(chunk):,} filas | total acumulado: {total:,}")
    except Exception:
        logger.exception("Error procesando flights")
        sys.exit(1)

    logger.info(f"  ✓ flights completo: {total:,} filas escritas")


def main():
    parser = argparse.ArgumentParser(description="Bronze ETL — sube CSVs a S3 y Glue")
    parser.add_argument("--bucket",   required=True, help="Nombre del bucket S3")
    parser.add_argument("--data-dir", required=True, help="Directorio local con los CSVs")
    args = parser.parse_args()

    logger.info("=== INICIO Bronze ETL ===")

    database = "flights_bronze"
    try:
        wr.catalog.create_database(database, exist_ok=True)
        logger.info(f"Base de datos '{database}' lista en Glue Catalog")
    except Exception:
        logger.exception("Error creando la base de datos en Glue")
        sys.exit(1)

    load_small("airlines", f"{args.data_dir}/airlines.csv", args.bucket, database)
    load_small("airports", f"{args.data_dir}/airports.csv", args.bucket, database)
    load_flights_chunked(f"{args.data_dir}/flights.csv", args.bucket, database)

    logger.info("=== FIN Bronze ETL — éxito ===")


if __name__ == "__main__":
    main()
