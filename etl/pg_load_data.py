import logging
import sys
import boto3
import json
import pandas as pd
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session
from pg_create_tables import Airline, Airport, Flight, Base

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_credentials(secret_id: str) -> dict:
    try:
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secret = client.get_secret_value(SecretId=secret_id)
        return json.loads(secret["SecretString"])
    except Exception:
        logger.exception("Error obteniendo credenciales")
        sys.exit(1)


def load_airlines(session: Session, data_dir: str) -> None:
    logger.info("Cargando airlines ...")
    df = pd.read_csv(f"{data_dir}/airlines.csv")
    df.columns = [c.strip().lower() for c in df.columns]
    records = df.rename(columns={"iata_code": "iata_code", "airline": "airline"}).to_dict(orient="records")
    session.execute(insert(Airline), records)
    session.commit()
    logger.info(f"  ✓ {len(records)} aerolíneas insertadas")


def load_airports(session: Session, data_dir: str) -> None:
    logger.info("Cargando airports ...")
    df = pd.read_csv(f"{data_dir}/airports.csv")
    df.columns = [c.strip().lower() for c in df.columns]
    records = df.to_dict(orient="records")
    session.execute(insert(Airport), records)
    session.commit()
    logger.info(f"  ✓ {len(records)} aeropuertos insertados")


def load_flights(session: Session, data_dir: str) -> None:
    logger.info("Cargando primeros 500,000 vuelos ...")
    df = pd.read_csv(f"{data_dir}/flights.csv", nrows=500_000)
    df.columns = [c.strip().lower() for c in df.columns]

    cols = [
        "year", "month", "day", "airline",
        "origin_airport", "destination_airport",
        "departure_delay", "arrival_delay", "cancelled",
        "cancellation_reason", "distance",
        "air_system_delay", "airline_delay", "weather_delay",
        "late_aircraft_delay", "security_delay",
    ]
    df = df[cols]

    # Reemplazar NaN con None para que PostgreSQL los reciba como NULL
    df = df.where(pd.notnull(df), None)

    # Insertar en batches de 10,000
    batch_size = 10_000
    total = len(df)
    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size].to_dict(orient="records")
        session.execute(insert(Flight), batch)
        session.commit()
        logger.info(f"  → {min(i+batch_size, total):,} / {total:,} filas insertadas")

    logger.info(f"  ✓ {total:,} vuelos insertados")


def main():
    creds = get_credentials("itam/rds/flights/credentials")

    host = "itam-flights-448591726855.ci7osqgw4ccz.us-east-1.rds.amazonaws.com"
    url  = (
        f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
        f"@{host}:{creds['port']}/{creds['dbname']}"
    )

    logger.info("Conectando a PostgreSQL primario ...")
    try:
        engine = create_engine(url, echo=False)
    except Exception:
        logger.exception("Error conectando a PostgreSQL")
        sys.exit(1)

    with Session(engine) as session:
        load_airlines(session, "data/flights")
        load_airports(session, "data/flights")
        load_flights(session, "data/flights")

    logger.info("=== Carga completa ===")


if __name__ == "__main__":
    main()
