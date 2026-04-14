import logging
import sys
import boto3
import json
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

Base = declarative_base()


class Airline(Base):
    __tablename__ = "airlines"
    iata_code = Column(String(10), primary_key=True)
    airline   = Column(String(100), nullable=False)
    flights   = relationship("Flight", back_populates="airline_rel")


class Airport(Base):
    __tablename__ = "airports"
    iata_code  = Column(String(10), primary_key=True)
    airport    = Column(String(200))
    city       = Column(String(100))
    state      = Column(String(50))
    country    = Column(String(50))
    latitude   = Column(Float)
    longitude  = Column(Float)


class Flight(Base):
    __tablename__ = "flights"
    id                   = Column(Integer, primary_key=True, autoincrement=True)
    year                 = Column(Integer)
    month                = Column(Integer)
    day                  = Column(Integer)
    airline              = Column(String(10), ForeignKey("airlines.iata_code"))
    origin_airport       = Column(String(10), ForeignKey("airports.iata_code"))
    destination_airport  = Column(String(10), ForeignKey("airports.iata_code"))
    departure_delay      = Column(Float)
    arrival_delay        = Column(Float)
    cancelled            = Column(Integer)
    cancellation_reason  = Column(String(1))
    distance             = Column(Float)
    air_system_delay     = Column(Float)
    airline_delay        = Column(Float)
    weather_delay        = Column(Float)
    late_aircraft_delay  = Column(Float)
    security_delay       = Column(Float)
    airline_rel          = relationship("Airline", back_populates="flights")


def get_credentials(secret_id: str) -> dict:
    logger.info(f"Obteniendo credenciales desde Secrets Manager: {secret_id}")
    try:
        client = boto3.client("secretsmanager", region_name="us-east-1")
        secret = client.get_secret_value(SecretId=secret_id)
        return json.loads(secret["SecretString"])
    except Exception:
        logger.exception("Error obteniendo credenciales")
        sys.exit(1)


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
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        logger.info("✓ Tablas creadas: airlines, airports, flights")
    except Exception:
        logger.exception("Error creando tablas")
        sys.exit(1)


if __name__ == "__main__":
    main()
