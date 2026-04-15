# Flights Data Engineering — 2015 US Domestic Flights
 
Pipeline de datos end-to-end sobre el dataset de vuelos domésticos de EE.UU. en 2015 (5.8 millones de vuelos). Construido sobre AWS con arquitectura Medallion en S3 + Athena, modelo relacional en PostgreSQL, y análisis estadístico con regresión OLS y pronóstico de series de tiempo.
 
---
 
## Estructura del repositorio
 
```
flights-data-engineering-a/
├── etl/
│   ├── bronze.py              # Ingesta de CSVs a S3 + Glue Catalog
│   ├── silver.py              # Transformaciones y agregaciones en Parquet
│   ├── gold.py                # CTAS en Athena — tabla analítica desnormalizada
│   ├── pg_create_tables.py    # Definición del modelo relacional con SQLAlchemy
│   └── pg_load_data.py        # Carga de datos a PostgreSQL
├── notebooks/
│   └── flights_analytics.ipynb  # Análisis exploratorio, regresión y pronóstico
├── docs/
│   ├── erd-flights.drawio     # Diagrama ERD editable
│   ├── erd-flights.png        # Diagrama ERD exportado
│   └── screenshots/           # Evidencia de ejecución
├── infra/
│   └── rds-flights.yaml       # Template CloudFormation para RDS PostgreSQL
└── data/                      # (en .gitignore — no se sube a GitHub)
```
 
---
 
## Dataset
 
| Archivo | Filas | Descripción |
|---------|-------|-------------|
| `flights.csv` | ~5.8M | Un registro por vuelo: fechas, aerolínea, origen, destino, demoras, cancelaciones |
| `airlines.csv` | 14 | Catálogo de aerolíneas: código IATA y nombre completo |
| `airports.csv` | 322 | Catálogo de aeropuertos: código IATA, nombre, ciudad, estado, lat/lon |
 
### Descarga
 
```bash
aws s3 cp s3://itam-analytics-dante/flights-hwk/flights.zip . --no-sign-request
unzip flights.zip -d data/
```
 
---
 
## Arquitectura Medallion
 
```
CSV locales
    │
    ▼
Bronze (S3 + Glue)          → flights_bronze.{flights, airlines, airports}
    │
    ▼
Silver (Parquet + Snappy)   → flights_silver.{flights_daily, flights_monthly, flights_by_airport}
    │
    ▼
Gold (CTAS Athena)          → flights_gold.vuelos_analitica
```
 
---
 
## Cómo ejecutar el ETL
 
Desde el terminal de SageMaker Jupyter Lab, en orden:
 
```bash
# 1. Bronze — sube CSVs a S3 y registra en Glue
python etl/bronze.py --bucket <tu-bucket> --data-dir data/flights
 
# 2. Silver — transforma a Parquet y construye agregaciones
python etl/silver.py --bucket <tu-bucket>
 
# 3. Gold — CTAS en Athena para tabla analítica desnormalizada
python etl/gold.py --bucket <tu-bucket>
 
# 4. PostgreSQL — crea tablas y carga datos
python etl/pg_create_tables.py
python etl/pg_load_data.py
```
 
### Buenas prácticas implementadas en todos los scripts
 
- `logging` con timestamp, nivel y mensaje (nunca `print`)
- `argparse` para parámetros externos (bucket, rutas)
- `try/except` con `logger.exception()` y `sys.exit(1)` en errores críticos
- Validaciones con `assert` antes de escribir a S3
- Idempotencia con `mode="overwrite"` y `delete_table_if_exists()`
- Estructura en funciones con bloque `if __name__ == "__main__"`
---
 
## 2.2 Bronze — `etl/bronze.py`
 
Lee los tres CSVs en chunks de 500,000 filas y los escribe directamente a S3 en formato Parquet sin transformaciones, registrándolos en el Glue Data Catalog bajo la base de datos `flights_bronze`.
 
| Tabla | Ruta S3 | Filas |
|-------|---------|-------|
| `flights` | `s3://<bucket>/flights/bronze/flights/` | ~5.8M |
| `airlines` | `s3://<bucket>/flights/bronze/airlines/` | 14 |
| `airports` | `s3://<bucket>/flights/bronze/airports/` | 322 |
 
### Evidencia — Glue `flights_bronze`
 
![Bronze Glue](docs/screenshots/1.png)
 
---
 
## 2.3 Silver — `etl/silver.py`
 
Lee los archivos Parquet de Bronze archivo por archivo (sin cargar todo en RAM), calcula agregaciones parciales por chunk y las consolida al final. Escribe en Parquet + Snappy bajo `flights_silver`.
 
| Tabla | Descripción | Partición |
|-------|-------------|-----------|
| `flights_daily` | Total vuelos, cancelados, retrasados y promedios por día | `MONTH` |
| `flights_monthly` | Métricas por mes y aerolínea, incluyendo `on_time_pct` | — |
| `flights_by_airport` | Salidas, retrasos y `pct_weather_delay` por aeropuerto | — |
 
### Evidencia — Glue `flights_silver`
 
![Silver Glue](docs/screenshots/2.png)
![Silver schema flights_by_airport](docs/screenshots/3.png)
 
---
 
## 2.4 Gold — `etl/gold.py`
 
Ejecuta un CTAS en Athena que desnormaliza `flights_bronze.flights` con los catálogos de aerolíneas y aeropuertos, produciendo la tabla analítica `flights_gold.vuelos_analitica` lista para consumo.
 
```sql
CREATE TABLE flights_gold.vuelos_analitica AS (
  SELECT f.*, al.airline AS airline_name,
         ap_orig.airport AS origin_airport_name, ...
  FROM flights_bronze.flights f
  LEFT JOIN flights_bronze.airlines al ON f.airline = al.iata_code
  LEFT JOIN flights_bronze.airports ap_orig ON f.origin_airport = ap_orig.iata_code
  LEFT JOIN flights_bronze.airports ap_dest ON f.destination_airport = ap_dest.iata_code
)
```
 
### Evidencia — Gold ETL + Athena SELECT LIMIT 5
 
![Gold terminal](docs/screenshots/4.png)
![Gold Glue](docs/screenshots/5.png)
![Gold Athena SELECT](docs/screenshots/6.png)
 
---
 
## 5. PostgreSQL con CloudFormation
 
### 5.1 Infraestructura
 
Stack provisionado con CloudFormation (`infra/rds-flights.yaml`) que incluye instancia RDS `db.t3.micro`, Read Replica, subnet group, security group y credenciales en Secrets Manager.
 
| Output | Endpoint |
|--------|----------|
| Primario | `itam-flights-448591726855.ci7osqgw4ccz.us-east-1.rds.amazonaws.com` |
| Read Replica | `itam-flights-replica-448591726855.ci7osqgw4ccz.us-east-1.rds.amazonaws.com` |
 
> Destruir el stack al terminar: `aws cloudformation delete-stack --stack-name itam-flights-rds`
 
### Evidencia — CloudFormation `CREATE_COMPLETE`
 
![CloudFormation](docs/screenshots/7.png)
 
---
 
### 5.2 Diagrama ERD
 
Tres entidades con sus atributos, PKs y FKs. `flights` tiene dos FK hacia `airports`: una por `origin_airport` y otra por `destination_airport`.
 
![ERD draw.io](docs/erd-flights.png)
 
---
 
### 5.3 y 5.4 — Tablas y datos en PostgreSQL
 
Modelo definido con SQLAlchemy 2.0. Carga en orden de dependencias FK: `airlines` → `airports` → `flights` (primeros 500,000 registros en batches de 10,000).
 
### Evidencia — DBeaver conexión y tablas
 
![DBeaver conexión](docs/screenshots/9.png)
![DBeaver árbol](docs/screenshots/10.png)
![DBeaver tablas con conteos](docs/screenshots/11.png)
![DBeaver diagrama relacional](docs/screenshots/12.png)
 
---
 
## 6. Queries SQL en DBeaver
 
Todas las queries se ejecutaron sobre la **Read Replica** para no afectar el rendimiento de escritura.
 
### P1 — Top 10 rutas con más vuelos
![P1](docs/screenshots/13.png)
 
### P2 — Top 5 aerolíneas con mayor % de cancelaciones
![P2](docs/screenshots/14.png)
 
### P3 — Cancelaciones por causa
![P3](docs/screenshots/15.png)
 
### P4 — Retraso promedio de salida por mes
![P4](docs/screenshots/16.png)
 
### P5 — Top 10 aeropuertos con más minutos de retraso por clima
![P5](docs/screenshots/17.png)
 
### W1 — Vuelo con mayor retraso de llegada por aerolínea (RANK)
![W1](docs/screenshots/18.png)
 
### W2 — Variación mes a mes en total de vuelos (LAG)
Respondida en el notebook con `flights_silver.flights_monthly` desde Athena (la carga de 500K filas en PostgreSQL cubre solo enero–febrero).
 
### W3 — Primeros 5 vuelos en LAX el 2015-01-01 (ROW_NUMBER)
> Nota: `scheduled_departure` no fue incluida en la carga a PostgreSQL. Se usó `departure_delay ASC` como proxy de ordenamiento.
 
![W3](docs/screenshots/19.png)
 
---
 
## 7. Notebook de análisis — `notebooks/flights_analytics.ipynb`
 
Replica las 8 queries (P1–P5 y W1–W3) como DataFrames de pandas usando SQLAlchemy + `pd.read_sql` sobre la Read Replica. W2 usa `awswrangler` sobre Athena Silver. Incluye al menos una visualización por pregunta con `matplotlib`.
 
---
 
## 8. Análisis Estadístico
 
### 8.1 Regresión lineal — ¿Qué factores explican el retraso de llegada?
 
Modelo OLS con `statsmodels` sobre ~1M vuelos leídos desde `flights_gold.vuelos_analitica`.
 
- **Variable objetivo:** `arrival_delay`
- **Features:** `departure_delay`, `distance`, `air_system_delay`, `airline_delay`, `weather_delay`, `late_aircraft_delay`, `security_delay`
| Métrica | Valor |
|---------|-------|
| R² | 1.0000 |
| RMSE | 0.00 minutos |
 
> El R² = 1 se explica porque los cinco componentes de retraso (`air_system_delay`, `airline_delay`, `weather_delay`, `late_aircraft_delay`, `security_delay`) suman matemáticamente el retraso total — es multicolinealidad perfecta por diseño del dataset.
 
### Evidencia — OLS Summary y gráficas
 
![OLS Summary](docs/screenshots/20.png)
![R² y RMSE](docs/screenshots/21.png)
![Residuos vs Predichos](docs/screenshots/22.png)
![Coeficientes con IC 95%](docs/screenshots/23.png)
 
**Interpretación:**
 
1. R² = 1.0 y RMSE ≈ 0 confirman que los cinco componentes de retraso son una descomposición exacta de `arrival_delay`, no features independientes.
2. Los coeficientes de los cinco componentes son todos ≈ 1.0, como es matemáticamente esperado dado que suman el total.
3. `departure_delay` y `distance` tienen coeficientes ≈ 0, absorbidos por los componentes.
4. La gráfica de residuos vs predichos muestra heterocedasticidad en escala de `1e-11` — errores de precisión numérica, no varianza real.
5. El número de condición de 2,030 confirma multicolinealidad severa.
---
 
### 8.2 Pronóstico de series de tiempo — StatsForecast
 
Tres modelos automáticos (`AutoETS`, `AutoARIMA`, `AutoTheta`) sobre la demanda mensual de vuelos en 2015, con split train (ene–sep) / test (oct–dic) y pronóstico de 6 meses hacia adelante (ene–jun 2016).
 
Con solo 9 puntos de entrenamiento, los tres modelos seleccionan especificaciones **no estacionales** — insuficientes datos para estimar un ciclo completo de 12 meses.
 
| Modelo | MAE (test set) |
|--------|----------------|
| AutoETS | ver notebook |
| AutoARIMA | ver notebook |
| AutoTheta | ver notebook |
 
---
 
## Stack tecnológico
 
| Capa | Tecnología |
|------|-----------|
| Almacenamiento | AWS S3 |
| Catálogo | AWS Glue Data Catalog |
| Query engine | AWS Athena |
| Base de datos | AWS RDS PostgreSQL 17.4 |
| Infraestructura | AWS CloudFormation |
| ETL | Python, awswrangler, pandas |
| ORM | SQLAlchemy 2.0 |
| Análisis | statsmodels, StatsForecast (Nixtla) |
| Visualización | matplotlib |
| Entorno | AWS SageMaker Jupyter Lab |
 
---
 
## Autor
 
**Manuel De La Tejera** — ITAM — Maestría en Ciencia de Datos  
Tarea 08: Data Engineering End-to-End