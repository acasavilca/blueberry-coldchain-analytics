# ColdChain Analytics 🧊
### Physics-Based Cold Chain Monitoring Pipeline for Peruvian Agroindustrial Facilities

[![DE Zoomcamp 2025](https://img.shields.io/badge/DE%20Zoomcamp-2025-blue)](https://github.com/DataTalksClub/data-engineering-zoomcamp)
[![GCP](https://img.shields.io/badge/Cloud-GCP-orange)](https://cloud.google.com)
[![dbt](https://img.shields.io/badge/Transform-dbt-red)](https://www.getdbt.com)
[![Kestra](https://img.shields.io/badge/Orchestration-Kestra-purple)](https://kestra.io)

---

## Dashboard
https://datastudio.google.com/reporting/da3ffe2c-fe19-4375-9059-93ef6eb5df9a

---

## Problem Statement

Post-harvest cold chain failures are a leading cause of produce loss in Peruvian agroindustrial export operations. Blueberry and avocado packinghouses operate refrigeration systems around the clock during harvest season, yet most facilities have no systematic way to monitor energy efficiency or equipment performance over time.

This project builds an end-to-end data pipeline that ingests real ambient weather data from Open-Meteo and generates physics-based cold storage telemetry from a packinghouse simulator. The data is processed through BigQuery and dbt, and delivered as a Looker Studio dashboard showing refrigeration efficiency and throughput across harvest seasons for two fruit types.

---

## Architecture

```
Open-Meteo API (real, hourly weather data)
        │
        ▼
Physics Simulator (Docker container, runs via Kestra)
  ├── Cold storage zone heat balance
  ├── Stochastic door opening events (Gosney & Olama model)
  ├── Batch arrivals / FIFO dispatch
  └── Sensor noise + lag model
        │
        ▼ Parquet files (telemetry / events / batches)
GCS Data Lake (raw/telemetry/monthly/, raw/events/monthly/, etc.)
        │
        ▼ BigQuery External Tables (via Kestra bq_setup flow)
BigQuery Raw Dataset
        │
        ▼ dbt (staging → intermediate → marts)
BigQuery Processed Dataset
        │
        ▼
Looker Studio Dashboard
```

---

## Tech Stack

| Layer | Tool |
|-------|------|
| **IaC** | Terraform |
| **Cloud** | GCP (GCS, BigQuery) |
| **Weather data** | Open-Meteo Archive & Forecast API |
| **Orchestration** | Kestra (self-hosted on GCP VM) |
| **Transformation** | dbt (BigQuery adapter) |
| **Dashboard** | Looker Studio |
| **Simulator** | Python + Numba (JIT-compiled physics loop) |
| **Containerization** | Docker |

---

## Simulator

The simulator generates realistic cold chain telemetry seeded by real hourly weather data for La Libertad, Peru (-8.58°N, -78.57°W).

### Heat balance
```
dT_room/dt = (Q_walls + Q_roof + Q_floor + Q_fans +
              Q_fruit_exchange + Q_infiltration -
              Q_cooling - Q_humidifier) / C_room
```

### Physics implemented
- Sol-air roof temperature (ASHRAE Ch. 18)
- Gosney & Olama buoyancy-driven door infiltration
- Two-node fruit thermal model (T_pulp decoupled from C_room)
- COP derived from Carnot basis, degraded by isentropic efficiency η
- Dynamic humidity balance with coil bypass factor model
- Fruit transpiration (vapor pressure deficit driven)
- Stochastic batch arrivals (Gamma-distributed masses, FIFO dispatch)
- Sensor noise and first-order lag filters

### Outputs (10-second resolution, Parquet)
- `telemetry` — T_room, T_pulp, RH_room, CO2_ppm, O2_pct, W_compressor_kw, cooling_call, humidifier_call, evaporator temperatures
- `events` — batch arrivals and dispatches with timestamps, mass, quality grade
- `batches` — batch lifecycle summary (arrival → dispatch, weight loss)

### Fruit types supported
| Fruit | Setpoint | Peak season (Peru) |
|-------|----------|-------------------|
| Blueberry | 0°C | Sep–Nov |
| Avocado | 5°C | May–Aug |

### Data sources
| Parameter | Source |
|-----------|--------|
| T_ambient, RH, wind, solar, dewpoint | Open-Meteo Archive API |
| Soil temperature | Open-Meteo Archive API |
| Respiration rates | USDA Agriculture Handbook 66 (AH-66) |
| Specific heat | UW-Madison Extension |
| U-values, COP, infiltration | ASHRAE Handbook of Refrigeration 2018 |

---

## Pipeline

### Kestra flows

| Flow | Purpose |
|------|---------|
| `kestra_gen_kv` | Sets GCP project/bucket/dataset KV pairs |
| `generate_initial_simulation_state` | Initializes simulation state in KV store |
| `bq_setup` | Creates BigQuery external tables pointing to GCS |
| `backfill_monthly` | Runs one month of simulation + uploads to GCS |
| `backfill` | Loops `backfill_monthly` over all months (2022-01 → 2026-04) |
| `backfill_monthly_schedule` | Monthly scheduled trigger for ongoing data generation |

### Simulation state
Simulation state (`next_start`, `next_batch_id`, `active_batches`) is persisted in Kestra KV store between runs as `simulation_state_{fruit_type}`, enabling continuous stateful simulation across monthly executions.

### BigQuery schema

**Raw dataset** — external tables over GCS Parquet files:
- `telemetry_ext`, `events_ext`, `batches_ext`, `satellite_ext`

**Processed dataset** — dbt-managed tables:
- `staging/` — type casting and basic validation
- `intermediate/` — hourly/daily aggregations, COP computation, batch summaries
- `marts/` — `mart_energy_overview`, `mart_batch_quality`

**Partitioning:** staging tables partitioned by `datetime` (timestamp, day granularity), clustered by `fruit_type`.

---

## Dashboard

Built in Looker Studio connecting to `mart_energy_overview` and `mart_batch_quality`.

- **Daily Compressor Energy (kWh)** — seasonal energy patterns for blueberry vs avocado
- **Daily Mean Sensible COP** — efficiency trends over time by fruit type
- **Monthly Throughput (kg)** — seasonal volume by fruit type, filterable by year
- **Quality Grade Distribution** — pie chart of A/B/C batch grades, filterable by fruit type

---

## Reproducing the Project

### Prerequisites
- GCP account with billing enabled
- Terraform ≥ 1.5
- Docker
- Python 3.11+
- Kestra (self-hosted on a GCP VM or locally via Docker Compose)

### 1. Infrastructure
```bash
cd terraform/
terraform init
terraform apply
# Creates: GCS bucket, BigQuery datasets (raw_dataset, processed_dataset)
```

### 2. Configure Kestra
```bash
# Add GCP service account JSON to Kestra secrets as GCP_SERVICE_ACCOUNT
# Run kestra_gen_kv flow to set GCP_PROJECT_ID, GCP_BUCKET_NAME, GCP_DATASET, GCP_LOCATION
# Run generate_initial_simulation_state to initialize KV state for each fruit
```

### 3. Build Docker image
```bash
docker build -t blueberry_simulator:v001 .
```

### 4. Create BigQuery external tables
```bash
# Run bq_setup flow in Kestra
```

### 5. Run backfill
```bash
# Run backfill flow in Kestra
# Select fruit_type = blueberry, then repeat for avocado
# Each run processes 51 months (2022-01 → 2026-04) sequentially
```

### 6. Run dbt
```bash
cd blueberry_dbt/
dbt run
```

### 7. Dashboard
Connect Looker Studio to BigQuery `processed_dataset`, use `mart_energy_overview` and `mart_batch_quality`.

---

## Repository Structure

```
blueberry-coldchain-analytics/
├── terraform/                      # GCP infrastructure (excluded from repo)
├── app/
│   ├── simulator.py                # Physics engine (Numba JIT)
│   ├── run_one_chunk.py            # State management helpers
│   ├── config.py                   # Parameters and FRUIT_CONFIGS
│   └── respiration_data.py         # USDA AH-66 respiration tables
├── flows/
│   ├── kestra_gen_kv.yaml
│   ├── generate_initial_simulation_state.yaml
│   ├── bq_setup.yaml
│   ├── backfill_monthly.yaml
│   ├── backfill.yaml
│   └── backfill_monthly_schedule.yaml
├── blueberry_dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── dbt_project.yml
├── Dockerfile
└── README.md
```

---

## Facility Parameters

| Parameter | Blueberry | Avocado | Source |
|-----------|-----------|---------|--------|
| Setpoint | 0°C | 5°C | ASHRAE Refrigeration 2018 |
| Target RH | 95% | 90% | ASHRAE Refrigeration 2018 |
| Cp fruit | 3,640 J/kg·°C | 3,010 J/kg·°C | UW-Madison Extension / USDA |
| Respiration at setpoint | 6 mg CO₂/kg/hr | 35 mg CO₂/kg/hr | USDA AH-66 |
| U-wall | 0.225 W/m²·°C | 0.225 W/m²·°C | ASHRAE Refrigeration 2018 |
| Q_rated | 22 kW | 22 kW | ASHRAE: ~12 kW/1000m³ |
| COP efficiency η | 0.55 ± 0.02 | 0.55 ± 0.02 | ASHRAE Ch. 2 |
| Location | La Libertad, Peru (-8.58, -78.57) | same | — |

---

## Notes on Synthetic Data

This project uses a physics-based simulator as its data source rather than real proprietary sensor data. The simulator is seeded by real Open-Meteo weather data for the actual facility coordinates. All physics equations have primary source citations, and the pipeline architecture is identical to what would be built with real IoT sensors.

---

*Built for the DataTalks.Club Data Engineering Zoomcamp 2026 capstone project.*
