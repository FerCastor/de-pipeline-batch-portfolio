# Traffic Accidents Data Pipeline

A complete batch data pipeline extracting Brazilian federal highway accident data from PRF (Polícia Rodoviária Federal) public API, processing through bronze-silver-gold layers, and visualizing insights in a Metabase dashboard.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   PRF API   │────▶│   Extract   │────▶│  Transform  │────▶│  Dashboard  │
│  (Source)   │     │   (Python)  │     │   (dbt)     │     │  (Metabase) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                   │
                           ▼                   ▼
                    ┌─────────────┐     ┌─────────────┐
                    │   Bronze    │────▶│   Silver    │
                    │ (Raw JSON)  │     │ (Cleaned)   │
                    └─────────────┘     └─────────────┘
                                                │
                                                ▼
                                          ┌─────────────┐
                                          │    Gold     │
                                          │ (Analytics) │
                                          └─────────────┘

Orchestration: Apache Airflow
Storage: PostgreSQL
```

## Tech Stack

- **Python 3.14** - Data extraction and API consumption
- **PostgreSQL 15** - Data warehouse with bronze/silver/gold schemas
- **dbt** - Data transformation and modeling
- **Apache Airflow 2.9** - Pipeline orchestration and scheduling
- **Metabase** - Data visualization and dashboards
- **Docker Compose** - Local development environment

## Data Layers

| Layer | Schema | Content | Purpose |
|-------|--------|---------|---------|
| Bronze | `bronze.*` | Raw JSON from API | Preserve source data, schema flexibility |
| Silver | `silver.*` | Cleaned, deduplicated, typed | Reliable historical data |
| Gold | `gold.*` | Business aggregates | Dashboard-ready analytics |

## Quick Start

1. **Clone and navigate:**
   ```bash
   cd de-pipeline-batch-portfolio
   ```

2. **Start services:**
   ```bash
   docker compose up -d
   ```

3. **Access services:**
   - Airflow: http://localhost:8080 (admin/admin)
   - Metabase: http://localhost:3000
   - PostgreSQL: localhost:5432

4. **Trigger pipeline:**
   In Airflow UI, enable and trigger `prf_accidents_pipeline` DAG.

## Project Structure

```
.
├── docker-compose.yml          # Infrastructure definition
├── .env                        # Environment variables
├── README.md                   # This file
├── init-scripts/               # PostgreSQL initialization
│   └── 01_create_schemas.sql
├── extract/                    # Data extraction code
│   └── prf_extractor.py
├── transform/                  # dbt transformations
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
├── orchestration/              # Airflow DAGs
│   └── dags/
│       └── prf_pipeline.py
├── dashboard/                  # Metabase configuration
├── docs/                       # Documentation
├── tests/                      # Unit and integration tests
└── data/                       # Local data storage (gitignored)
    ├── bronze/
    ├── silver/
    └── gold/
```

## Data Source

**Polícia Rodoviária Federal (PRF)** - Brazilian Federal Highway Police
- API: https://portal.prf.gov.br/dados-abertos
- Data: Traffic accidents on federal highways (BR-101, BR-116, etc.)
- Frequency: Monthly updates
- Format: CSV/JSON

## Analysis Questions

This pipeline enables analysis of:

- Which federal highways have the most accidents?
- What are the peak hours/days for accidents?
- What are the most common causes?
- How have accident patterns evolved over years?
- Which vehicle types are most involved?
- Weather and road condition correlations

## Development

### Running Tests
```bash
python -m pytest tests/
```

### Manual Extraction
```bash
cd extract && python prf_extractor.py
```

### dbt Commands
```bash
cd transform
dbt run        # Execute models
dbt test       # Run tests
dbt docs generate  # Generate documentation
```

## License

MIT
