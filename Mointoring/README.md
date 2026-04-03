# FastAPI SQL Monitoring Stack

A production-ready FastAPI application integrated with PostgreSQL, monitored by Prometheus and Grafana.

## Architecture

This project uses a modern monitoring stack to track both application and database performance.

```mermaid
graph TD
    User([User / Client]) -->|HTTP Requests| FastAPI[FastAPI App]
    FastAPI -->|Write/Read Data| Postgres[(PostgreSQL)]
    
    subgraph Monitoring Stack
        Prometheus[Prometheus Server]
        Grafana[Grafana Dashboard]
        PGExporter[Postgres Exporter]
        
        Prometheus -->|Scrape /metrics| FastAPI
        Prometheus -->|Scrape /metrics| PGExporter
        PGExporter -->|Fetch DB Stats| Postgres
        Grafana -->|Query Metrics| Prometheus
    end
    
    User -->|View Dashboards| Grafana
```
## Monitoring

![alt text](img/fastapi.png)

![alt text](img/node-exporter.png)

## Quick Start
To start all services, run:
```bash
docker-compose up -d
```

## Service URLs
- **FastAPI Application**: [http://localhost:8000](http://localhost:8000)
- **FastAPI Metrics**: [http://localhost:8000/metrics](http://localhost:8000/metrics)
- **Prometheus Dashboard**: [http://localhost:9090](http://localhost:9090)
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (Default login: `admin` / `admin`)
