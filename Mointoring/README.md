# FastAPI SQL Monitoring Stack

A production-ready FastAPI application integrated with PostgreSQL and PgBouncer, monitored by Prometheus, Grafana, Node Exporter, and PostgreSQL Exporter.

## Architecture

This project uses a modern monitoring stack to track both application, system, and database performance.

```mermaid
graph TD
    User([User / Client]) -->|HTTP Requests| FastAPI[FastAPI App]
    FastAPI -->|Write/Read Data| PgBouncer{PgBouncer}
    PgBouncer -->|Connection Pool| Postgres[(PostgreSQL)]
    
    subgraph Monitoring Stack
        Prometheus[Prometheus Server]
        Grafana[Grafana Dashboard]
        PGExporter[Postgres Exporter]
        NodeExporter[Node Exporter]
        
        Prometheus -->|Scrape /metrics| FastAPI
        Prometheus -->|Scrape /metrics| PGExporter
        Prometheus -->|Scrape /metrics| NodeExporter
        PGExporter -->|Fetch DB Stats| Postgres
        Grafana -->|Query Metrics| Prometheus
    end
    
    User -->|View Dashboards| Grafana
```

## Included Services

- **FastAPI**: The main web application serving endpoints.
- **PostgreSQL**: The relational database for application data.
- **PgBouncer**: Lightweight connection pooler for PostgreSQL.
- **Prometheus**: Time-series database for scraping and storing metrics.
- **Grafana**: Visualization platform for the monitored metrics.
- **Node Exporter**: Hardware and OS metrics exporter for the host machine.
- **Postgres Exporter**: Exporter for detailed PostgreSQL metrics.

## Quick Start

To start all services, run:
```bash
docker-compose up -d
```

## Service URLs

- **FastAPI Application**: [http://localhost:8000](http://localhost:8000)
- **FastAPI Metrics**: [http://localhost:8000/metrics](http://localhost:8000/metrics)
- **PgBouncer**: `localhost:6432`
- **Prometheus Dashboard**: [http://localhost:9090](http://localhost:9090)
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (Default login: `admin` / `admin`)

## Monitoring Dashboards

### FastAPI Monitoring
![FastAPI Dashboard](img/fastapi.png)

### Node Exporter Monitoring
![Node Exporter Dashboard](img/node-exporter.png)
