# Система мониторинга качества данных в потоковом режиме

Программный прототип универсальной системы мониторинга Data Quality на базе Kappa-архитектуры.

## Технологический стек:
- **Транспорт:** Apache Kafka
- **Хранилище:** PostgreSQL (JSONB)
- **Трансформация:** dbt (Data Build Tool)
- **Backend:** FastAPI
- **Frontend:** Bootstrap 5 + Chart.js
- **Инфраструктура:** Docker Compose

## Как запустить:
1. Поднять инфраструктуру: `docker-compose up -d`
2. Запустить загрузчик: `python ingestion.py`
3. Запустить генератор: `python producer.py`
4. Выполнить трансформации dbt: `dbt run --profiles-dir .`
5. Запустить дашборд: `uvicorn api:app --reload`