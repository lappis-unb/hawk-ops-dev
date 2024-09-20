# Makefile para gerenciar o Docker Compose

# Nome do compose
COMPOSE_FILE = docker-compose.yaml

.PHONY: all airflow postgres metabase

all: postgres airflow metabase

postgres:
	docker compose -f $(COMPOSE_FILE) up -d postgres

airflow:
	docker compose -f $(COMPOSE_FILE) up -d airflow-webserver airflow-scheduler

metabase:
	docker compose -f $(COMPOSE_FILE) up -d metabase
