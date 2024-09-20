# Makefile para gerenciar o Docker Compose
ifeq ($(shell command -v docker-compose),)
  DOCKER_COMPOSE=docker compose
else
  DOCKER_COMPOSE=docker-compose
endif

# Nome do compose
COMPOSE_FILE = docker-compose.yaml

.PHONY: all airflow postgres metabase

all: postgres airflow metabase

postgres:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d postgres postgres_rasa

airflow:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d airflow-webserver airflow-scheduler

metabase:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d metabase postgres_rasa

stop-airflow:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop airflow-webserver airflow-scheduler

stop-postgres:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop postgres postgres_rasa

stop-metabase:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop metabase

stop:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop
	
status:
	$(DOCKER_COMPOSE) ps
