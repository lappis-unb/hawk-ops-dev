# Makefile para gerenciar o Docker Compose

# Verificação criada para ver se o comando é <docker-compose> ou <docker compose>
ifeq ($(shell command -v docker-compose),)
  DOCKER_COMPOSE=docker compose
else
  DOCKER_COMPOSE=docker-compose
endif

# Nome do compose
COMPOSE_FILE = docker-compose.yaml

.PHONY: all airflow postgres metabase stop-airflow stop-postgres stop-metabase status clean clean-volumes

all: postgres airflow metabase

postgres:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d postgres_rasa

airflow:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d airflow-webserver airflow-scheduler

metabase:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d metabase

stop-airflow:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop airflow-webserver airflow-scheduler

stop-postgres:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop postgres_rasa

stop-metabase:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop metabase

stop:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop

clean-airflow:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down --remove-orphans

clean-postgres:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down --remove-orphans

clean-metabase:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down --remove-orphans

clean:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down --remove-orphans

clean-volumes:
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down --volumes --remove-orphans

status:
	$(DOCKER_COMPOSE) ps
