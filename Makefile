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
	@echo "Subindo serviços postgres_rasa"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d postgres_rasa

airflow:
	@echo "Subindo serviços airflow"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d airflow-webserver airflow-scheduler

metabase:
	@echo "Subindo serviços metabase"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d metabase

stop-airflow:
	@echo "Pausando serviços airflow"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop airflow-webserver airflow-scheduler

stop-postgres:
	@echo "Pausando serviços postgres_rasa"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop postgres_rasa

stop-metabase:
	@echo "Pausando serviços metabase"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop metabase

stop:
	@echo "Pausando serviços"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop

clean-airflow:
	@echo "Limpando serviços airflow"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop airflow-webserver airflow-scheduler
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down airflow-webserver airflow-scheduler --remove-orphans

clean-postgres:
	@echo "Limpando serviços postgres"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop postgres_rasa
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down postgres_rasa --remove-orphans

clean-metabase:
	@echo "Pausando serviços postgres_rasa..."
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop postgres_rasa
	@echo "Limpando serviços metabase..."
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down metabase --remove-orphans
	@echo "Subindo novamente serviços postgres_rasa..."
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up -d postgres_rasa

clean:
	@echo "Pausando e limpando serviços"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) stop
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down --remove-orphans

clean-volumes:
	@echo "Pausando e limpando serviços"
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down --volumes --remove-orphans

status:
	$(DOCKER_COMPOSE) ps

psql:
	@echo "Entrando no container do banco de dados e iniciando o psql"
	docker exec -it hawk-ops-dev_postgres_rasa_1 /bin/bash -c "psql -U rasa -d dbrasa"