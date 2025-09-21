# ---------- Variables ----------
INFRA_DIR	?= infra
AIRFLOW_CONTAINER	?= airflow
DBT_CONTAINER	?= dbt-runner
DBT_PORT	?= 8081   
.DEFAULT_GOAL := help
# ---------- Targets ----------
.PHONY: up down plan dbt-docs logs-airflow logs-dbt help

help:
	@echo "Alvos disponíveis:"
	@echo "  make up              -> terraform init + apply"
	@echo "  make down            -> terraform destroy"
	@echo "  make plan            -> terraform plan"
	@echo "  make dbt-docs        -> dbt docs generate + serve (porta $(DBT_PORT))"
	@echo "  make logs-airflow    -> tail -f dos logs do $(AIRFLOW_CONTAINER) container"
	@echo "  make logs-dbt        -> tail -f do container $(DBT_CONTAINER)"

up:
	cd $(INFRA_DIR) && terraform init && terraform apply -auto-approve

down:
	cd $(INFRA_DIR) && terraform destroy -auto-approve

plan:
	cd $(INFRA_DIR) && terraform plan

dbt-docs:
	docker exec -it $(DBT_CONTAINER) dbt docs generate
	docker exec -it $(DBT_CONTAINER) dbt docs serve --host 0.0.0.0 --port $(DBT_PORT)

logs-airflow:
	@echo ">> Logs Airflow:"
	-@docker logs -f $(AIRFLOW_CONTAINER)

logs-dbt:
	@echo ">> Logs dbt:"
	-@docker logs -f $(DBT_CONTAINER)