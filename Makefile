# ---------- Variables ----------
INFRA_DIR	?= infra
AIRFLOW_CONTAINER	?= airflow
DBT_CONTAINER	?= dbt-runner
DBT_PORT	?= 8081
.DEFAULT_GOAL := help
# ---------- Targets ----------
.PHONY: help up down plan dbt-shell dbt-docs duck-db logs-airflow

help:
	@echo "Available targets:"
	@echo "  make up              -> terraform init + apply"
	@echo "  make down            -> terraform destroy"
	@echo "  make plan            -> terraform plan"
	@echo "  make dbt-shell       -> open bash shell in $(DBT_CONTAINER) container"
	@echo "  make dbt-docs        -> dbt docs generate + serve (port $(DBT_PORT))"
	@echo "  make duck-db         -> open duckdb shell to inspect /database/warehouse.duckdb"
	@echo "  make logs-airflow    -> tail -f logs from $(AIRFLOW_CONTAINER) container"

up:
	cd $(INFRA_DIR) && terraform init && terraform apply -auto-approve

down:
	cd $(INFRA_DIR) && terraform destroy -auto-approve

plan:
	cd $(INFRA_DIR) && terraform plan

dbt-shell:
	docker exec -it $(DBT_CONTAINER) bash	

dbt-docs:
	docker exec -it $(DBT_CONTAINER) dbt docs generate
	docker exec -it $(DBT_CONTAINER) dbt docs serve --host 0.0.0.0 --port $(DBT_PORT)

duck-db:
	docker exec -it $(DBT_CONTAINER) duckdb /database/warehouse.duckdb

logs-airflow:
	@echo ">> Logs Airflow:"
	-@docker logs -f $(AIRFLOW_CONTAINER)

