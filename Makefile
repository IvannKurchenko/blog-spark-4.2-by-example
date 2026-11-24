.PHONY: setup install format check clean clean-spark docker-compose-up down \
        demo-remote-spark demo-semantic-modeling demo-vector-function demo-geo-functions \
        demo-declarative-pipelines-scd

setup:
	uv venv

install:
	@uv lock
	@uv sync

format:
	@black src/

check:
	@ruff check src/

clean:
	rm -rf .venv uv.lock __pycache__

clean-spark:
	rm -rf data metastore_db spark-warehouse

docker-compose-up:
	docker compose up

down:
	docker compose down

demo-remote-spark:
	@uv run app-remote-spark

demo-semantic-modeling:
	@rm -rf spark-warehouse
	@uv run app-semantic-modeling

demo-vector-function:
	@rm -rf spark-warehouse
	@uv run app-vector-function

demo-geo-functions:
	@rm -rf spark-warehouse
	@uv run app-geo-functions

demo-declarative-pipelines-scd:
	@rm -rf spark-warehouse
	@uv run app-create-warehouses
	@spark-pipelines run --conf spark.sql.catalogImplementation=hive --spec src/demo/declaratime_pipelines_sdc/spark-pipelines.yaml
	@uv run app-update-warehouses
	@spark-pipelines run --conf spark.sql.catalogImplementation=hive --spec src/demo/declaratime_pipelines_sdc/spark-pipelines.yaml
