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

.PHONY: app-remote-spark
app-remote-spark:
	@uv run app-remote-spark

app-semantic-modeling:
	@rm -rf spark-warehouse
	@uv run app-semantic-modeling

app-vector-function:
	@rm -rf spark-warehouse
	@uv run python -m src.demo.vector_functions

app-geo-functions:
	@rm -rf spark-warehouse
	@uv run python -m src.demo.geo_functions
