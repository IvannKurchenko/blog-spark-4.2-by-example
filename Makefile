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

up:
	docker compose up

down:
	docker compose down

app-semantic-modeling:
	@rm -rf spark-warehouse
	@uv run app-semantic-modeling
