PROJECT_NAME:=sw_minio_service
EXECUTER:=uv run
MINIO_DATA_DIR:=./.minio-data

all: format lint type security test

init:
	git init
	$(EXECUTER) uv sync
	$(EXECUTER) pre-commit install

clean:
	rm -rf .mypy_cache .pytest_cache .coverage htmlcov
	$(EXECUTER) ruff clean

format:
	$(EXECUTER) ruff format .

lint:
	$(EXECUTER) ruff check . --fix

test:
	$(EXECUTER) pytest --cov-report term-missing --cov-report html --cov $(PROJECT_NAME)/

type:
	$(EXECUTER) mypy .

security:
	$(EXECUTER) bandit -r $(PROJECT_NAME)/
	$(EXECUTER) pip-audit

# MinIO related commands
minio-start:
	mkdir -p $(MINIO_DATA_DIR)
	docker run -d \
		--name sw-minio \
		-p 9000:9000 \
		-p 9001:9001 \
		-v $(MINIO_DATA_DIR):/data \
		-e "MINIO_ROOT_USER=minioadmin" \
		-e "MINIO_ROOT_PASSWORD=minioadmin" \
		minio/minio server /data --console-address ":9001"
	@echo "MinIO started on port 9000 (API) and 9001 (Console)"
	@echo "Access credentials: minioadmin / minioadmin"
	@echo "Console URL: http://localhost:9001"

# MinIO related commands
minio-start-local:
	mkdir -p $(MINIO_DATA_DIR)
	docker run -d \
		--name sw-minio \
		-p 9000:9000 \
		-p 9001:9001 \
		-v $(MINIO_DATA_DIR):/data \
		minio/minio server /data --console-address ":9001"
	@echo "MinIO started on port 9000 (API) and 9001 (Console)"
	@echo "Access credentials: minioadmin / minioadmin"
	@echo "Console URL: http://localhost:9001"

minio-stop:
	docker stop sw-minio || true
	docker rm sw-minio || true



