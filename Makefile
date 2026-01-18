.PHONY: help install run test docker_up docker_down

help:
	@echo "Available commands:"
	@echo "  make install      Install Python dependencies"
	@echo "  make run          Run the data pipeline"
	@echo "  make test         Run all tests"
	@echo "  make docker_up    Start Docker services"
	@echo "  make docker_down  Stop Docker services"

install: requirements.txt
	pip install -r requirements.txt

run:
	python src/pipeline.py

test:
	python -m pytest tests/

docker_up:
	docker compose up -d

docker_down:
	docker compose down
