.PHONY: help install run test docker-all

help:
	@echo "Available commands:"
	@echo "  make install       Install Python dependencies"
	@echo "  make run           Run the Sentinel Pipeline"
	@echo "  make test          Run all tests"
	@echo "  make docker-all    Run the Sentinel Pipeline inside Docker (full execution)"

install: requirements.txt
	python -m pip install --upgrade pip
	pip install -r requirements.txt

test:
	python -m pytest -v tests/

run:
	python -m src.pipeline

docker_test:
	docker compose run --rm pipeline python -m pytest -v tests/

docker_run:
	docker compose run --rm pipeline

docker_all: docker-test docker-run


clean:
	rm -rf data/bronze/*
	rm -rf data/silver/*
#	rm -rf data/gold/*

