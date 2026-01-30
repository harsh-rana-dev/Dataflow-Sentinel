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

run:
	python -m src.pipeline

test:
	python -m pytest -v tests/

docker-all:
	docker compose up -d --build
	- docker compose exec app make run
	- docker compose exec app make test
	docker compose down

clean:
	rm -rf data/bronze/*
	rm -rf data/silver/*
#	rm -rf data/gold/*

