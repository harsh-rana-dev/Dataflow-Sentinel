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