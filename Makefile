install: requirements.txt
	pip install -r requirements.txt

run:
	python src/pipeline.py

test:
	python tests/

	