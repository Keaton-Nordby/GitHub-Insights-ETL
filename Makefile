run:
	python src/main.py

install:
	pip install -r requirements.txt

test:
	pytest -q
