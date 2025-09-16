.PHONY: clean-data run-dashboard install test

install:
	pip install -r requirements.txt

clean-data:
	rm -rf data/cleaned/*
	rm -f data/*.db

run-dashboard:
	streamlit run scripts/dashboard.py

test:
	pytest tests/

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/

all: install clean-data run-dashboard