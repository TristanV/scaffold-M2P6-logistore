
# Makefile for the inventory management system
.PHONY: install fake_data
setup:
	@echo "Setting up the project..."
	$(MAKE) install
	$(MAKE) fake_data
	@echo "Setup complete."


install:
	@echo "Installing the dependencies..."
	python -m venv .venv && source .venv/bin/activate
	uv sync
	docker compose up -d # PostgreSQL + Airflow
	python scripts/load_to_postgres.py --init	
	@echo "Installation complete."

fake_data:
	@echo "Generating fake data..."
	python scripts/generate_flows.py --palier small # 200 produits, 10 000 mouvements
	python scripts/generate_flows.py --palier medium # 500 produits, 100 000 mouvements
	python scripts/generate_flows.py --palier large # 1 000 produits, 1 000 000 mouvements
	@echo "Fake data generation complete."