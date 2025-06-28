.PHONY: help build up down init clean logs test run-local setup format lint venv jupyter

# Default target
help:
	@echo "📋 Available commands:"
	@echo "  make setup       - Setup initial environment"
	@echo "  make build       - Build Docker images"
	@echo "  make up          - Start all services"
	@echo "  make down        - Stop all services"
	@echo "  make init        - Initialize Airflow"
	@echo "  make clean       - Clean up data and volumes"
	@echo "  make logs        - Show logs"
	@echo "  make test        - Run tests"
	@echo "  make run-local   - Run pipeline locally"
	@echo "  make format      - Format code with black and isort"
	@echo "  make lint        - Run linting checks"
	@echo "  make venv        - Create virtual environment"
	@echo "  make jupyter     - Start Jupyter notebook locally"

# Setup initial environment
setup:
	@echo "🛠️ Setting up environment..."
	./scripts/setup.sh

# Build Docker images
build:
	@echo "🔨 Building Docker images..."
	docker-compose build

# Start services
up:
	@echo "🚀 Starting services..."
	docker-compose up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 10
	@echo "✅ Services started!"
	@echo "📊 Access points:"
	@echo "  - Airflow: http://localhost:8080 (admin/admin)"
	@echo "  - Spark: http://localhost:8081"
	@echo "  - Jupyter: http://localhost:8888 (token: easy)"

# Stop services
down:
	@echo "🛑 Stopping services..."
	docker-compose down

# Initialize Airflow
init: up
	@echo "🎯 Initializing Airflow..."
	docker-compose run --rm airflow-init
	@echo "✅ Airflow initialized!"

# Clean up
clean: down
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	rm -rf data/bronze/* data/silver/* data/gold/*
	@echo "✅ Cleanup complete!"

# Show logs
logs:
	docker-compose logs -f

# Run tests
test:
	@echo "🧪 Running tests..."
	python -m pytest tests/

# Run tests with coverage
test-cov:
	@echo "🧪 Running tests with coverage..."
	python -m pytest tests/ --cov=src/log_analyzer --cov-report=term --cov-report=html

# Run pipeline locally
run-local:
	@echo "🏃 Running pipeline localmente..."
	python -m log_analyzer.cli run

# Format code
format:
	@echo "✨ Formatting code..."
	isort src/ tests/
	black src/ tests/

# Lint code
lint:
	@echo "🔍 Linting code..."
	flake8 src/ tests/
	mypy src/

# Create virtual environment
venv:
	@echo "🐍 Creating virtual environment..."
	python -m venv venv
	@echo "Now run: source venv/bin/activate"
	@echo "Then: pip install -e '.[dev]'"

# Start Jupyter notebook locally
jupyter:
	@echo "📓 Starting Jupyter notebook..."
	jupyter notebook --notebook-dir=notebook/

# Quick start (build + init + show status)
quickstart: setup build init
	@echo "🎉 Log Analyzer is ready!"
	@echo "Visit http://localhost:8080 to access Airflow"
	@echo "Visit http://localhost:8888 to access Jupyter Notebook (token: easy)"