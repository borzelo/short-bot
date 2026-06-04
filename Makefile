.PHONY: build run test clean docker-build docker-run help

# Build the bot
build:
	@echo "Building bot..."
	@go build -o bot cmd/bot/main.go
	@echo "✅ Build complete: ./bot"

# Run the bot locally
run:
	@echo "Running bot..."
	@go run cmd/bot/main.go

# Run tests
test:
	@echo "Running tests..."
	@go test -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f bot
	@go clean
	@echo "✅ Clean complete"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy
	@echo "✅ Dependencies updated"

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	@docker build -t millionaire-bot:latest .
	@echo "✅ Docker image built: millionaire-bot:latest"

# Run Docker container locally
docker-run:
	@echo "Running Docker container..."
	@docker run --rm --env-file .env millionaire-bot:latest

# Start local PostgreSQL for development
postgres:
	@echo "Starting PostgreSQL container..."
	@docker run --name millionaire-postgres \
		-e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=millionaire_bot \
		-p 5432:5432 \
		-d postgres:15
	@echo "✅ PostgreSQL started on localhost:5432"

# Stop local PostgreSQL
postgres-stop:
	@echo "Stopping PostgreSQL container..."
	@docker stop millionaire-postgres || true
	@docker rm millionaire-postgres || true
	@echo "✅ PostgreSQL stopped"

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@echo "✅ Code formatted"

# Lint code
lint:
	@echo "Linting code..."
	@golangci-lint run || echo "⚠️  golangci-lint not installed, run: brew install golangci-lint"

# Show help
help:
	@echo "Available commands:"
	@echo "  make build          - Build the bot binary"
	@echo "  make run            - Run the bot locally"
	@echo "  make test           - Run tests"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make deps           - Download and tidy dependencies"
	@echo "  make docker-build   - Build Docker image"
	@echo "  make docker-run     - Run Docker container"
	@echo "  make postgres       - Start local PostgreSQL"
	@echo "  make postgres-stop  - Stop local PostgreSQL"
	@echo "  make fmt            - Format code"
	@echo "  make lint           - Lint code"
	@echo ""
	@echo "Import historical data:"
	@echo "  make import-build   - Build import tool"
	@echo "  make import-data    - Import historical CSV data"
	@echo "  make import-debug   - Import with debug logging"
	@echo "  make full-setup     - Start DB + Import data + Start bot"
	@echo ""
	@echo "Database commands:"
	@echo "  make db-shell       - Open psql shell"
	@echo "  make db-clear       - Clear training_data table"
	@echo "  make db-stats       - Show database statistics"
	@echo "  make help           - Show this help message"

# === HISTORICAL DATA IMPORT COMMANDS ===

# Local database connection
LOCAL_DB_URL := postgresql://postgres:postgres@localhost:5432/millionaire_bot?sslmode=disable
DATA_DIR := ./data
WORKERS := 10

# Build import tool
import-build:
	@echo "🔨 Building import_historical tool..."
	cd cmd/import_historical && go build -o import_historical
	@echo "✅ Built: cmd/import_historical/import_historical"

# Import historical data
import-data: import-build postgres
	@echo "📥 Importing historical data..."
	@echo "📂 Data directory: $(DATA_DIR)"
	@echo "👷 Workers: $(WORKERS)"
	@echo ""
	@if [ ! -d "$(DATA_DIR)" ]; then \
		echo "❌ Error: Data directory '$(DATA_DIR)' not found!"; \
		echo "Please create it and add your CSV files:"; \
		echo "  mkdir -p $(DATA_DIR)"; \
		echo "  cp /path/to/your/*.csv.gz $(DATA_DIR)/"; \
		exit 1; \
	fi
	@if [ ! -f "$(DATA_DIR)/BTCUSDT_processed.csv.gz" ]; then \
		echo "❌ Error: BTCUSDT_processed.csv.gz not found in $(DATA_DIR)!"; \
		echo "This file is required for RS calculation."; \
		exit 1; \
	fi
	@echo "⏳ Waiting for PostgreSQL to be ready..."
	@sleep 3
	@echo "🚀 Starting import..."
	DATABASE_URL=$(LOCAL_DB_URL) ./cmd/import_historical/import_historical \
		-dir $(DATA_DIR) \
		-workers $(WORKERS)
	@echo ""
	@echo "✅ Import completed!"
	@echo "📊 Check results: make db-stats"

# Import with debug logging
import-debug: import-build postgres
	@echo "📥 Importing with debug logging..."
	@sleep 2
	DATABASE_URL=$(LOCAL_DB_URL) ./cmd/import_historical/import_historical \
		-dir $(DATA_DIR) \
		-workers $(WORKERS) \
		-debug

# Full setup: Start DB + Import data
full-setup: postgres
	@echo "⏳ Waiting for database initialization..."
	@sleep 5
	@echo ""
	@$(MAKE) import-data
	@echo ""
	@echo "✅ Full setup completed!"
	@echo "📊 Database: localhost:5432"
	@echo "📈 Training data: imported"
	@echo ""
	@echo "You can now:"
	@echo "  - Run 'make run' to start the bot"
	@echo "  - Run 'make db-stats' to see statistics"
	@echo "  - Run 'make db-shell' to explore data"

# Open psql shell
db-shell:
	@echo "🐘 Opening PostgreSQL shell..."
	@echo "Database: millionaire_bot"
	@echo "Type \\q to exit, \\dt to list tables"
	@echo ""
	@docker exec -it millionaire-postgres psql -U postgres -d millionaire_bot

# Clear training_data table
db-clear:
	@echo "⚠️  This will delete ALL data from training_data table!"
	@read -p "Are you sure? (y/N): " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		echo "🗑️  Clearing training_data..."; \
		docker exec -it millionaire-postgres psql -U postgres -d millionaire_bot -c "TRUNCATE TABLE training_data RESTART IDENTITY;"; \
		echo "✅ Table cleared!"; \
	else \
		echo "❌ Cancelled."; \
	fi

# Show database stats
db-stats:
	@echo "📊 Database Statistics:"
	@echo ""
	@docker exec -it millionaire-postgres psql -U postgres -d millionaire_bot -c "\
		SELECT \
			COUNT(*) as total_records, \
			COUNT(*) FILTER (WHERE is_shadow_mode = false) as elite_signals, \
			COUNT(*) FILTER (WHERE is_shadow_mode = true) as shadow_signals, \
			COUNT(DISTINCT symbol) as unique_symbols, \
			COUNT(price_15m_close) as with_15m_outcomes, \
			COUNT(price_60m_close) as with_60m_outcomes \
		FROM training_data;" 2>/dev/null || echo "⚠️  Table not created yet or no data"
