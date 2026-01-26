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
	@echo "  make build         - Build the bot binary"
	@echo "  make run           - Run the bot locally"
	@echo "  make test          - Run tests"
	@echo "  make clean         - Remove build artifacts"
	@echo "  make deps          - Download and tidy dependencies"
	@echo "  make docker-build  - Build Docker image"
	@echo "  make docker-run    - Run Docker container"
	@echo "  make postgres      - Start local PostgreSQL"
	@echo "  make postgres-stop - Stop local PostgreSQL"
	@echo "  make fmt           - Format code"
	@echo "  make lint          - Lint code"
	@echo "  make help          - Show this help message"
