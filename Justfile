default:
    @just --list

# Run the server
run:
    go run ./cmd/stevy serve --migrate

# Build the binary
build:
    go build -o stevy ./cmd/stevy

# Generate proto code
generate:
    buf generate

# Run database migrations
migrate:
    go run ./cmd/stevy migrate

# Tidy dependencies
tidy:
    go mod tidy

# Lint proto files
lint:
    buf lint

# Clean build artifacts
clean:
    rm -f stashy
    rm -f *.db *.db-journal *.db-wal *.db-shm