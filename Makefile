.PHONY: all download sync parse clean tidy

all: download sync parse

tidy:
	go mod tidy

download:
	@echo "Building download command..."
	@mkdir -p bin
	go build -o ./bin/download ./cmd/download

sync:
	@echo "Building sync command..."
	@mkdir -p bin
	go build -o ./bin/sync ./cmd/sync

parse:
	@echo "Building parse command..."
	@mkdir -p bin
	go build -o ./bin/parse ./cmd/parse

clean:
	rm -rf bin

help:
	@echo "Available targets:"
	@echo "  all     - Build all commands (download, sync, parse)"
	@echo "  download - Build download command"
	@echo "  sync    - Build sync command"
	@echo "  parse   - Build parse command"
	@echo "  tidy    - Run go mod tidy"
	@echo "  clean   - Remove bin directory"
	@echo "  help    - Show this help message"

