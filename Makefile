# ---------------------------------------------------------------------------
# productivity-metrics-queue - Makefile portatil (Windows + macOS + Linux)
# ---------------------------------------------------------------------------

.PHONY: help build test lint up down run logs clean check-go

# --- Deteccao de "docker compose" (V2 plugin) vs "docker-compose" (V1) -----
# A Compose V2 e integrada ao Docker CLI (`docker compose ...`). A V1 e o
# binario Python legado (`docker-compose ...`). Detectamos em tempo de `make`
# e expomos como variavel $(DC).
#
# A bifurcacao abaixo escolhe o null device certo pro shell em uso. Em sh,
# `>nul` tenta criar um arquivo literal chamado "nul" (nome reservado no
# Windows), entao a redirection falha e o $(shell) retorna string vazia.
# MSYSTEM e exportada pelo Git Bash/MSYS2 (ex.: MINGW64).
ifdef MSYSTEM
# Git Bash / MSYS2 / MinGW (Windows com sh.exe)
DC := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
else ifeq ($(OS),Windows_NT)
# Windows nativo (make usando cmd.exe)
DC := $(shell docker compose version >nul 2>&1 && echo docker compose || echo docker-compose)
else
# macOS / Linux / WSL
DC := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
endif

help:
	@echo "Targets disponiveis:"
	@echo "  make build   - compila os dois servicos"
	@echo "  make test    - roda go test ./... em ambos"
	@echo "  make lint    - go vet ./... em ambos"
	@echo "  make up      - sobe a stack via $(DC) up -d"
	@echo "  make down    - derruba a stack"
	@echo "  make run     - up + segue logs"
	@echo "  make logs    - segue logs da stack"
	@echo "  make clean   - down + remove volumes"
	@echo ""
	@echo "Compose detectado: $(DC)"

check-go:
	@go version

build: check-go
	cd services/processor  && go build ./...
	cd services/aggregator && go build ./...

test: check-go
	cd services/processor  && go test ./...
	cd services/aggregator && go test ./...

lint: check-go
	cd services/processor  && go vet ./...
	cd services/aggregator && go vet ./...

up:
	$(DC) up -d

down:
	$(DC) down

run: up
	$(DC) logs -f

logs:
	$(DC) logs -f

clean:
	$(DC) down -v
