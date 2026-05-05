include .env
export

.PHONY: run build test migrate-up migrate-down docker-up docker-down

run:
	go run ./cmd/taskqueue

build:
	go build -o taskqueue ./cmd/taskqueue

test:
	go test ./...

migrate-up:
	goose -dir migrations postgres "$(DATABASE_URL)" up

migrate-down:
	goose -dir migrations postgres "$(DATABASE_URL)" down

docker-up:
	docker compose up -d db

docker-down:
	docker compose down
