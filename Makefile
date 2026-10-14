.PHONY: up down logs ps test lint build

up:        ## Butun stekni ko'tarish (generate -> api -> pipeline -> report)
	docker compose up -d --build report

down:      ## To'xtatish va volume'larni o'chirish
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps -a

build:
	docker compose build

lint:
	ruff check .

test:
	python -m pytest -q
