.PHONY: run prod-test prod-test-bare run-agent docker-up docker-down deploy logs restart status dbt-parse dbt-run dbt-test

# Local development (Flask debug server)
run:
	python app.py

# Dedicated agent service — run in separate terminal so chat doesn't queue behind reports
run-agent:
	python agent_service.py

# Production-like test: Docker (works on Windows and Linux)
prod-test:
	docker compose up

# Production test: native gunicorn (Linux only)
prod-test-bare:
	gunicorn -c gunicorn.conf.py server:server --bind 127.0.0.1:8050

# Docker convenience
docker-up:
	docker compose up -d

docker-down:
	docker compose down

# Deploy on server: pull + restart (run from /opt/dashapp)
# Uses Docker if available, else systemctl
deploy:
	git pull
	@if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then \
		docker compose up -d --build; \
	else \
		sudo systemctl restart dashapp; \
	fi

# View service logs
logs:
	sudo journalctl -u dashapp -f

# Restart service
restart:
	sudo systemctl restart dashapp

# Service status
status:
	sudo systemctl status dashapp

# dbt - run from dbt/ with profiles-dir=. (load .env first for Snowflake credentials)
dbt-parse:
	cd dbt && dbt parse --profiles-dir .
dbt-run:
	cd dbt && dbt run --profiles-dir .
dbt-test:
	cd dbt && dbt test --profiles-dir .
