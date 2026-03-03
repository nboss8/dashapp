.PHONY: run prod-test run-agent deploy logs restart status dbt-parse dbt-run dbt-test

# Local development (Flask debug server)
run:
	python app.py

# Dedicated agent service — run in separate terminal so chat doesn't queue behind reports
run-agent:
	python agent_service.py

# Production test: gunicorn on TCP (override socket for local testing)
prod-test:
	gunicorn -c gunicorn.conf.py server:server --bind 127.0.0.1:8050

# Deploy on server: pull + restart (run from /opt/dashapp)
deploy:
	git pull
	sudo systemctl restart dashapp

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
