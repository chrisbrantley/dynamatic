test:
	@docker-compose up -d
	@pytest -s --cov dynamatic --cov-report term-missing:skip-covered