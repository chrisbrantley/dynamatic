test:
	@docker-compose up -d
	@pytest -s --cov dynamatic --cov-report term-missing:skip-covered

build:
	@python setup.py sdist bdist_wheel

publish:
	@twine upload dist/*