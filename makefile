test:
	@docker-compose up -d
	@pytest -s --cov dynamatic --cov-report term-missing:skip-covered

build:
	@rm -r build
	@rm -r dist
	@rm -r dynamatic.egg-info
	@python setup.py sdist bdist_wheel

publish:
	@twine upload dist/*