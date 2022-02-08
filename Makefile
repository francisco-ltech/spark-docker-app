local:
		mypy .\src
		flake8
		pytest

all:
		tox