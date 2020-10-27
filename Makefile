#!make
include env.dev
export $(shell sed 's/=.*//' env.dev)
export WORKER_NAME=worker
export PYTHON=/usr/bin/python3
export IMAGE_NAME=tbevents
export VERSION=latest
export CONTAINER=$(IMAGE_NAME)_container

default: run

build:
	$(PYTHON) -m pip install -r ./requirements/dev.txt

run:
	ddtrace-run $(PYTHON) -m app.main

buildc:
	docker build --tag $(IMAGE_NAME):${VERSION} . -f Dockerfile

stopc:
	docker rm --force $(CONTAINER)

runc:
	docker run --name ${CONTAINER} $(IMAGE_NAME):${VERSION}

upload-pip:
	# https://dzone.com/articles/executable-package-pip-install
	python setup.py bdist_wheel
	python -m twine upload dist/*

pip-upload:
	cd dist && twistd -n web  --path .

install-local:
	pip install -e .
