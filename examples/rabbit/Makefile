#!make
#include env.dev
#export $(shell sed 's/=.*//' env.dev)
export WORKER_NAME=worker
export PYTHON=/usr/bin/python3
export IMAGE_NAME=rabbit-server-custom
export VERSION=latest
export CONTAINER=$(IMAGE_NAME)_container

default: run

build:
	docker build --tag $(IMAGE_NAME):${VERSION} . -f Dockerfile

stopc:
	docker rm --force $(CONTAINER)

runc:
	docker run --name ${CONTAINER} $(IMAGE_NAME):${VERSION}

run:
	docker-compose up -d
	
stop:
	docker-compose down
