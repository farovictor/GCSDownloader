SHELL := /bin/bash

include .env

export BUCKET_NAME
export PROJECT_ID
export STORAGE_EMULATOR_HOST

init-gcs-emulator:
	docker run -d \
		-e PORT=9023 \
		-p 9023:9023 \
		--name gcp-storage-emulator \
		--rm \
		oittaa/gcp-storage-emulator


build:
	go build --race -o ./bin/gcsdownloader ./src
	chmod a+x ./bin/gcsdownloader

run-help: build
	./bin/gcsdownloader --help

run-download-batch: build
	./bin/gcsdownloader download-batch -o ./data -a emulator -b test -x 50 -p "some" --flatten-directory false

run-download-blob: build
	./bin/gcsdownloader download -o ./data -a emulator -b test --blob-path "some/prefix/now_100_workers/mbl_e7c3ce56-646c-11ee-853b-96e281524756.json" --flatten-directory false

.PHONY: run-help, run-download-batch, run-download-blob
