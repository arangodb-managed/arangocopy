SHELL = bash
PROJECT := arangocopy

COMMIT := $(shell git rev-parse --short HEAD)
DOCKERIMAGE ?= $(shell zutano docker image --name=$(PROJECT))

all: binaries

clean:
	rm -Rf bin

binaries:
	CGO_ENABLED=0 gox \
		-osarch="linux/amd64 linux/arm darwin/amd64 windows/amd64" \
		-ldflags="-X main.projectVersion=${VERSION} -X main.projectBuild=${COMMIT}" \
		-output="bin/{{.OS}}/{{.Arch}}/$(PROJECT)" \
		-tags="netgo" \
		./...
	mkdir -p assets
	zip -r assets/arangocopy.zip bin/*

.PHONY: test
test:
	mkdir -p bin/test
	go test -coverprofile=bin/test/coverage.out -v ./... | tee bin/test/test-output.txt ; exit "$${PIPESTATUS[0]}"
	cat bin/test/test-output.txt | go-junit-report > bin/test/unit-tests.xml
	go tool cover -html=bin/test/coverage.out -o bin/test/coverage.html

bootstrap:
	go get github.com/mitchellh/gox
	go get github.com/jstemmer/go-junit-report
	go get github.com/arangodb/go-driver

docker:
	docker build \
		--build-arg=GOARCH=amd64 \
		-t $(DOCKERIMAGE) .

docker-push:
	docker push $(DOCKERIMAGE)
