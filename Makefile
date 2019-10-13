REGISTRY_NAME=opene
IMAGE_NAME=joviandss-csi
IMAGE_VERSION=$(shell git describe --long --tags)
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD)
IMAGE_TAG=$(REGISTRY_NAME)/$(IMAGE_NAME):$(BRANCH_NAME)-$(IMAGE_VERSION)
IMAGE_LATEST=$(REGISTRY_NAME)/$(IMAGE_NAME):latest

.PHONY: default all joviandss clean hostpath-container iscsi rest

default: joviandss
	
all:  joviandss container-container rest

rest:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o _output/rest ./app/rest

joviandss: rest

	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-X JovianDSS-KubernetesCSI/pkg/joviandss.Version=$(IMAGE_VERSION) -extldflags "-static"' -o _output/jdss-csi-plugin ./app/joviandssplugin

joviandss-container: joviandss
	@echo Building Container
	sudo docker build -t $(IMAGE_LATEST) -f ./app/joviandssplugin/Dockerfile .

clean:
	go clean -r -x
	-rm -rf _outpusudo docker push $(IMAGE_TAG)
