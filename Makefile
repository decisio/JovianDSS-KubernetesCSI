REGISTRY_NAME=andreiperepiolkin
IMAGE_NAME=joviandss-kubernetes-csi
IMAGE_VERSION=latest
IMAGE_TAG=$(REGISTRY_NAME)/$(IMAGE_NAME):$(IMAGE_VERSION)
REV=$(shell git describe --long --tags)


.PHONY: default all joviandss clean hostpath-container iscsi rest

default: joviandss
	
all:  joviandss container-container rest


test:
	go test open-e-csi-kubernetes/pkg/... -cover
	go vet open-e-csi-kubernetes/pkg/...

rest:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o _output/rest ./app/rest

joviandss: rest

	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-X open-e-csi-kubernetes/pkg/joviandss.Version=$(REV) -extldflags "-static"' -o _output/jdss-csi-plugin ./app/joviandssplugin

joviandss-container: joviandss
	@echo Building Container
	sudo docker build -t $(IMAGE_TAG) -f ./app/joviandssplugin/Dockerfile .

push: joviandss-container
	@echo Publish Container
	sudo docker push $(IMAGE_TAG)

clean:
	go clean -r -x
	-rm -rf _outpusudo docker push $(IMAGE_TAG)
