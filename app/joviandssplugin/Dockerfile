FROM registry.fedoraproject.org/f29/origin-base:latest
#FROM alpine
LABEL maintainers="Andrei Perapiolkin"
LABEL description="JovianDSS CSI Plugin"

#RUN apk add util-linux
RUN dnf -y install iscsi-initiator-utils ca-certificates e2fsprogs util-linux iproute scsi-target-utils
COPY ./_output/jdss-csi-plugin /jdss-csi-plugin
ENTRYPOINT ["/jdss-csi-plugin"]
