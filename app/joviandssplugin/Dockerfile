FROM alpine:latest
LABEL maintainers="Andrei Perapiolkin"
LABEL description="JovianDSS CSI Plugin"

RUN apk add util-linux open-iscsi e2fsprogs iproute
COPY ./_output/jdss-csi-plugin /jdss-csi-plugin
ENTRYPOINT ["/jdss-csi-plugin"]
