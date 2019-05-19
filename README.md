# Open-E JovianDSS Kubernetes CSI plugin

## Source code is comming

## Deployment

### Add config

Add config files as secrets:

``` bash
kubectl create secret generic jdss-controller-cfg --from-file=./deploy/cfg/controller.yaml

kubectl create secret generic jdss-node-cfg --from-file=./deploy/cfg/node.yaml
```
Node config do not provides nothing but storage address and request to create proper services.

### Deploy plugin

Create plugin instances across your cluster

``` bash
kubectl apply -f ./deploy/joviandss/joviandss-csi-controller.yaml

kubectl apply -f ./deploy/joviandss/joviandss-csi-node.yaml 
```

### Deploy application

``` bash
kubectl apply -f ./deploy/examples/joviandss-csi-sc.yaml

kubectl apply -f ./deploy/examples/nginx-pvc.yaml

kubectl apply -f ./deploy/examples/nginx.yaml
```
