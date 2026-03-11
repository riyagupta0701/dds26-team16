#!/usr/bin/env bash
set -e   # exit immediately if any command fails

# 1. Build service images into minikube's Docker daemon
eval $(minikube docker-env)
docker build -t order:latest ./order
docker build -t stock:latest ./stock
docker build -t payment:latest ./payment

# 2. Enable the minikube ingress addon
minikube addons enable ingress

# 3. Create configmap for internal nginx gateway
kubectl create configmap gateway-nginx-conf \
  --from-file=nginx.conf=k8s/gateway-nginx.conf \
  --dry-run=client -o yaml | kubectl apply -f -

# 4. Deploy everything: Redis (master+replica+sentinel) + gateway + app services
kubectl apply -f k8s/
