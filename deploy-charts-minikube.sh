#!/usr/bin/env bash
set -e   # exit immediately if any command fails

# 1. Install Redis and nginx ingress controller with Helm
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm upgrade --install redis bitnami/redis -f helm-config/redis-helm-values.yaml

# 2. Create configmap for internal nginx gateway
kubectl create configmap gateway-nginx-conf \
  --from-file=nginx.conf=k8s/gateway-nginx.conf \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Deploy the gateway and app services
kubectl apply -f k8s/
