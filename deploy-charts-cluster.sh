#!/usr/bin/env bash
set -e   # exit immediately if any command fails

# 1. Install Helm charts: Redis and nginx ingress controller
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update

# 2. Install Redis with custom values
helm upgrade --install redis bitnami/redis -f helm-config/redis-helm-values.yaml

# 3. Install nginx ingress controller with custom values
helm upgrade --install nginx ingress-nginx/ingress-nginx -f helm-config/nginx-helm-values.yaml

# 4. Create configmap for internal nginx gateway
kubectl create configmap gateway-nginx-conf \
  --from-file=nginx.conf=k8s/gateway-nginx.conf \
  --dry-run=client -o yaml | kubectl apply -f -

# 5. Deploy the gateway and app services
kubectl apply -f k8s/