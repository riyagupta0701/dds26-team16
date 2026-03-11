#!/usr/bin/env bash
set -e   # exit immediately if any command fails

# 1. Install nginx ingress controller via Helm
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm upgrade --install nginx ingress-nginx/ingress-nginx -f helm-config/nginx-helm-values.yaml

# 2. Create configmap for internal nginx gateway
kubectl create configmap gateway-nginx-conf \
  --from-file=nginx.conf=k8s/gateway-nginx.conf \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Deploy everything: Redis (master+replica+sentinel) + gateway + app services
kubectl apply -f k8s/
