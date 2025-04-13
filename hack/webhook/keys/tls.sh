#!/bin/bash
set -e

# Generate CA key and certificate
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -subj "/CN=Admission Controller CA" -days 3650

# Generate server key
openssl genrsa -out server.key 2048

# Create config for certificate
cat <<EOF > csr.conf
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
CN = redis-cluster-webhook.redis.svc

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = redis-cluster-webhook
DNS.2 = redis-cluster-webhook.redis
DNS.3 = redis-cluster-webhook.redis.svc
EOF

# Generate certificate signing request
openssl req -new -key server.key -out server.csr -config csr.conf

# Sign the certificate with our CA
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -extensions req_ext -extfile csr.conf

# Create the secret with the certificate and key
# kubectl create secret tls webhook-certs --cert=server.crt --key=server.key -n webhook-system --dry-run=client -o yaml | kubectl apply -f -

# Update the ValidatingWebhookConfiguration with the CA data
CA_BUNDLE=$(base64 -w 0 ca.crt)
sed -e "s|\${CA_BUNDLE}|${CA_BUNDLE}|g" validatingwebhookconfiguration.yaml > validatingwebhookconfiguration-ready.yaml
