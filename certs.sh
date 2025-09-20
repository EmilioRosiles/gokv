#!/bin/bash

HOST="${HOST:-localhost}"
CERT_DIR="./certs"
CERT_CONF="$CERT_DIR/cert.conf"

# Create certs directory if it doesn't exist
mkdir -p "$CERT_DIR"

# Generate OpenSSL config with SANs
cat > "$CERT_CONF" <<EOF
[req]
default_bits       = 2048
prompt             = no
default_md         = sha256
req_extensions     = req_ext
distinguished_name = dn

[dn]
CN = $HOST

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = $HOST
DNS.2 = 127.0.0.1
DNS.3 = gokv1
DNS.4 = gokv2
DNS.5 = gokv3
EOF

openssl req -new -nodes \
  -newkey rsa:2048 \
  -keyout "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.csr" \
  -config "$CERT_CONF"


openssl x509 -req -days 365 \
  -in "$CERT_DIR/server.csr" \
  -signkey "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.crt" \
  -extensions req_ext \
  -extfile "$CERT_CONF"

echo "TLS certs generated for host: $HOST"
