#!/usr/bin/env bash
set -e
OUT_DIR="./certs"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

# 1) Generate CA key & cert
openssl genrsa -out ca.key.pem 4096
openssl req -x509 -new -nodes -key ca.key.pem -sha256 -days 3650 -subj "/CN=Local Dev CA" -out ca.cert.pem

# Helper for cert signing:
function gen_cert() {
  local NAME=$1
  local CN=$2
  local HOSTS=$3 # comma-separated SANs, e.g., DNS:localhost,IP:127.0.0.1

  openssl genrsa -out ${NAME}.key.pem 2048
  openssl req -new -key ${NAME}.key.pem -subj "/CN=${CN}" -out ${NAME}.csr.pem

  cat > ${NAME}_ext.cnf <<EOF
[ v3_req ]
subjectAltName = ${HOSTS}
extendedKeyUsage = serverAuth,clientAuth
keyUsage = critical, digitalSignature, keyEncipherment
EOF

  openssl x509 -req -in ${NAME}.csr.pem -CA ca.cert.pem -CAkey ca.key.pem -CAcreateserial \
    -out ${NAME}.cert.pem -days 825 -sha256 -extfile ${NAME}_ext.cnf -extensions v3_req

  rm -f ${NAME}.csr.pem ${NAME}_ext.cnf
}

# Server certificate for internal cluster server (include cluster DNS names/IPs)
gen_cert "internal-server" "internal.server.local" "DNS:gokv1,DNS:gokv2,DNS:gokv3"

# External server cert (for clients)
gen_cert "external-server" "external.server.local" "DNS:localhost,IP:127.0.0.1"

# Client certificates for mTLS (internal client)
openssl genrsa -out internal-client.key.pem 2048
openssl req -new -key internal-client.key.pem -subj "/CN=internal-client" -out internal-client.csr.pem

cat > internal-client_ext.cnf <<EOF
[ v3_req ]
extendedKeyUsage = clientAuth
keyUsage = critical, digitalSignature
EOF

openssl x509 -req -in internal-client.csr.pem -CA ca.cert.pem -CAkey ca.key.pem -CAcreateserial \
  -out internal-client.cert.pem -days 825 -sha256 -extfile internal-client_ext.cnf -extensions v3_req

rm -f internal-client.csr.pem internal-client_ext.cnf

echo "Created CA and certs under $(pwd)"
ls -l