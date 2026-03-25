#!/usr/bin/env bash
# Generate self-signed CA + broker certs for local SSL Kafka testing.
# Uses only openssl (no keytool). Output: test/ssl/
set -euo pipefail

SSL_DIR="$(dirname "$0")/ssl"
rm -rf "$SSL_DIR"
mkdir -p "$SSL_DIR"
cd "$SSL_DIR"

PASSWORD="testpassword"

# CA key + cert
openssl req -new -x509 -keyout ca-key.pem -out ca-cert.pem -days 365 \
    -subj "/CN=MillpondTestCA" -passout "pass:$PASSWORD" -nodes 2>/dev/null

# Broker key + CSR
openssl req -newkey rsa:2048 -keyout kafka-key.pem -out kafka.csr \
    -subj "/CN=kafka" -nodes 2>/dev/null

# Sign broker cert with CA (with SAN)
openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem -in kafka.csr \
    -out kafka-cert.pem -days 365 -CAcreateserial \
    -extfile <(echo "subjectAltName=DNS:kafka") 2>/dev/null

# Create PKCS12 keystore for Kafka broker (combines key + cert + CA)
openssl pkcs12 -export -in kafka-cert.pem -inkey kafka-key.pem \
    -chain -CAfile ca-cert.pem -name kafka \
    -out kafka.keystore.p12 -password "pass:$PASSWORD" 2>/dev/null

# Create PKCS12 truststore with CA cert via keytool (from Kafka image)
docker run --rm -v "$PWD:/ssl" -w /ssl --entrypoint keytool apache/kafka:3.9.0 \
    -importcert -alias ca -file ca-cert.pem -keystore kafka.truststore.p12 \
    -storetype PKCS12 -storepass "$PASSWORD" -noprompt 2>/dev/null

# Kafka CLI client properties (for kafka-topics.sh etc.)
cat > client.properties <<EOF
security.protocol=SSL
ssl.truststore.location=/opt/kafka/config/ssl/kafka.truststore.p12
ssl.truststore.password=$PASSWORD
ssl.truststore.type=PKCS12
EOF

echo "SSL certs generated in $SSL_DIR"
