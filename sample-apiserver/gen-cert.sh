#!/bin/bash

OPENSSL_BIN=$(command -v openssl)
cert_dir=ssl

gen_signing_certkey() {
    local id=$1
    ${OPENSSL_BIN} req -x509 -nodes -newkey rsa:4096 -keyout ${cert_dir}/${id}-ca.key -out ${cert_dir}/${id}-ca.crt -sha256 -days 365 -subj "/C=xx/ST=x/L=x/O=x/OU=x/CN=ca/emailAddress=x/"
}

gen_certkey() {
    local id=$1

    ${OPENSSL_BIN} genrsa -out ${cert_dir}/${id}.key
    ${OPENSSL_BIN} req -new -key ${cert_dir}/${id}.key -out ${cert_dir}/${id}.csr -config ${cert_dir}/csr.conf
    ${OPENSSL_BIN} x509 -req -days 365 -in ${cert_dir}/${id}.csr \
        -CA ${cert_dir}/${id}-ca.crt -CAkey ${cert_dir}/${id}-ca.key \
        -CAcreateserial -out ${cert_dir}/${id}.crt \
        -extensions v3_ext -extfile ${cert_dir}/csr.conf
}

gen_signing_certkey server
gen_signing_certkey client
gen_certkey server
gen_certkey client