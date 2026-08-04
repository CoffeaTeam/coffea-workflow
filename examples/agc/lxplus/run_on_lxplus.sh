#!/bin/bash
set -e
voms-proxy-init --voms cms --valid 192:00
KRB5DIR=$(dirname ${KRB5CCNAME#FILE:})
apptainer exec \
  --bind /tmp \
  --bind /etc/condor \
  --bind "$KRB5DIR" \
  --env KRB5CCNAME=$KRB5CCNAME \
  --env X509_USER_PROXY=/tmp/x509up_u$(id -u) \
  --env PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  ./worker.sif python3 workflow_lxplus.py
