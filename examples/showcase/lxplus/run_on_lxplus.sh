#!/bin/bash
set -e
voms-proxy-init --voms cms --valid 192:00
KRB5DIR=$(dirname ${KRB5CCNAME#FILE:})
apptainer exec \
  --bind /tmp \
  --bind /etc/condor \
  --bind "$KRB5DIR" \
  --env KRB5CCNAME=$KRB5CCNAME \
  ./worker.sif python3 run_workflow.py
