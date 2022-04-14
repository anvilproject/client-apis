#!/bin/sh
# TODO document why we are unsetting/resetting PIP_TARGET
unset PIP_TARGET
# Install our package, includes dependencies drsclient and gen3
pip install pyAnVIL==0.0.13rc7 --user
# Install other dependencies
pip install fhirclient@git+https://github.com/smart-on-fhir/client-py#egg=fhirclient  --user
# Restore original setting
set PIP_TARGET=/home/jupyter/notebooks/packages 
echo Please re-start jupyter kernel