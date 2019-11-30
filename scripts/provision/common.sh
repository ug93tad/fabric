#!/bin/bash

# Add any logic that is common to both the peer and docker environments here

apt-get update -qq

# Used by CHAINTOOL
apt-get install -y default-jre python

wget https://dl.google.com/go/go1.11.13.linux-amd64.tar.gz
tar -zxvf go1.11.13.linux-amd64.tar.gz
rm -rf /opt/go
mv go /opt/go
