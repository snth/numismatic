#!/bin/ash -ex
cd /home/user
git clone https://github.com/snth/numismatic.git
cd ./numismatic
python3.6 -m venv venv
source ./venv/bin/activate
pip install -U pip
pip install wheel
pip install -e .
chown user:user -R /home/user
