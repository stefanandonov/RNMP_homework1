#!/bin/sh
python -m venv ./venv
sleep 3
. ./venv/bin/activate
sleep 3
pip install -r requirements.txt
sleep 3
docker-compose up -d
sleep 3
sed -i '/from kafka.vendor.six.moves import range/d' venv/lib/python3.12/site-packages/kafka/codec.py
sleep 3
python produce_messages.py
