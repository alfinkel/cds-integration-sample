import os
import ConfigParser
import uuid
import json
from cloudant import cloudant
from kafka import KafkaClient, SimpleProducer

import logging
logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.ERROR
)

config_file = os.path.expanduser('~/.config')
config = ConfigParser.RawConfigParser()
config.read(config_file)
acct = config.get('cloudant', 'account')
user = config.get('cloudant', 'user')
pwd = config.get('cloudant', 'pwd')

if not acct or not user or not pwd:
    raise Exception('Cannot connect with Cloudant client.')

kafka = config.get('kafka', 'server')
topic = config.get('kafka', 'topic')

if not kafka or not topic:
    raise Exception('Cannot connect with Kafka.')
kafka_client = KafkaClient(kafka)
producer = SimpleProducer(kafka_client, async=False)

count = 0
with cloudant(user, pwd, account=acct) as client:
    db = client['sdp_kafka_test0000']
    if not db.exists():
        raise Exception('Database does not exists!!')
    for change in db.changes():
        producer.send_messages(topic, json.dumps(change))
        count = count + 1
        print '{0} changes processed'.format(count)

kafka_client.close()
