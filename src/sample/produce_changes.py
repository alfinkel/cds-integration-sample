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

with cloudant(user, pwd, account=acct) as client:
    db = client.create_database('sample-{0}'.format(unicode(uuid.uuid4())))
    if not db.exists():
        raise Exception('Database creation failed!!')
    i = 0
    db.create_document({'_id': 'julia{0:03d}'.format(i), 'name': 'julia', 'age': i})
    for change in db.changes(include_docs=True):
        if change:
            print change
            producer.send_messages(
                topic,
                json.dumps({'db': db.database_name, 'doc': change['doc']})
            )
            i += 1
            if i < 10:
                db.create_document({'_id': 'julia{0:03d}'.format(i), 'name': 'julia', 'age': i})

kafka_client.close()
