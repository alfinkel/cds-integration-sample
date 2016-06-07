import os
import ConfigParser
import json
import requests
import posixpath

from kafka import KafkaConsumer

config_file = os.path.expanduser('~/.config')
config = ConfigParser.RawConfigParser()
config.read(config_file)
kafka = config.get('kafka', 'server')
topic = config.get('kafka', 'topic')

if not kafka or not topic:
    raise Exception('Cannot connect with Kafka.')

wsk_auth = config.get('whisk', 'auth')
wsk_url = config.get('whisk', 'url')
wsk_namespace = config.get('whisk', 'namespace')

#if not wsk_auth or not wsk_url or not wsk_namespace:
#    raise Exception('Cannot connect with Whisk.')

url = posixpath.join(wsk_url, wsk_namespace, 'triggers', 'updateJules')
headers = {
    'Authorization': 'Basic {0}'.format(wsk_auth),
    'Content-Type': 'application/json'
}

consumer = KafkaConsumer(topic, bootstrap_servers=[kafka])
for message in consumer:
    data = json.loads(message.value)
    update_status = ''
    if data['doc']['name'] == 'julia' and data['doc']['age'] % 2 == 0:
        update_status = ' - sent for update'
        data['doc']['name'] = 'jules'
        #resp = requests.post(url, headers=headers, data=json.dumps(data))
        #print resp.json()
        #resp.raise_for_status()

    print '{0:s}:{1:d}:{2:d}: key={3:s} value={4:s}{5}'.format(
        message.topic,
        message.partition,
        message.offset,
        message.key,
        message.value,
        update_status
    )
    