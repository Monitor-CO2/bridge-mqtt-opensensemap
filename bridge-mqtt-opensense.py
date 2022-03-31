import paho.mqtt.client as mqtt
import requests
import json
import config
import argparse
import logging

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logger.info('MQTT: connection to client: %s port: %s with result code: %i: %s', client._host, client._port, rc, mqtt.connack_string(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    if rc==0:
        for association in cfg['mqtt.association']:
            client.subscribe(association['topic'])
            logger.info('MQTT: subscribed to topic: %s sensor: %s', association['topic'], association['sensor'])

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logger.info('MQTT: topic: %s received msg: %s', msg.topic, msg.payload)

    json_payload = json.loads(msg.payload.decode('utf8'))
    
    # association is the mapping between the mqtt topic and the sensor_id for opensensemap    
    for association in cfg['mqtt.association']:
        if msg.topic == association['topic']:
            full_host = cfg['opensense.host']+'/'+cfg['opensense.box']+'/'+association['sensor']
            logger.debug('full_host to opensensemap: %s', full_host)
            break
      
    post_req = requests.post(full_host, headers={'Content-Type':'application/json', 'Authorization': cfg['opensense.authorization']}, json = json_payload)
    logger.info('Opensensemap: %i response from post: %s', post_req.status_code, post_req.reason)
    
    
cmparser = argparse.ArgumentParser(description='Bridge from a MQTT broker to Opensensemap.')
cmparser.add_argument("-f", "--configfile", dest="configfile", help="Config file name. Default: bridge.cfg")
cmparser.add_argument("-l", "--log", dest="logLevel", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Set the logging level")

args = cmparser.parse_args()
if args.logLevel:
    logging.basicConfig(level=getattr(logging, args.logLevel), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

if args.configfile == None:
    args.configfile = 'bridge.cfg'
    
logger.debug('Using config file: %s', args.configfile)    
    
cfg = config.Config(args.configfile)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username=cfg['mqtt.username'],password=cfg['mqtt.password'])
client.tls_set()
client.connect(cfg['mqtt.host'], cfg['mqtt.port'], 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
