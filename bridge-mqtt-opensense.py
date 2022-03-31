import paho.mqtt.client as mqtt
import requests
import json
import config

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if  rc==0:
        print('MQTT: connected to client with result code '+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    for topic in cfg['mqtt.topics']:
        client.subscribe(topic)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

    my_json = msg.payload.decode('utf8')
    print("JSON:",my_json)
    json_payload = json.loads(my_json)
    
    post_req = requests.post(cfg['opensense.host'], headers={'Content-Type':'application/json', 'Authorization': cfg['opensense.authorization']}, json = json_payload)
    print("Response: ", post_req.status_code, post_req.reason)
    
cfg = config.Config('bridge.cfg')

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
