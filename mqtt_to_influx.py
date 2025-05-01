import paho.mqtt.client as mqtt                                              # connect to an MQTT broker
from influxdb_client import InfluxDBClient as InfluxDBClient2, Point, WritePrecision # Point: This represents a single data point/ WritePrecision: This specifies the precision used for timestamps
from influxdb_client.client.write_api import SYNCHRONOUS # waits for the data to be written before continuing
from datetime import datetime
from flask import Flask, render_template
from flask_cors import CORS
import threading # module allows you to run multiple threads (separate tasks) concurrently.
import time


app = Flask(__name__)
CORS(app)

# MQTT settings
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_USER = "iot"
MQTT_PASSWORD = "1234567890"
MQTT_TOPICS = [("sensor/temperature", 0), ("sensor/humidity", 0)]

# InfluxDB 2.x settings
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "Your API"
INFLUXDB_ORG = "your Org"
INFLUXDB_BUCKET = "Your Bucket"

# InfluxDB 2.x client
influx_client = InfluxDBClient2(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)  # Writing data
# query_api = influx_client.query_api()

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:   #  If rc == 0, the connection is successful.Other values 1, 2, 3, etc indicate different connection errors (1: Incorrect protocol version./2: Invalid client identifier./3: Server unavailable./4: Bad username or password./5: Not authorized.)
        print(f"Connected to MQTT broker successfully: {userdata}")
        print(f"Flags received: {flags}")
        if not hasattr(client, 'already_subscribed') or not client.already_subscribed:
            client.subscribe(MQTT_TOPICS)
            client.already_subscribed = True # Is set to True after the first successful
    else:
        print(f"Failed to connect to MQTT broker, return code: {rc}")

def on_message(client, userdata, msg):
    try:
        value = float(msg.payload.decode())  # Converts the raw byte message (e.g., b'22.5') to a Python float.
        measurement = msg.topic.split('/')[1]  # Extracts "temperature" or "humidity" from the topic like "sensor/temperature"

        # Create InfluxDB point / Constructs a data point and writes it to InfluxDB with the current UTC time.
        point = Point(measurement).field("value", value).time(datetime.utcnow(), WritePrecision.NS) 
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        # Print ISO-formatted timestamp
        current_time = datetime.utcnow().isoformat()
        print(f"[{current_time}] Stored {measurement}: {value}")
        
    except ValueError as e:
        print(f"Error converting payload to float: {msg.payload}, error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")

# MQTT client setup (with a unique client_id)
mqtt_client = mqtt.Client(client_id="sensor_app" , userdata="Hello From IOT Station to the World", clean_session=False) # if you have many clients u can use client_id =f"sensor_app_{uuid.uuid4()}"
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect and start MQTT loop only once/Connects to the MQTT broker and starts loop_forever() in a background thread so the Flask server can run independently.
try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_thread = threading.Thread(target=mqtt_client.loop_forever)
    mqtt_thread.daemon = True
    mqtt_thread.start()
except Exception as e:
    print(f"Failed to connect to MQTT broker: {e}")
    exit(1)

# Flask app run
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
