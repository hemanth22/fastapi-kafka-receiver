from fastapi import FastAPI
from kafka import KafkaConsumer
import six
import sys
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves
import requests
import os
import json
from typing import List

app = FastAPI()

FASTAPI_WEBHOOK_SERVERLESS = os.environ.get('FASTAPI_WEBHOOK_SERVER')
bootstrap_servers_sv1 = os.environ.get('BOOTSTRAP_SERVER_NAME')
sasl_mechanism_sv1 = os.environ.get('SASL_MECH')
security_protocol_sv1 = os.environ.get('SSL_SEC')
sasl_plain_username_sv1 = os.environ.get('SASL_USERNAME')
sasl_plain_password_sv1 = os.environ.get('SASL_PASSD')

def serialize_data(data):
    """
    Serialize the given data to a JSON string.

    Args:
        data (dict): The data to serialize

    Returns:
        str: The serialized JSON string
    """
    return json.dumps(data)

def send_dataToFastAPI(payload):
    headers = {
        'Content-Type': 'application/json',
        }
    response = requests.request("POST", url=FASTAPI_WEBHOOK_SERVERLESS, headers=headers, data=payload)
    print(response.text)
    if response.status_code == 200:
        print("Data sent successfully")

    if response.status_code != 200:
        print(f"Failed to send data. Status code: {response.status_code}")

topic_name = 'news'

consumer = KafkaConsumer(
  topic_name,
  bootstrap_servers=bootstrap_servers_sv1,
  sasl_mechanism=sasl_mechanism_sv1,
  security_protocol=security_protocol_sv1,
  sasl_plain_username=sasl_plain_username_sv1,
  sasl_plain_password=sasl_plain_password_sv1,
  group_id='$GROUP_NAME',
  auto_offset_reset='earliest',
  consumer_timeout_ms=60000,
  value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

@app.get("/messages", response_model=List[dict])
for message in consumer:
    json_data = message.value
    print("Serialized json message: ",serialize_data(json_data))
    json_data_serialized = serialize_data(json_data)
    print(f"Received raw json message: {json_data}")
    send_dataToFastAPI(json_data_serialized)

@app.on_event("shutdown")
def shutdown_event():
    consumer.close()

# Run the FastAPI application using `uvicorn`
# Example: uvicorn app:app --reload
