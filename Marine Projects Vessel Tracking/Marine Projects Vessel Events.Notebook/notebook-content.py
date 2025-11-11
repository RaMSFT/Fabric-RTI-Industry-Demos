# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Install Event Hub packages

# CELL ********************


evh_name = "<your-eventhub>"
evh_conn_string = "<your-eventhub-connection-string-primary-key>"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

! python --version

! pip install azure-eventhub==5.11.5 --upgrade --force --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Connect to Event Stream

# CELL ********************

import json
from azure.eventhub import EventHubProducerClient, EventData
import os
import socket
import random

from random import randrange

eventHubNameevents = evh_name
eventHubConnString = evh_conn_string
producer_events = EventHubProducerClient.from_connection_string(conn_str=eventHubConnString, eventhub_name=eventHubNameevents)

hostname = socket.gethostname()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Send events to Event Stream

# CELL ********************

def sendToEventsHub(jsonEvent, producer):
    eventString = json.dumps(jsonEvent)
    #print(eventString) 
    event_data_batch = producer.create_batch() 
    event_data_batch.add(EventData(eventString)) 
    producer.send_batch(event_data_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Wrapper to generate events and send to event Hub

# CELL ********************

import random
import time
import uuid
from datetime import datetime

class EnergyGridSensorSimulator:
    def __init__(self, vessel_id, vessel_details, current_time_pas):
        self.vessel_id = vessel_id
        self.vessel_number = vessel_details[0]
        self.vessel_source = vessel_details[1]
        self.vessel_lat = vessel_details[2]
        self.vessel_lang = vessel_details[3]
        self.unique_id = str(uuid.uuid4())
        self.current_time = current_time_pas

    def log_event(self, activity, vessel_id, vessel_number,vessel_source ,vessel_lat, vessel_lang, sensor_data):
        event = {
            "timestamp": self.current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_id": self.unique_id,
            "activity": activity,
            "vessel_id": vessel_id,
            "vessel_number": vessel_number,
            "vessel_source": vessel_source,
            "vessel_lat": vessel_lat,
            "vessel_lang": vessel_lang,
            "sensor_data": sensor_data
        }
        log_event = event
        log_obj = json.dumps(log_event)
        #impressionEvent = json_object    
        sendToEventsHub(log_event, producer_events)
        #print(log_event)

    def get_sensor_data(self):
        weather_condition = round(1 if random.random() < 1/100 else 0)
        maintenance = round(1 if random.random() < 1/100 else 0)
        print(weather_condition)
        generators=[1,2,3,4]
        active_generator = random.choice(generators)
        thrustor=[1,2,3,4]
        choices = (1 if random.random() < 1/100 else random.choice([2,3]))
        active_thrustor = random.sample(thrustor, choices)
        sensors = {
            "heel": {"value": round(random.uniform(*random.choice([(-10, -5), (5, 10)])) if weather_condition else random.uniform(-5, 5), 2), "unit": "Degrees"},
            "role": {"value": round(random.uniform(*random.choice([(-25, -20), (20, 25)])) if weather_condition else random.uniform(-20, 20), 2), "unit": "Degrees"},
            "trim": {"value": round(random.uniform(*random.choice([(-10, -5), (5, 10)])) if weather_condition else random.uniform(-5, 5), 2), "unit": "Degrees"},
            "heave": {"value": round(random.uniform(*random.choice([(-5, -2), (2, 5)])) if weather_condition else random.uniform(-2, 2), 2), "unit": "Degrees"},
            "pitch": {"value": round(random.uniform(*random.choice([(-15, -10), (10, 15)])) if weather_condition else random.uniform(-10, 10), 2), "unit": "Degrees"},
            "moored": {"value": 1 if maintenance else 0, "unit": "yes or no"},
            "maintenance": {"value": 1 if maintenance else 0, "unit": "yes or no"},
            "weather": {"value": 1 if weather_condition else 0, "unit": "yes or no"},
            "sailing": {"value": 0 if maintenance else 1, "unit": "yes or no"},
            "spog": {"value": round(0 if maintenance else (random.uniform(5, 30) if weather_condition else random.uniform(30, 60))), "unit": "kmph"},
            "wave_ht": {"value": round(random.uniform(0.5, 5) if weather_condition else random.uniform(0, 0.5)), "unit": "meters"},
            "water_tank1": {"value": round(random.uniform(0, 10) if random.random() < 1/1000 else random.uniform(10, 95)), "unit": "percentage"},
            "water_tank2": {"value": round(random.uniform(0, 10) if random.random() < 1/1000 else random.uniform(10, 95)), "unit": "percentage"},
            "gen1_running": {"value": round(0 if active_generator != 1 else 1), "unit": "kW"},
            "gen2_running": {"value": round(0 if active_generator != 2 else 1), "unit": "kW"},
            "gen3_running": {"value": round(0 if active_generator != 3 else 1), "unit": "kW"},
            "gen4_running": {"value": round(0 if active_generator != 4 else 1), "unit": "kW"},
            "gen1_power": {"value": round(0 if active_generator != 1 else random.uniform(100, 999)), "unit": "kW"},
            "gen2_power": {"value": round(0 if active_generator != 2 else random.uniform(100, 999)), "unit": "kW"},
            "gen3_power": {"value": round(0 if active_generator != 3 else random.uniform(100, 999)), "unit": "kW"},
            "gen4_power": {"value": round(0 if active_generator != 4 else random.uniform(100, 999)), "unit": "kW"},
            "thr1_running": {"value": round(0 if 1 not in active_thrustor else 1), "unit": "yes or no"},
            "thr2_running": {"value": round(0 if 2 not in active_thrustor else 1), "unit": "yes or no"},
            "thr3_running": {"value": round(0 if 3 not in active_thrustor else 1), "unit": "yes or no"},
            "thr4_running": {"value": round(0 if 4 not in active_thrustor else 1), "unit": "yes or no"},
            "fueloil_percentage": {"value": round(random.uniform(0, 10) if random.random() < 1/1000 else random.uniform(10, 100)), "unit": "%"},
            "battery_health": {"value": round(random.uniform(80, 100) if random.random() < 1/1000 else random.uniform(40, 80)), "unit": "%"}
        }
        return sensors

# Simulate sensor data for 10 grids
def simulate_sensor_data_for_grids():
    vessels = {
    'vessel_s1': [28931, 'ft1_s1', 51.948, 4.142],
    'vessel_s2': [69154, 'ft1_s2', 52.379, 4.900],
    'vessel_s3': [56490, 'ft1_s3', 52.460, 4.570],
    'vessel_s4': [14897, 'ft1_s4', 51.442, 3.573],
    'vessel_s5': [39073, 'ft1_s5', 52.959, 4.759],
    'vessel_s6': [83910, 'ft1_s6', 53.445, 6.837],
    'vessel_s7': [90860, 'ft1_s7', 53.331, 6.918],
    'vessel_s8': [26247, 'ft1_s8', 53.174, 5.414],
    'vessel_s9': [36907, 'ft1_s9', 51.335, 3.832],
    'vessel_s10': [38761, 't1_s10', 52.104, 4.273]
    }
    while True:
        for key, vs_value in vessels.items():
            print(key, vs_value[0], vs_value[1],  vs_value[2],  vs_value[3])
            vessel = EnergyGridSensorSimulator(vessel_id=key, vessel_details = vs_value, current_time_pas=datetime.now())
            sensor_data = vessel.get_sensor_data()
            vessel.log_event("sensor event", vessel.vessel_id,vs_value[0],vs_value[1], vs_value[2], vs_value[3],sensor_data)
            sleep_time = 12
            time.sleep(sleep_time)
            #self.current_time += timedelta(minutes=travel_time)

simulate_sensor_data_for_grids()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random
weather_condition = round(1 if random.random() < 1/1000 else 0)
maintenance = round(1 if random.random() < 1/1000 else 0)
print(weather_condition)
generators=[1,2,3,4]
active_generator = random.choice(pumps)
thrustor=[1,2,3,4]
choices = (1 if random.random() < 1/100 else random.choice([2,3]))
active_thrustor = random.sample(thrustor, choices)
sensors = {
    "heel": {"value": round(random.uniform(*random.choice([(-10, -5), (5, 10)])) if weather_condition else random.uniform(-5, 5), 2), "unit": "Degrees"},
    "role": {"value": round(random.uniform(*random.choice([(-25, -20), (20, 25)])) if weather_condition else random.uniform(-20, 20), 2), "unit": "Degrees"},
    "trim": {"value": round(random.uniform(*random.choice([(-10, -5), (5, 10)])) if weather_condition else random.uniform(-5, 5), 2), "unit": "Degrees"},
    "heave": {"value": round(random.uniform(*random.choice([(-5, -2), (2, 5)])) if weather_condition else random.uniform(-2, 2), 2), "unit": "Degrees"},
    "pitch": {"value": round(random.uniform(*random.choice([(-15, -10), (10, 15)])) if weather_condition else random.uniform(-10, 10), 2), "unit": "Degrees"},
    "moored": {"value": 1 if maintenance else 0, "unit": "yes or no"},
    "maintenance": {"value": 1 if maintenance else 0, "unit": "yes or no"},
    "weather": {"value": 1 if weather_condition else 0, "unit": "yes or no"},
    "sailing": {"value": 0 if maintenance else 1, "unit": "yes or no"},
    "spog": {"value": round(0 if maintenance else (random.uniform(5, 30) if weather_condition else random.uniform(30, 60))), "unit": "kmph"},
    "wave_ht": {"value": round(random.uniform(0.5, 5) if weather_condition else random.uniform(0, 0.5)), "unit": "meters"},
    "water_tank1": {"value": round(random.uniform(0, 10) if random.random() < 1/1000 else random.uniform(10, 95)), "unit": "percentage"},
    "water_tank2": {"value": round(random.uniform(0, 10) if random.random() < 1/1000 else random.uniform(10, 95)), "unit": "percentage"},
    "gen1_running": {"value": round(0 if active_generator != 1 else 1), "unit": "kW"},
    "gen2_running": {"value": round(0 if active_generator != 2 else 1), "unit": "kW"},
    "gen3_running": {"value": round(0 if active_generator != 3 else 1), "unit": "kW"},
    "gen4_running": {"value": round(0 if active_generator != 4 else 1), "unit": "kW"},
    "gen1_power": {"value": round(0 if active_generator != 1 else random.uniform(100, 999)), "unit": "kW"},
    "gen2_power": {"value": round(0 if active_generator != 2 else random.uniform(100, 999)), "unit": "kW"},
    "gen3_power": {"value": round(0 if active_generator != 3 else random.uniform(100, 999)), "unit": "kW"},
    "gen4_power": {"value": round(0 if active_generator != 4 else random.uniform(100, 999)), "unit": "kW"},
    "thr1_running": {"value": round(0 if 1 not in active_thrustor else 1), "unit": "yes or no"},
    "thr2_running": {"value": round(0 if 2 not in active_thrustor else 1), "unit": "yes or no"},
    "thr3_running": {"value": round(0 if 3 not in active_thrustor else 1), "unit": "yes or no"},
    "thr4_running": {"value": round(0 if 4 not in active_thrustor else 1), "unit": "yes or no"},
    "fueloil_percentage": {"value": round(random.uniform(0, 10) if random.random() < 1/1000 else random.uniform(10, 100)), "unit": "%"},
    "battery_health": {"value": round(random.uniform(80, 100) if random.random() < 1/1000 else random.uniform(40, 80)), "unit": "%"}
}

print(sensors)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
