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

evh_name = "<enter Event Hub name Here>"
evh_conn_string = "<eneter Event Hub Connection String>"


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

all_euorpe_countries = {
    "Albania": {"capital": "Tirana", "latitude": 41.3275, "longitude": 19.8189},
    "Andorra": {"capital": "Andorra la Vella", "latitude": 42.5078, "longitude": 1.5211},
    "Austria": {"capital": "Vienna", "latitude": 48.2082, "longitude": 16.3738},
    "Belarus": {"capital": "Minsk", "latitude": 53.9006, "longitude": 27.5590},
    "Belgium": {"capital": "Brussels", "latitude": 50.8503, "longitude": 4.3517},
    "Bosnia and Herzegovina": {"capital": "Sarajevo", "latitude": 43.8563, "longitude": 18.4131},
    "Bulgaria": {"capital": "Sofia", "latitude": 42.6977, "longitude": 23.3219},
    "Croatia": {"capital": "Zagreb", "latitude": 45.8150, "longitude": 15.9819},
    "Cyprus": {"capital": "Nicosia", "latitude": 35.1856, "longitude": 33.3823},
    "Czech Republic": {"capital": "Prague", "latitude": 50.0755, "longitude": 14.4378},
    "Denmark": {"capital": "Copenhagen", "latitude": 55.6761, "longitude": 12.5683},
    "Estonia": {"capital": "Tallinn", "latitude": 59.4370, "longitude": 24.7536},
    "Finland": {"capital": "Helsinki", "latitude": 60.1695, "longitude": 24.9354},
    "France": {"capital": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    "Germany": {"capital": "Berlin", "latitude": 52.5200, "longitude": 13.4050},
    "Greece": {"capital": "Athens", "latitude": 37.9838, "longitude": 23.7275},
    "Hungary": {"capital": "Budapest", "latitude": 47.4979, "longitude": 19.0402},
    "Iceland": {"capital": "Reykjavik", "latitude": 64.1355, "longitude": -21.8954},
    "Ireland": {"capital": "Dublin", "latitude": 53.3498, "longitude": -6.2603},
    "Italy": {"capital": "Rome", "latitude": 41.9028, "longitude": 12.4964},
    "Latvia": {"capital": "Riga", "latitude": 56.9496, "longitude": 24.1052},
    "Liechtenstein": {"capital": "Vaduz", "latitude": 47.1416, "longitude": 9.5215},
    "Lithuania": {"capital": "Vilnius", "latitude": 54.6872, "longitude": 25.2797},
    "Luxembourg": {"capital": "Luxembourg", "latitude": 49.6117, "longitude": 6.1319},
    "Malta": {"capital": "Valletta", "latitude": 35.8997, "longitude": 14.5146},
    "Moldova": {"capital": "Chisinau", "latitude": 47.0105, "longitude": 28.8638},
    "Monaco": {"capital": "Monaco", "latitude": 43.7384, "longitude": 7.4246},
    "Montenegro": {"capital": "Podgorica", "latitude": 42.4304, "longitude": 19.2594},
    "Netherlands": {"capital": "Amsterdam", "latitude": 52.3676, "longitude": 4.9041},
    "North Macedonia": {"capital": "Skopje", "latitude": 41.9981, "longitude": 21.4254},
    "Norway": {"capital": "Oslo", "latitude": 59.9139, "longitude": 10.7522},
    "Poland": {"capital": "Warsaw", "latitude": 52.2297, "longitude": 21.0122},
    "Portugal": {"capital": "Lisbon", "latitude": 38.7169, "longitude": -9.1399},
    "Romania": {"capital": "Bucharest", "latitude": 44.4268, "longitude": 26.1025},
    "Russia": {"capital": "Moscow", "latitude": 55.7558, "longitude": 37.6173},
    "San Marino": {"capital": "San Marino", "latitude": 43.9333, "longitude": 12.4500},
    "Serbia": {"capital": "Belgrade", "latitude": 44.7866, "longitude": 20.4489},
    "Slovakia": {"capital": "Bratislava", "latitude": 48.1486, "longitude": 17.1077},
    "Slovenia": {"capital": "Ljubljana", "latitude": 46.0569, "longitude": 14.5058},
    "Spain": {"capital": "Madrid", "latitude": 40.4168, "longitude": -3.7038},
    "Sweden": {"capital": "Stockholm", "latitude": 59.3293, "longitude": 18.0686},
    "Switzerland": {"capital": "Bern", "latitude": 46.9481, "longitude": 7.4474},
    "Ukraine": {"capital": "Kyiv", "latitude": 50.4501, "longitude": 30.5234},
    "United Kingdom": {"capital": "London", "latitude": 51.5074, "longitude": -0.1278},
    "Vatican City": {"capital": "Vatican City", "latitude": 41.9029, "longitude": 12.4534}
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random, math, json
import random
import time
import uuid
from datetime import datetime, timedelta

class EnergyGridSensorSimulator:
    def __init__(self, current_time_pas):
        # self.grid_id = grid_id
        self.unique_id = str(uuid.uuid4())
        self.current_time = current_time_pas

    def log_event(self, activity, sensor_data):
        event = {
            "timestamp": self.current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_id": self.unique_id,
            "source": activity,
            # "grid_id": grid_id,
            "sensor_data": sensor_data
        }
        log_event = event
        log_obj = json.dumps(log_event)
        #impressionEvent = json_object    
        sendToEventsHub(log_event, producer_events)
        #print(log_event)

    def get_sensor_data(self, source, country, details):
        #sources = ["thermal", "wind", "solar", "coal", "natural gas"]
        data = {}

        # for source in sources:
        production_value = round(random.uniform(300, 500) if random.random() < 1/1000 else random.uniform(100, 300), 0)

        # Ensure consumption < production only 1 in 10 times
        if random.random() > 1/20:
            consumption_value = round(random.uniform(0.5 * production_value, production_value), 0)
        else:
            consumption_value = round(random.uniform(1.1 * production_value, 2 * production_value), 0)

        # Calculate price based on the difference
        percentage = round((consumption_value / production_value) * 100, 0)

        if percentage < 70:
            price = round(random.uniform(-10, 30), 0)
        elif percentage < 80:
            price = round(random.uniform(30, 50), 0)
        elif percentage < 90:
            price = round(random.uniform(50, 60), 0)
        elif percentage < 100:
            price = round(random.uniform(60, 100), 0)
        else:
            # print("greater")
            price = round(random.uniform((100 + ((percentage-100)/5)*10), 300), 0)

        
        if source in ["coal", "natural gas"]:
            base_emission = 20
            emission_multiplier = math.floor(max((production_value - 50),0) / 10)
            co2_emission = round(base_emission + emission_multiplier * 10, 0)    
        else:
            base_emission = 50
            emission_multiplier = math.floor(max((production_value - 50),0) / 10)
            co2_emission = round(base_emission + emission_multiplier * 5, 0)
        
        

        #print(production_value, consumption_value, percentage, price, co2_emission)

        data = {
            "country": country,
            "capital":details['capital'],
            "latitude":details['latitude'],
            "longitude":details['longitude'],
            "production": production_value,
            "consumption": consumption_value,
            "co2_emission": co2_emission,
            "hourly_price": price
        }

        return data

def simulate_energy_data():
    start_time = datetime.now() - timedelta(days=3)
    counter = 0

    while True:
        for country, details in all_euorpe_countries.items():
            current_time_pas = start_time + timedelta(seconds=80 * counter)
            grid = EnergyGridSensorSimulator(current_time_pas)

            for src in ["thermal", "wind", "solar", "coal", "natural gas"]:
                sensor_data = grid.get_sensor_data(src, country, details)
                grid.log_event(src, sensor_data)

            counter += 1
            print(f"Counter: {counter}, Time: {current_time_pas}, Country: {country}")

            # Determine if we need to sleep
            if current_time_pas >= datetime.now():
                time.sleep(80)
            else:
                time.sleep(1)
    # # grids = [EnergyGridSensorSimulator(grid_id=i, current_time_pas=datetime.now()) for i in range(1, 11)]
    # # grid
    
    # start_time = datetime.now() - timedelta(days=3)
    # counter = 0
    # current_time_pas = start_time + timedelta(seconds=80*counter)
    # while True:
        
    #     for country, details in all_euorpe_countries.items():
    #         # if datetime.now() < current_time_pas:
    #         #     current_time_pas = datetime.now()
    #         # else:
    #         current_time_pas = start_time + timedelta(seconds=80*counter)
    #         grid = EnergyGridSensorSimulator( current_time_pas)
    #         for src in ["thermal", "wind", "solar", "coal", "natural gas"]:
    #             sensor_data = grid.get_sensor_data(src, country, details)
    #             grid.log_event(src, sensor_data)
    #         counter = counter + 1
    #         print(counter)
    #         #self.current_time += timedelta(minutes=travel_time)

    #     sleep_time = 2
    #     #print("---------------------------------------")
    #     time.sleep(sleep_time)
    #     # if datetime.now() < current_time_pas:
    #     #     break
    #         # sleep_time = 80
    #         # time.sleep(sleep_time)

simulate_energy_data()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
