from kafka import KafkaProducer 
import numpy as np
from time import time, sleep
from sys import argv, exit

DEVICES_PROFILE = {
    "boston": { 'temperature': (65, 75), 'humidity': (40, 60) },
    "newyork": { 'temperature': (55, 65), 'humidity': (30, 50) },
    "sanfrancisco": { 'temperature': (45, 55), 'humidity': (20, 40) }
}


profile_name = argv[1]
profile = DEVICES_PROFILE[profile_name]

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
    temperature = np.random.uniform(profile['temperature'][0], profile['temperature'][1])
    humidity = np.random.uniform(profile['humidity'][0], profile['humidity'][1])

    msg = f'{time()},{profile_name},{temperature}, {humidity}'


    # print(msg)

    producer.send('weather', bytes(msg, encoding='utf-8'))
    print('sending data to kafka') 
    sleep(1)

