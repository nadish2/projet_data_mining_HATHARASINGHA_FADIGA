# -*- coding: utf-8 -*-
"""
Created on Fri Jan  7 01:15:44 2022

@author: nadee
"""

   
#Envoie un fichier json ligne ligne par ligne dans le bus Kafka
import json
from kafka import KafkaProducer
import time
#On se connecte à la machine Kafka
producer = KafkaProducer(bootstrap_servers='192.168.33.13:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

#On récupère chaque ligne du fichier json dans une liste 
with open('data.csv', 'r') as f:
    listusers= f.readlines()


for i in listusers:
    user=i.strip()
    print(user)
    producer.send('projet_datamining', user)
    time.sleep(10) 
    