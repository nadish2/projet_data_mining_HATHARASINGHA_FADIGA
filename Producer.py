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
with open('dataessai.csv', 'r') as f:
    listusers= f.readlines()


for i in listusers:
    user=i.strip()
    print(user)
    producer.send('projet_datamining', user)
    time.sleep(5) 
    

#"id","diagnosis","radius_mean","texture_mean","perimeter_mean","area_mean","smoothness_mean","compactness_mean","concavity_mean","concave points_mean","symmetry_mean","fractal_dimension_mean","radius_se","texture_se","perimeter_se","area_se","smoothness_se","compactness_se","concavity_se","concave points_se","symmetry_se","fractal_dimension_se","radius_worst","texture_worst","perimeter_worst","area_worst","smoothness_worst","compactness_worst","concavity_worst","concave points_worst","symmetry_worst","fractal_dimension_worst",
#id,gender,age,hypertension,heart_disease,ever_married,work_type,Residence_type,avg_glucose_level,bmi,smoking_status,stroke
