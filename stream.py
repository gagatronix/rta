#!/usr/bin/env python
# coding: utf-8

# In[4]:


import json
import numpy as np
import pandas as pd
import random
import sys
from datetime import datetime, timedelta
from time import sleep

from kafka import KafkaProducer

KAFKA_TOPIC = 'urzedy'
SERVER = "broker:9092"
LAG = 3

# Warszawa districts and streets
district_street_combinations = [
    ("Bemowo", "Powstańców Śląskich"),
    ("Bielany", "Żeromskiego"),
    ("Mokotów", "Rakowiecka"),
    ("Ochota", "Grójecka"),
    ("Praga Południe", "Grochowska",),
    ("Praga Północ", "Kłopotowskiego"),
    ("Rembertów", "Chruściela Montera"),
    ("Targówek", "Kondratowicza"),
    ("Wawer", "Żegańska"),
    ("Wola", "Solidarności")]


# Urzedy i kolejki
office_types = [
    "Architektura, geodezja, ochrona środowiska, infrastruktura",
    "Dowody osobiste - składanie wniosków",
    "Działalność gospodarcza",
    "PESEL dla obywateli Ukrainy",
    "Rejestracja pojazdów - składanie wniosków",
    "Wybory"]

sprawy = pd.read_excel('https://jakubreniec.s3.eu-north-1.amazonaws.com/kolejki.xlsx')
sprawy = sprawy.set_index(['Sprawa','Dzielnica'])

district_street_index = 0
office_index = 0

if __name__ == "_main_":
    
    producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"), #polskie znaki
        api_version=(3, 7, 0),
    )
    
    try:
        while True:
            t = datetime.now() + timedelta(seconds=random.randint(-15, 0))
                    
            # Generate data for district and street
            district, street = district_street_combinations[district_street_index]
            if office_index == len(office_types) -1:
                district_street_index = (district_street_index + 1) % len(district_street_combinations)
            
            office = office_types[office_index]
            office_index = (office_index + 1) % len(office_types)
            
            queue_length = int(round(max(0, np.random.normal(sprawy.loc[(office, district), 'L_osob_srednia']), sprawy.loc[(office, district), 'L_osob_odch']),0))
            number_of_widnows = int(round(max(1, np.random.normal(sprawy.loc[(office, district), 'L_okienek_srednia']), sprawy.loc[(office, district), 'L_okienek_odch']),0))
    
    
            estimated_wait_time = int(round(queue_length /  number_of_widnows * random.uniform(2, 5),0))
            # czas oczekiwania ma rozkład jednostajny na przedziale (2,5)
            # czas zależy od tego jacy ludzie przychodzą i od humoru urzędników 
            
            message = {
                    "Godzina": str(t),
                    "Miasto": "Warszawa",
                    "Dzielnica": district,
                    "Ulica": street,
                    "Rodzaj_sprawy": office,
                    "Długość_kolejki": queue_length,
                    "Liczba stanowisk": number_of_widnows,
                    "Czas_oczekiwania": estimated_wait_time  # Estymowany czas oczekiwania w minutach
                }
        

            producer.send(KAFKA_TOPIC, value=message)
        
            sleep(LAG)

    except KeyboardInterrupt:
        producer.close()

