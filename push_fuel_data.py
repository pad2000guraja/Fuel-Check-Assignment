import requests
import pandas as pd
import uuid
from datetime import datetime
import base64
import time
import msgpack
import paho.mqtt.client as mqtt
import os

class FuelDataClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
        self.full_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"
        self.new_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices/new"
        self.first_run = True

    def get_access_token(self):
        headers = {
            "Authorization": "Basic " + base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode(),
            "Content-Type": "application/json"
        }
        params = {"grant_type": "client_credentials"}
        response = requests.get(self.token_url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()['access_token']

    def fetch_data(self):
        token = self.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            "apikey": self.client_id,
            "transactionid": str(uuid.uuid4()),
            "requesttimestamp": datetime.utcnow().strftime("%d/%m/%Y %I:%M:%S %p"),
        }
        # fetch the entire prices in the first call, then switch to the second api to fetch only the updated prices
        url = self.full_url if self.first_run else self.new_url
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        self.first_run = False
        stations = pd.json_normalize(data.get("stations", []))
        prices = pd.json_normalize(data.get("prices", []))
        return stations, prices

# clean and merge station and price data
def clean_and_merge(stations, prices):
    stations = stations.rename(columns={
        'code': 'stationcode',
        'name': 'station_name',
        'location.latitude': 'lat',
        'location.longitude': 'lon'
    }).dropna(subset=['lat', 'lon'])

    stations['lat'] = pd.to_numeric(stations['lat'], errors='coerce')
    stations['lon'] = pd.to_numeric(stations['lon'], errors='coerce')
    
    prices['price'] = pd.to_numeric(prices['price'], errors='coerce')
    prices['lastupdated'] = pd.to_datetime(prices['lastupdated']).dt.strftime('%d %b %Y %I:%M %p')

    
    prices = prices[prices['price'] > 0]

    latest_prices = prices.sort_values("lastupdated").groupby(['stationcode', 'fueltype']).last().reset_index()
    merged_data = pd.merge(latest_prices, stations, on='stationcode', how='left')
    return merged_data

# store the last published price for each (station, fueltype)
last_sent_prices = {}

# publish only new or changed records to the MQTT broker
def publish_if_changed(client, fuel_data):
    global last_sent_prices

    for _, row in fuel_data.iterrows():
        record = row.dropna().to_dict()
        key = f"{record.get('stationcode')}_{record.get('fueltype')}"
        current_price = record.get('price')

        if last_sent_prices.get(key) != current_price:
            payload = msgpack.packb(record)
            client.publish("fuel/new_prices", payload, qos=0)
            last_sent_prices[key] = current_price
            
        time.sleep(0.1)

def main():
    client_id = 'CvRQC1qC8akmwp9Qfy5owgzWk8izoa9Q'
    client_secret = 'SNmuC7nzn3IISVcG'

    mqtt_client = mqtt.Client()
    mqtt_client.connect("broker.hivemq.com", 1883, 60)
    mqtt_client.loop_start()

    fuel_client = FuelDataClient(client_id, client_secret)

    #call the api every 60 after sending all the messages to the broker
    while True:
        try:
            print("fetching fuel data started")
            stations, prices = fuel_client.fetch_data()
            if not prices.empty:
                
                combined_data = clean_and_merge(stations, prices)
                
                #save to csv and append the new records. 
                combined_data.to_csv("fuel_prices_latest.csv", mode='a', header=not os.path.exists("fuel_prices_latest.csv"), index=False)
                print(f"Saved {len(combined_data)} records to 'fuel_prices_latest.csv'")
                
                publish_if_changed(mqtt_client, combined_data)
            else:
                print("no new price data received")
            print("waiting 60 seconds for next update\n")
        except Exception as e:
            print(f"error occurred: {e}")
        time.sleep(60)

if __name__ == "__main__":
    main()