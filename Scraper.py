import requests
import json
import time
from logger import log
from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

config = dotenv_values(".env") # config = {"USER": "foo", "EMAIL": "foo@example.org"}

# BallChasing API
# Get replay meta data
def get_replays(playlist = 'ranked-doubles', **kwargs):
	bc_url = 'https://ballchasing.com/api/'
	bc_headers = {'Authorization' : config["BallChasingAPIKey"]}
	replays_url = f"{bc_url}replays"
	params = {'playlist':playlist}

	for key, value in kwargs.items():
		params[key.replace('_', '-')] = value
		print(f'{key}: {value}')
	print(params)
	response = requests.get(replays_url, headers=bc_headers, params=params)
	data = {}

	match response.status_code:
		case code if code >= 200 and code < 300:
			print('Success')
			data = json.loads(response.text)
			print(len(data['list']))
		case code if code >= 400 and code < 500:
			print(f'Client side error: {response.status_code}')
			log(message=response.text, error=True)
		case code if code >= 500:
			print('Server side error getting replay meta data from BC')
			log(message=response.text, error=True)

	return data

# Get individual replay data
def get_replay_data(replay_id):
	bc_url = 'https://ballchasing.com/api/'
	bc_headers = {'Authorization' : config["BallChasingAPIKey"]}

	replay_url = f"{bc_url}replays/{replay_id}"
	response = requests.get(replay_url, headers=bc_headers)
	print(response.status_code)
	if response.status_code >= 500:
		print('Error getting replay data from BC')
		print(response)
		exit()
	data = json.loads(response.text)
	return data

# MongoDB
def insert_replays(data, playlist):
	db_uri = config["MongoDBURI"]

	# Create a new client and connect to the server
	client = MongoClient(db_uri, server_api=ServerApi('1'))
	db_name = playlist.replace('-', ' ').title().replace(' ', '')
	database = client[db_name]
	if 'ReplayMetaData' not in database.list_collection_names():
		database.create_collection('ReplayMetaData')
	collection = database['ReplayMetaData']

	# Send a ping to confirm a successful connection
	try:
		client.admin.command('ping')
		print("Pinged your deployment. You successfully connected to MongoDB!")
	except Exception as e:
		log(message=e, error=True)

	try:
		collection.insert_many(data['list'], ordered=False)
		print('Insert Successful!')
	except Exception as e:
		log(message=e, error=True)

if __name__ == "__main__":
	playlist = 'ranked-standard'
	years = ["2020"] #, "2021", "2022", "2023", "2024"]
	months = ["02"] #, "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
	days = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28"]
	hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]
	for year in years:
		for month in months:
			for day in days:
				for hour in hours:
					replays = get_replays(playlist = playlist, replay_date_after = f'{year}-{month}-{day}T{hour}:00:00Z', replay_date_before = f'{year}-{month}-{day}T{hour}:59:59Z')
					if (replays):
						insert_replays(replays, playlist)
					time.sleep(1)
	print("Success")
