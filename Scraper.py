import requests
import json
import time
import sys
import os
import asyncio
from pprint import pprint
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
			print('Successfully retrieved replay meta data')
			data = json.loads(response.text)
			print(len(data['list']))
		case code if code >= 400 and code < 429:
			print(f'Client side error during replay meta data retrieval: {response.status_code}')
			log(message=response.text, error=True)
		case code if code == 429:
			print(f"Rate limit hit during replay meta data retrieval, exiting at {time.ctime()}")
			log(message=f'Rate limit hit', error=True)
			sys.exit()
		case code if code >= 500:
			print('Server side error during replay meta data retrieval')
			log(message=response.text, error=True)

	return data

# Get individual replay data
def get_replay_data(replay_id):
	bc_url = 'https://ballchasing.com/api/'
	bc_headers = {'Authorization' : config["BallChasingAPIKey"]}

	replay_url = f"{bc_url}replays/{replay_id}"
	response = requests.get(replay_url, headers=bc_headers)

	data = {}

	match response.status_code:
		case code if code >= 200 and code < 300:
			print(f'Successfully retrieved replay {replay_id} data')
			data = json.loads(response.text)
			print(len(data['list']))
		case code if code >= 400 and code < 429:
			print(f'Client side error during replay {replay_id} data retrieval: {response.status_code}')
			log(message=response.text, error=True)
		case code if code == 429:
			print(f"Rate limit hit during replay {replay_id} data retrieval, exiting")
			log(message=f'Rate limit hit', error=True)
			sys.exit()
		case code if code >= 500:
			print(f'Server side error during replay {replay_id} data retrieval')
			log(message=response.text, error=True)

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

def meta_data_scraper():
	# Season 18 is March 14 - June 18
	playlist = 'ranked-standard'
	years = ["2025"]
	months = ["06"] #, "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
	days = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30"] #, "31"]
	hours = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"]
	for year in years:
		for month in months:
			for day in days:
				for hour in hours:
					replays = get_replays(playlist = playlist, replay_date_after = f'{year}-{month}-{day}T{hour}:00:00Z', replay_date_before = f'{year}-{month}-{day}T{hour}:59:59Z')
					if (replays):
						insert_replays(replays, playlist)
					time.sleep(1)
	print(f"Successfully completed at {time.ctime()}")

async def get_mongo_data(playlist):
	db_uri = config["MongoDBURI"]
	client = MongoClient(db_uri, server_api=ServerApi('1'))
	db_name = playlist.replace('-', ' ').title().replace(' ', '')
	database = client[db_name]
	collection = database['ReplayMetaData']

	cursor = collection.find({})
	# data = await cursor.to_list()
	return cursor

def save_data_to_file(data, filename):
	dir_path = config['DIR_PATH']

	if not os.path.exists(f'{dir_path}/data'):
		os.mkdir(f'{dir_path}/data')

	file_path = f'{dir_path}/data/{filename}'
	with open(file_path, 'w') as file:
		json.dump(data, file, indent=4)

if __name__ == "__main__":
	#meta_data_scraper()
	data = asyncio.run(get_mongo_data('ranked-standard'))
	all_data = []
	counter = 0
	for entry in data:
		print(f"Link: {entry['link']}, Date: {entry['date']}")
		entry.pop('_id') # This removes the MongoDB assigned object ID, which I don't use for anything but gives the json package trouble
		all_data.append(entry)
		counter += 1
		if counter > 10:
			break
	pprint(all_data)
	save_data_to_file(all_data, 'test')


#{"min_rank": {"$exists": true}, "min_rank.name": {"$regex": "grand champion", "$options": "i"}}
# This query will return all entries where the min_rank field exists and the name of the min_rank is "grand champion"
# This is a query for the ranked-standard playlist
