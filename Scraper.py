import requests
import json
from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

config = dotenv_values(".env") # config = {"USER": "foo", "EMAIL": "foo@example.org"}

# BallChasing API
def get_replays(playlist):
	bc_url = 'https://ballchasing.com/api/'
	bc_headers = {'Authorization' : config["BallChasingAPIKey"]}

	replays_url = f"{bc_url}replays"
	params = {'playlist':playlist}
	response = requests.get(replays_url, headers=bc_headers, params=params)
	print(response.status_code)
	if response.status_code >= 500:
		print('Error getting replay meta data from BC')
		print(response)
		exit()
	data = json.loads(response.text)
	print(len(data['list']))
	first_match = data['list'][0]
	print(json.dumps(first_match, indent=2))
	replay = first_match['id']
	replay_url = f"{replays_url}/{replay}"
	response = requests.get(replay_url, headers=bc_headers)
	if response.status_code >= 500:
		print('Error getting replay data from BC')
		print(response)
		exit()
	game = json.loads(response.text)
	print(json.dumps(game, indent=2))
	return data

# MongoDB
def insert_replays(data):
	db_uri = config["MongoDBURI"]

	# Create a new client and connect to the server
	client = MongoClient(db_uri, server_api=ServerApi('1'))
	database = client['RankedDoubles']
	if 'ReplayMetaData' not in database.list_collection_names():
		database.create_collection('ReplayMetaData')
	collection = database['ReplayMetaData']

	# Send a ping to confirm a successful connection
	try:
		client.admin.command('ping')
		print("Pinged your deployment. You successfully connected to MongoDB!")
	except Exception as e:
		print(e)

	try:
		collection.insert_many(data['list'], ordered=False)
		print('Insert Successful!')
	except Exception as e:
		print(e)

if __name__ == "__main__":
	replays = get_replays('ranked-doubles')
	insert_replays(replays)
	print("Success")
