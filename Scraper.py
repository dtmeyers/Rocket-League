import requests
import json
from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

config = dotenv_values(".env") # config = {"USER": "foo", "EMAIL": "foo@example.org"}

url = 'https://ballchasing.com/api/'
headers = {'Authorization' : config["BallChasingAPIKey"]}
api_key = {'Public' : 'egreacgj', 'Private' : config["MongoDBAPIKey"]}

replays_url = f"{url}replays"
params = {'playlist':'ranked-doubles'}
response = requests.get(replays_url, headers=headers, params=params)
print(response.status_code)
data = json.loads(response.text)
print(len(data['list']))
first_match = data['list'][0]
print(json.dumps(first_match, indent=2))
replay = first_match['id']
replay_url = f"{replays_url}/{replay}"
response = requests.get(replay_url, headers=headers)
game = json.loads(response.text)
print(json.dumps(game, indent=2))

uri = config["MongoDBURI"]

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
database = client['RankedDoubles']
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
