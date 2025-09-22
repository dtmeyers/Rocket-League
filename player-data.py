import os
import json
import utilities
from logger import log
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()
dir_path = os.getenv('DIR_PATH')
data_path = f"{dir_path}/data"
test_path = f"{data_path}/test-games"
games_path = f"{data_path}/Games"

def save_player_data(player_id, match_id, player_data, team_data):
	player_path = f"{dir_path}/data/Players/"
	player_data['team_stats'] = team_data
	if player_id in os.listdir(player_path):
		with open(f"{player_path}{player_id}", 'r+') as file:
			data = json.load(file)
			if match_id in data:
				return 0
			data[match_id] = player_data
			file.seek(0) #move point back to beginning of file
			json.dump(data, file, indent=4)
		return 0
	else:
		data = {match_id : player_data}
		with open(f"{player_path}{player_id}", 'w') as file:
			json.dump(data, file, indent=4)
	return 1


players = {}
new_player_count = 0
for entry in os.listdir(f"{games_path}/"):
	try:
		with open(f"{games_path}/{entry}", 'r') as file:
			data = json.load(file)
	except Exception as e:
		log(message=f"File load error {e} with file: {entry}", error=True)

	try:

		for game in data:
			if game['match_details'] == {}:
				continue
			match_details = game['match_details']
			match_id = game['id']
			blue_team = match_details['blue']
			orange_team = match_details['orange']
			for player in blue_team['players']:
				team_stats = blue_team['stats']
				player_id = f"{player['id']['platform']}-{player['id']['id']}"
				new_player_count += save_player_data(player_id, match_id, player, team_stats)
			for player in orange_team['players']:
				team_stats = orange_team['stats']
				player_id = f"{player['id']['platform']}-{player['id']['id']}"
				new_player_count += save_player_data(player_id, match_id, player, team_stats)
	except Exception as e:
		log(message=f"Missing game information with file: {entry} - game: {game['id']}", error=True)

#utilities.save_data_to_file(players, 'Players/players')
print(new_player_count)
