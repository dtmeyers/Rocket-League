import os
import json
from dotenv import dotenv_values

config = dotenv_values(".env") # config = {"USER": "foo", "EMAIL": "foo@example.org"}

def save_data_to_file(data, filename):
	dir_path = config['DIR_PATH']

	if not os.path.exists(f'{dir_path}/data'):
		os.mkdir(f'{dir_path}/data')

	file_path = f'{dir_path}/data/{filename}'
	with open(file_path, 'w') as file:
		json.dump(data, file, indent=4)
