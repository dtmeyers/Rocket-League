import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

dir_path = os.getenv("DIR_PATH")

def log(message = '', error = False):
	# Create logs directory if it doesn't exist
	if not os.path.exists(f'{dir_path}/logs'):
		os.mkdir(f'{dir_path}/logs')

	if error:
		full_path = f'{dir_path}/logs/error.log'
	else:
		full_path = f'{dir_path}/logs/info.log'

	with open(full_path, 'a') as file:
		file.write(f"{datetime.now()} {message}\n")
