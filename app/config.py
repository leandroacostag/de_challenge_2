import load_dotenv
import os

# Read the .env file
load_dotenv.load_dotenv()

process_all = bool(int(os.getenv("PROCESS_ALL", 0)))
logs_level = os.getenv("LOGS_LEVEL", "INFO")
input_data_path = os.getenv("INPUT_DATA_PATH", "./data/input")
output_data_path = os.getenv("OUTPUT_DATA_PATH", "./data/output")
