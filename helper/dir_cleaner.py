import os
from dotenv import load_dotenv

load_dotenv()

DIR_EXTRACT = os.getenv("DIR_EXTRACT")
DIR_TRANSFORM = os.getenv("DIR_TRANSFORM")
DIR_LOAD = os.getenv("DIR_LOAD")

def clean_output():
    for file in os.listdir(f"{DIR_EXTRACT}"):
        os.remove(f"{DIR_EXTRACT}/{file}")
    for file in os.listdir(f"{DIR_TRANSFORM}"):
        os.remove(f"{DIR_TRANSFORM}/{file}")
    for file in os.listdir(f"{DIR_LOAD}"):
        os.remove(f"{DIR_LOAD}/{file}")