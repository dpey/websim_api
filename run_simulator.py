import requests
from requests import Session
import urllib
import pandas as pd
import numpy as np
import getpass
import time
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib
import matplotlib.ticker as ticker
import json
import simulator
import warnings

def read_json(path):
	with open(path) as f:
		return json.load(f)

def write_json(data, path):
	with open(path, 'wb') as f:
		json.dump({'alphas':data}, f)

		
if __name__ == '__main__':

	print('Simulator')
	warnings.filterwarnings("ignore")

	path_input = 'input/'
	path_data = 'data/'

	filenames = ["p.json"]

	for filename in filenames:
		alphas_list = read_json(path_input + filename)['alphas']

		s1 = simulator.simulator()
		s1.debug_on()
		s1.log_in()
		s1.load_df_input(alphas_list)
		s1.multi_research(6)
		s1.save_log