from datetime import datetime, timedelta
import json
from io import BytesIO
from gzip import GzipFile
import io
import sys
import gzip
import time
import logging
import pandas as pd
import snowflake.connector
import utils
import stations
from configparser import RawConfigParser
import model 

cfg = RawConfigParser()

def getCmdLineParser():
	import argparse
	desc = 'Execute divvy_ml'
	parser = argparse.ArgumentParser(description=desc)

	parser.add_argument('-c', '--config_file', default='../config/divvy_ml.ini',
		               help='configuration file name (*.ini format)')

	return parser
	
	
def main(argv):
	# Overhead to manage command line opts and config file
	p = getCmdLineParser()
	args = p.parse_args()
	cfg.read(args.config_file)

	logger = utils.initLog(cfg)

	log_line = {"function": "main", "event_type": "Start", "event": "Connecting to Snowflake", 
			 		"user":cfg.get('snowflake','user'),"account":cfg.get('snowflake','account'), "warehouse":cfg.get('snowflake','warehouse'),
			 		"database":cfg.get('snowflake','database'),"schema":cfg.get('snowflake','schema'), "role":cfg.get('snowflake','role')
			}
	logger.info(json.dumps(log_line))
	tick = datetime.now()
	snowflake_connection = snowflake.connector.connect(
		 user=cfg.get('snowflake','user'),
		 password=cfg.get('snowflake','password'),
		 account=cfg.get('snowflake','account'),
		 warehouse=cfg.get('snowflake','warehouse'),
		 database=cfg.get('snowflake','database'),
		 schema=cfg.get('snowflake','schema'),
		 role=cfg.get('snowflake','role')
	 )
	dur = datetime.now() - tick
	log_line = {"function": "main", "event_type": "End", "event": "Connecting to Snowflake", "duration":dur.total_seconds()}
	logger.info(json.dumps(log_line))

	station_list = stations.LoadStations(snowflake_connection, cfg, '2019-06-30')
	model.save_poisson_results(snowflake_connection, cfg, station_list, include_rebalance = False)
	snowflake_connection.close()


if __name__ == "__main__":
	main(sys.argv[1:])
