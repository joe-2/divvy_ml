from datetime import datetime
import logging
import json
import pandas as pd
import sys

logger = logging.getLogger('divvy_ml')

def LoadStations(snowflake_connection, cfg, the_day):

	qry = cfg.get('stations','station_list_query').replace('the_day', the_day)
	cols = cfg.get('stations','station_list_columns').split(',')

	log_line = {"function": "LoadStations", "event_type": "Start", "event": "Loading Stations", 
			 		"date":the_day, "query":qry
					}
	logger.info(json.dumps(log_line))
	tick = datetime.now()

	cs = snowflake_connection.cursor()
	df = pd.DataFrame()
	try:
		cur = cs.execute(qry)
		df = pd.DataFrame.from_records(iter(cur), columns=cols)
		dur = datetime.now() - tick
		log_line = {"function": "LoadStations", "event_type": "End", "event": "Loading Stations", "duration": dur.total_seconds(), "station_df_shape":df.shape, "station_df_types":str(df.dtypes)}
		logger.info(json.dumps(log_line))
	except Exception as e:
		dur = datetime.now() - tick
		log_line = {"function": "LoadStations", "event_type": "End", "event": "Loading Stations", "duration": dur.total_seconds(), "error":str(e)}
		logger.error(json.dumps(log_line))
		sys.exit(str(e))
	finally:
		cs.close()

	return df


def GetStationData(snowflake_connection, cfg, the_station_id, the_latitude, the_longitude):

	#TODO: fix the int() call here...no idea why we need it.
	qry = (cfg.get('stations','get_station_query')).replace('the_station_id',str(int(the_station_id))).replace('the_latitude',str(the_latitude)).replace('the_longitude',str(the_longitude))
	cols = cfg.get('stations','get_station_columns').split(',')

	log_line = {"function": "GetStationData", "event_type": "Start", "event": "Getting Station", "query":qry}
	logger.info(json.dumps(log_line))
	tick = datetime.now()

	cs = snowflake_connection.cursor()
	df = pd.DataFrame()
	try:
		cur = cs.execute(qry)
		df = pd.DataFrame.from_records(iter(cur), columns=cols, index="timestamp")
		dur = datetime.now() - tick
		log_line = {"function": "GetStationData", "event_type": "End", "event": "Getting Station", "duration": dur.total_seconds(), "station_df_shape":df.shape, "station_df_types":str(df.dtypes)}
		logger.info(json.dumps(log_line))
	except Exception as e:
		dur = datetime.now() - tick
		log_line = {"function": "GetStationData", "event_type": "End", "event": "Getting Station", "duration": dur.total_seconds(), "error":str(e)}
		logger.error(json.dumps(log_line))
		sys.exit(str(e))
	finally:
		cs.close()

	return df

