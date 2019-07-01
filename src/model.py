import pandas as pd
import numpy as np
import patsy
import statsmodels.api as sm
import pickle
import logging
import json
from datetime import datetime
import stations

logger = logging.getLogger('divvy_ml')

def rebalance_station_poisson_data(station_updates, station_id, time_interval, include_rebalance = False):

	log_line = {"function": "rebalance_station_poisson_data", "event_type": "Start", "event": "setting up arrivals_departures", "station_id":station_id, "include_rebalance":include_rebalance}
	logger.info(json.dumps(log_line))
	tick = datetime.now()

	# Find changes (deltas) in bike count
	bikes_available = station_updates.bikes_available

	# Calculate the Changes in Bikes
	deltas = bikes_available - bikes_available.shift()

	# Include Rebalancing Data and Limit Observations to Window where Rebalancing Data Exists
	if (include_rebalance == True):
		rebalances = calc_non_rebalance_change(int(station_id), '1H')
		rebalances.index = rebalances.index.tz_localize('UTC').tz_convert('US/Central')
		minimum_rebalance_data = min(rebalances.index)

		# Separate Departure and Arrival of Rebalancing Bikes

		pos_adj_for_rebalances = rebalances[rebalances < 0]
		neg_adj_for_rebalances = rebalances[rebalances > 0]

		pos_deltas = deltas[deltas > 0]
		neg_deltas = deltas[deltas < 0]

		pos_interval_counts_null = pos_deltas.resample(time_interval, how ='sum')
		neg_interval_counts_null = neg_deltas.resample(time_interval, how ='sum')

		pos_interval_counts = pos_interval_counts_null.fillna(0)
		neg_interval_counts = neg_interval_counts_null.fillna(0)

		# Add the Rebalance Data to the Arrival and Departure Data
		# Can cause arrivals to become departures and vice versa.

		rebalanced_pos_deltas_interval_unadj = pos_interval_counts.add(pos_adj_for_rebalances, fill_value=0)
		rebalanced_neg_deltas_interval_unadj = neg_interval_counts.add(neg_adj_for_rebalances, fill_value=0)


		# Identify the Cases where rebalance causes aggregate positives to become negative and
		# adjust departure numbers.

		pos_to_neg_deltas_interval = rebalanced_pos_deltas_interval_unadj[rebalanced_pos_deltas_interval_unadj < 0]
		neg_to_pos_deltas_interval = rebalanced_neg_deltas_interval_unadj[rebalanced_neg_deltas_interval_unadj > 0]

		# print pos_to_neg_deltas_interval.head()
		# print neg_to_pos_deltas_interval.head()

		# These are the good cases we want to keep.  We will then match pos-pos and neg-pos into one 
		# combined vector of all positive deltas.

		pos_to_pos_deltas_interval = rebalanced_pos_deltas_interval_unadj[rebalanced_pos_deltas_interval_unadj > 0]
		neg_to_neg_deltas_interval = rebalanced_neg_deltas_interval_unadj[rebalanced_neg_deltas_interval_unadj < 0]

		rebalanced_pos_deltas_interval_adj = pos_to_pos_deltas_interval.add(neg_to_pos_deltas_interval, fill_value=0)
		rebalanced_neg_deltas_interval_adj = neg_to_neg_deltas_interval.add(pos_to_neg_deltas_interval, fill_value=0)

		# The adjusted numbers do not contain the hours where we observe zero arrivals or departures
		# We use resampling to fix this issue and then fill in the resulting NaN values.

		rebalanced_pos_deltas_interval = rebalanced_pos_deltas_interval_adj.resample(time_interval, how ='sum')
		rebalanced_neg_deltas_interval = rebalanced_neg_deltas_interval_adj.resample(time_interval, how ='sum')

		arrivals = rebalanced_pos_deltas_interval.fillna(0)
		departures = abs(rebalanced_neg_deltas_interval.fillna(0))
	else:
		# If we don't wish to use rebalancing data #

		# Separate positive and negative deltas
		pos_deltas = deltas[deltas > 0]
		neg_deltas = abs(deltas[deltas < 0])

		# Count the number of positive and negative deltas per half hour per day, add them to new dataframe.
		pos_interval_counts_null = pos_deltas.resample(time_interval).sum()
		neg_interval_counts_null = neg_deltas.resample(time_interval).sum()

		# Set NaN delta counts to 0
		# By default the resampling step puts NaN (null values) into the data when there were no observations
		# to count up during those thirty minutes. 
		arrivals = pos_interval_counts_null.fillna(0)
		departures = neg_interval_counts_null.fillna(0)

	#arrivals_departures = pd.DataFrame(arrivals, columns=["arrivals"])
	arrivals_departures = pd.DataFrame()
	arrivals_departures['arrivals'] = arrivals
	arrivals_departures['departures'] = departures

	# Extract months for Month feature, add to model data
	delta_months = arrivals_departures.index.month
	arrivals_departures['months'] = delta_months

	# Extract hours for Hour feature
	delta_hours = arrivals_departures.index.hour
	arrivals_departures['hours'] = delta_hours

	# Extract weekday vs. weekend variable
	arrivals_departures['weekday_dummy'] = np.where(arrivals_departures.index.weekday < 5, 1, 0)
	dur = datetime.now() - tick
	log_line = {"function": "rebalance_station_poisson_data", "event_type": "End", "event": "setting up arrivals_departures", "arrivals_departures_shape":arrivals_departures.shape, "arrivals_departures_types":str(arrivals_departures.dtypes), "duration":dur.total_seconds()}
	logger.info(json.dumps(log_line))

	return arrivals_departures

def simulation(station_id, starting_time, final_time, max_slots, starting_bikes_available, month, weekday, simulate_bikes, trials=250, include_rebalance = False):
    # Produces multiple simulated hours and records the final number of bikes
    # along with information regarding whether the station ever goes empty or
    # full in the time interval.

	poisson_results = load_poisson_result(station_id, include_rebalance)
	bikes_results = [] # numbikes at the station at the end of each trial
	go_empty_results = [] #
	go_full_results = [] #
	for i in range(1,trials):
		bikes, empty, full = simulate_bikes(station_id, starting_time,final_time,max_slots,starting_bikes_available,month,weekday, poisson_results)
		bikes_results.append(bikes)
		go_empty_results.append(empty)
		go_full_results.append(full)
	return (bikes_results, go_empty_results, go_full_results)

def fit_poisson(snowflake_connection, cfg, station, include_rebalance = False, time_interval = '1H'):
	# Use the correct delta data
	station_updates = stations.GetStationData(snowflake_connection, cfg, station["station_id"], station["latitude"], station["longitude"])
	#print(station_updates.dtypes)
	arrivals_departures = rebalance_station_poisson_data(station_updates, station["station_id"], time_interval, include_rebalance = False)
	# Create design matrix for months, hours, and weekday vs. weekend.
	# We can't just create a "month" column to toss into our model, because it doesnt
	# understand what "June" is. Instead, we need to create a column for each month
	# and code each row according to what month it's in. Ditto for hours and weekday (=1).
	
	y_arr, X_arr = patsy.dmatrices("arrivals ~ C(months, Treatment) + C(hours, Treatment) + C(weekday_dummy, Treatment)", arrivals_departures, return_type='dataframe')
	y_dep, X_dep = patsy.dmatrices("departures ~ C(months, Treatment) + C(hours, Treatment) + C(weekday_dummy, Treatment)", arrivals_departures, return_type='dataframe')

	y_dep[pd.isnull(y_dep)] = 0

	# Fit poisson distributions for arrivals and departures, print results
	arr_poisson_model = sm.Poisson(y_arr, X_arr)
	arr_poisson_results = arr_poisson_model.fit(disp=0)

	dep_poisson_model = sm.Poisson(y_dep, X_dep)
	dep_poisson_results = dep_poisson_model.fit(disp = 0)

	# print arr_poisson_results.summary(), dep_poisson_results.summary()

	poisson_results = [arr_poisson_results, dep_poisson_results]

	return poisson_results


def save_poisson_results(snowflake_connection, cfg, stations, include_rebalance = False):
	# Runs the Poisson Fit Code for Each of the Station IDs
	
	tag = "rebalanced"
	if (include_rebalance == False):
		tag = "notrebalanced"

	for index, this_station in stations.iterrows():
		log_line = {"function": "save_poisson_results", "event_type": "Start", "event": "Processing station", "station_id":this_station["station_id"], "tag":tag}
		logger.info(json.dumps(log_line))
		tick = datetime.now()

		poisson_results = fit_poisson(snowflake_connection, cfg, this_station, include_rebalance)
		fname = "../models/poisson_results_%i_%s.p" % (this_station["station_id"], tag)
		file_out = open(fname, "wb")
		to_save_ps = (poisson_results[0].params, poisson_results[1].params)
		pickle.dump(to_save_ps, file_out)
		file_out.close()

		dur = datetime.now() - tick
		log_line = {"function": "save_poisson_results", "event_type": "End", "event": "Processing station", "pkl_file":fname, "duration":dur.total_seconds()}
		logger.info(json.dumps(log_line))	
	
