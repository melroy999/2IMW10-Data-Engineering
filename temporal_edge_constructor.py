import csv
import datetime
import json
import queue as Q
from collections import namedtuple

import os
from dateutil.parser import parse


# The depth of the median tree, which is used to calculate the number of clusters.
k = 10
N = 2 ** k

# The current threshold used to determine when temporal edges are active.
# We need a minimum of 'threshold' edges active for an edge to be created:
edge_threshold = 3

# The location of the weather and the imported weather data.
time_format = '%Y-%m-%d %H:%M:%S'
weather_file_location = "Z:\\data_engineering\\weather_data\\weather.json"
with open(weather_file_location, "r") as wf:
    weather = json.load(wf)


# Convert the datetime format to the key used in the weather lookups.
def make_code(date):
    # Code in format: 2013-06-01 00:00:01"
    # Want in format HHDDMMYYYY
    hour = date[11] + date[12]
    day = date[8] + date[9]
    month = date[5] + date[6]
    year = "2013"
    return hour + day + month + year


# parses a string of a time to a python datetime
def get_time(date_string):
    return parse(date_string)


# Gets weather at a certain time.
# Returns a list of 4 items, temperature, fog, rain and condition
def get_weather(time):
    date_key = make_code(time)
    if date_key in weather: # Apparently some are not in there..
        weather_row = weather[date_key]
        return weather_row[4:]
    else:
        return [-0.0, -1, -1, "unknown"]


# Taxi data:
# taxi_id,start_time,end_time,from_cluster,to_cluster,passenger_count,trip_duration,fare_amount,tip_amount
# Get: from_cluster, to_cluster, passenger_count, trip_duration, tip_amount, trip_duration
def get_data_from_taxi_row(row):
    from_cluster = int(row[3])
    to_cluster = int(row[4])
    passenger_count = int(row[5])
    trip_duration = int(row[6])
    fare_amount = float(row[7])
    tip_amount = float(row[8])
    return from_cluster, to_cluster, passenger_count, trip_duration, fare_amount, tip_amount


# Bike data:
# bike_id,start_time,end_time,from_cluster,to_cluster,trip_duration
# Get: from_cluster, to_cluster, trip_duration
def get_data_from_bike_row(row):
    from_cluster = int(row[3])
    to_cluster = int(row[4])
    trip_duration = int(row[5])
    return from_cluster, to_cluster, trip_duration


# Writes a row to the output file
def write_taxi_row(start_time, end_time, from_cluster, to_cluster, avg_passenger_count, avg_trip_duration, 
                   avg_fare_amount, avg_tip_amount, count, writer):
    start_time_str = start_time.strftime(time_format)
    end_time_str = end_time.strftime(time_format)
    duration = (end_time-start_time).total_seconds()

    global id_counter
    row = [id_counter, "Taxi", start_time_str, end_time_str, from_cluster, to_cluster, avg_passenger_count, 
           avg_trip_duration, avg_fare_amount, avg_tip_amount]
    row += get_weather(start_time_str)
    row += [count, duration]
    writer.writerow(row)

    # increment the unique id.
    id_counter += 1


# Writes a bike row to the output file.
def write_bike_row(start_time, end_time, from_cluster, to_cluster, avg_trip_duration, count, writer):
    start_time_str = start_time.strftime(time_format)
    end_time_str = end_time.strftime(time_format)
    duration = (end_time-start_time).total_seconds()

    global id_counter
    row = [id_counter, "Bike", start_time_str, end_time_str, from_cluster, to_cluster, "", avg_trip_duration, "", ""]
    row += get_weather(start_time_str)
    row += [count, duration]
    writer.writerow(row)

    # increment the unique id.
    id_counter += 1


# Location of the clustered taxi files.
taxi_folder_location = "Z:\\data_engineering\\taxi_sorted_trip_data"
taxi_data_files = [taxi_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)][-1:]

# Location of the clustered taxi files.
bike_folder_location = "Z:\\data_engineering\\bike_sorted_trip_data"
bike_data_files = [bike_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(6, 13)]

# The location where the node file is located.
temporal_graph_folder = "Z:\\data_engineering\\temporal_graph"
temporal_edges_file = "edges.csv"

# The counter to generate unique ids.
id_counter = 0


def construct_taxi_temporal_edges(output_file):
    # Pre-formatted trip tuple.
    Trip = namedtuple("Trip", "time data")

    # Matrices that hold the current time data for each cluster, plus supportive data.
    time_matrix = [[[datetime.datetime.now(), datetime.datetime.now()] for _ in range(N)] for _ in range(N)]
    taxi_matrix = [[[0, 0, 0, 0, 0] for _ in range(N)] for _ in range(N)]
    max_count_matrix = [[0 for _ in range(N)] for _ in range(N)]

    # Create a writer.
    writer = csv.writer(output_file)

    # Gather information that will stay constant for all the files.
    queue = Q.PriorityQueue()
    top_taxi = None

    for filename in taxi_data_files:
        with open(filename, "r") as input_file:
            print("[" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "]", "Searching for temporal edges in", filename)

            # Create a reader for the csv file.
            reader = csv.reader(input_file)

            # Skip the header.
            header = next(reader, None)

            # Iterate over all the edges and update data as we go along.
            for row in reader:
                start_time = get_time(row[1])
                end_time = get_time(row[2])
                read_trip_start = Trip(start_time, row)
                read_trip_end = Trip(end_time, row)

                if top_taxi is not None:
                    # Remove trips from the queue that have ended, with respect to the currently observed trip.
                    while top_taxi.time < read_trip_start.time:
                        from_cluster, to_cluster, passenger_count, trip_duration, fare_amount, tip_amount = \
                            get_data_from_taxi_row(top_taxi.data)

                        # If we are currently on our threshold, the current active temporal edge should end.
                        # Thus, write the appropriate data to the file.
                        if taxi_matrix[from_cluster][to_cluster][0] == edge_threshold:
                            write_taxi_row(
                                # The time at which the edge starts and ends.
                                time_matrix[from_cluster][to_cluster][0],
                                top_taxi.time,
                                from_cluster,
                                to_cluster,
                                # The average taxi specific data.
                                (taxi_matrix[from_cluster][to_cluster][1]/taxi_matrix[from_cluster][to_cluster][0]),
                                (taxi_matrix[from_cluster][to_cluster][2]/taxi_matrix[from_cluster][to_cluster][0]),
                                (taxi_matrix[from_cluster][to_cluster][3]/taxi_matrix[from_cluster][to_cluster][0]),
                                (taxi_matrix[from_cluster][to_cluster][4]/taxi_matrix[from_cluster][to_cluster][0]),
                                max_count_matrix[from_cluster][to_cluster],
                                writer
                            )

                        # Update the number of active edges and total duration.
                        taxi_matrix[from_cluster][to_cluster][0] -= 1  # Update count
                        taxi_matrix[from_cluster][to_cluster][1] -= passenger_count  # Update duration
                        taxi_matrix[from_cluster][to_cluster][2] -= trip_duration  # Update fare
                        taxi_matrix[from_cluster][to_cluster][3] -= fare_amount  # Update tip
                        taxi_matrix[from_cluster][to_cluster][4] -= tip_amount  # Update passenger

                        if queue.empty():
                            top_taxi = None
                            break
                        else:
                            top_taxi = queue.get()

                # Add the new line to the database.
                from_cluster, to_cluster, passenger_count, trip_duration, fare_amount, tip_amount = \
                    get_data_from_taxi_row(row)

                # If we reach the threshold by adding this edge, register the start time of the temporal edge.
                if taxi_matrix[from_cluster][to_cluster][0] == edge_threshold - 1:
                    # Add starting time
                    time_matrix[from_cluster][to_cluster][0] = read_trip_start.time

                # Update the number of active edges and total duration.
                taxi_matrix[from_cluster][to_cluster][0] += 1  # Update count
                taxi_matrix[from_cluster][to_cluster][1] += passenger_count  # Update duration
                taxi_matrix[from_cluster][to_cluster][2] += trip_duration  # Update fare
                taxi_matrix[from_cluster][to_cluster][3] += fare_amount  # Update tip
                taxi_matrix[from_cluster][to_cluster][4] += tip_amount  # Update passenger

                if max_count_matrix[from_cluster][to_cluster] < taxi_matrix[from_cluster][to_cluster][0]:
                    max_count_matrix[from_cluster][to_cluster] = taxi_matrix[from_cluster][to_cluster][0]

                queue.put(read_trip_end)

                # If we have no top, pop the queue.
                if top_taxi is None:
                    top_taxi = queue.get()


def construct_bike_temporal_edges(output_file):
    # Pre-formatted trip tuple.
    Trip = namedtuple("Trip", "time data")

    # Matrices that hold the current time data for each cluster, plus supportive data.
    time_matrix = [[[datetime.datetime.now(), datetime.datetime.now()] for _ in range(N)] for _ in range(N)]
    bike_matrix = [[[0, 0] for _ in range(N)] for _ in range(N)]
    max_count_matrix = [[0 for _ in range(N)] for _ in range(N)]

    # Create a writer.
    writer = csv.writer(output_file)

    # Gather information that will stay constant for all the files.
    queue = Q.PriorityQueue()
    top_bike = None

    for filename in bike_data_files:
        with open(filename, "r") as input_file:
            print("[" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "]", "Searching for temporal edges in", filename)

            # Create a reader for the csv file.
            reader = csv.reader(input_file)

            # Skip the header.
            header = next(reader, None)

            # Iterate over all the edges and update data as we go along.
            for row in reader:
                start_time = get_time(row[1])
                end_time = get_time(row[2])
                read_trip_start = Trip(start_time, row)
                read_trip_end = Trip(end_time, row)

                if top_bike is not None:
                    # Remove trips from the queue that have ended, with respect to the currently observed trip.
                    while top_bike.time < read_trip_start.time:
                        from_cluster, to_cluster, trip_duration = get_data_from_bike_row(top_bike.data)

                        # If we are currently on our threshold, the current active temporal edge should end.
                        # Thus, write the appropriate data to the file.
                        if bike_matrix[from_cluster][to_cluster][0] == edge_threshold:
                            write_bike_row(
                                # The time at which the edge starts and ends.
                                time_matrix[from_cluster][to_cluster][0],
                                top_bike.time,
                                from_cluster,
                                to_cluster,
                                # The average distance traveled by the bikes during the trips.
                                (bike_matrix[from_cluster][to_cluster][1]/bike_matrix[from_cluster][to_cluster][0]),
                                max_count_matrix[from_cluster][to_cluster],
                                writer
                            )
                            max_count_matrix[from_cluster][to_cluster] = edge_threshold

                        # Update the number of active edges and total duration.
                        bike_matrix[from_cluster][to_cluster][0] -= 1
                        bike_matrix[from_cluster][to_cluster][1] -= trip_duration

                        if queue.empty():
                            top_bike = None
                            break
                        else:
                            top_bike = queue.get()

                # Add the new line to the database.
                from_cluster, to_cluster, trip_duration = get_data_from_bike_row(row)

                # If we reach the threshold by adding this edge, register the start time of the temporal edge.
                if bike_matrix[from_cluster][to_cluster][0] == edge_threshold - 1:
                    # Add starting time
                    time_matrix[from_cluster][to_cluster][0] = read_trip_start.time

                # Update the number of active edges and total duration.
                bike_matrix[from_cluster][to_cluster][0] += 1
                bike_matrix[from_cluster][to_cluster][1] += trip_duration

                if max_count_matrix[from_cluster][to_cluster] < bike_matrix[from_cluster][to_cluster][0]:
                    max_count_matrix[from_cluster][to_cluster] = bike_matrix[from_cluster][to_cluster][0]

                queue.put(read_trip_end)

                # If we have no top, pop the queue.
                if top_bike is None:
                    top_bike = queue.get()


def _construct_temporal_edges():
    # Open a writer to the result file.
    with open(temporal_graph_folder + "\\" + temporal_edges_file, "w", newline='') as output_file:
        # Write the header.
        writer = csv.writer(output_file)
        writer.writerow(["edge_id", "edge_label", "start_time", "end_time", "from_cluster", "to_cluster",
                         "passenger_count", "trip_duration", "fare_amount", "tip_amount", "temperature", "fog",
                         "rain", "condition", "no_of_edges", "duration"])

        construct_taxi_temporal_edges(output_file)
        # construct_bike_temporal_edges(output_file)


def construct_temporal_edges():
    if os.path.isfile(temporal_graph_folder + "\\" + temporal_edges_file):
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Temporal edges file already exists. Are you sure you want to continue [Y/N]? ").lower()
        if answer == "y":
            _construct_temporal_edges()
    else:
        _construct_temporal_edges()


construct_temporal_edges()
