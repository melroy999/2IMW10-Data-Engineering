import operator
from datetime import datetime


# Next, we want to sort the clustered files on time, more specifically the starting time followed by end time.
# Location of the clustered taxi source files.
import os

taxi_folder_location = "Z:\\data_engineering\\taxi_clustered_trip_data"
taxi_data_files = [taxi_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# Location of the clustered taxi files.
taxi_output_folder = "Z:\\data_engineering\\taxi_sorted_trip_data"
taxi_output_files = [taxi_output_folder + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# Location of the pre-processed taxi source files.
bike_folder_location = "Z:\\data_engineering\\bike_clustered_trip_data"
bike_data_files = [bike_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(6, 13)]

# Location of the clustered taxi files.
bike_output_folder = "Z:\\data_engineering\\bike_sorted_trip_data"
bike_output_files = [bike_output_folder + "\\trip_data_" + str(i) + ".csv" for i in range(6, 13)]

# Format of a date time entry.
time_format = '%Y-%m-%d %H:%M:%S'


def sort_taxi_file(_input, _output):
    # First, import the entire file...
    print("Setting clusters for", _input)

    entries = []

    with open(_output, "w") as output_file:
        with open(_input, "r") as input_file:
            is_first = True

            for line in input_file:
                if is_first:
                    is_first = False
                    continue

                # Select the fields we are interested in.
                data_fields = line.strip().split(",")

                entries += [
                    (
                        datetime.strptime(data_fields[1], time_format),
                        datetime.strptime(data_fields[2], time_format),
                        line
                    )
                ]

        # Write the header of the file.
        output_file.write("taxi_id,start_time,end_time,from_cluster,to_cluster,passenger_count,"
                          "trip_duration,fare_amount,tip_amount")

        # Sort the entries, and output them.
        entries.sort(key=lambda x: x[0])

        # Output all the entries.
        for e in entries:
            output_file.write('\n' + e[2].strip())


def sort_bike_file(_input, _output):
    # First, import the entire file...
    print("Setting clusters for", _input)

    entries = []

    with open(_output, "w") as output_file:
        with open(_input, "r") as input_file:
            is_first = True

            for line in input_file:
                if is_first:
                    is_first = False
                    continue

                # Select the fields we are interested in.
                data_fields = line.strip().split(",")

                entries += [
                    (
                        datetime.strptime(data_fields[1], time_format),
                        datetime.strptime(data_fields[2], time_format),
                        line
                    )
                ]

        # Write the header of the file.
        output_file.write("bike_id,start_time,end_time,from_cluster,to_cluster,trip_duration")

        # Sort the entries, and output them.
        entries.sort(key=lambda x: x[0])

        # Output all the entries.
        for e in entries:
            output_file.write('\n' + e[2].strip())


def process_taxi_files():
    for i in range(0, len(taxi_data_files)):
        sort_taxi_file(taxi_data_files[i], taxi_output_files[i])


def process_bike_files():
    for i in range(0, len(bike_data_files)):
        sort_bike_file(bike_data_files[i], bike_output_files[i])


def process_files():
    if any(os.path.isfile(filename) for filename in taxi_output_files):
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Sorted taxi files already exist. Are you sure you want to continue [Y/N]? ").lower()
        if answer == "y":
            process_taxi_files()
    else:
        process_taxi_files()

    if any(os.path.isfile(filename) for filename in bike_output_files):
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Sorted bike files already exist. Are you sure you want to continue [Y/N]? ").lower()
        if answer == "y":
            process_bike_files()
    else:
        process_bike_files()

process_files()
