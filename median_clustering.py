import json
import os
from collections import namedtuple

# Location of the pre-processed taxi source files.
taxi_folder_location = "Z:\\data_engineering\\taxi_pre_processed_trip_data"
taxi_data_files = [taxi_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# Location of the clustered taxi files.
taxi_output_folder = "Z:\\data_engineering\\taxi_clustered_trip_data"
taxi_output_files = [taxi_output_folder + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# Location of the pre-processed taxi source files.
bike_folder_location = "Z:\\data_engineering\\bike_pre_processed_trip_data"
bike_data_files = [bike_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(6, 13)]

# Location of the clustered taxi files.
bike_output_folder = "Z:\\data_engineering\\bike_clustered_trip_data"
bike_output_files = [bike_output_folder + "\\trip_data_" + str(i) + ".csv" for i in range(6, 13)]

# Import the desired cluster tree.
k = 10
median_cluster_tree_folder = "Z:\\data_engineering"
median_cluster_tree_file = "median_cluster_tree_k_" + str(k) + ".json"

with open(median_cluster_tree_folder + "\\" + median_cluster_tree_file, 'r') as median_tree_file:
    data = median_tree_file.read()
median_tree = json.loads(data, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))


# Get the appropriate cluster using the given (sub)tree.
def get_cluster_id(tree, pos, depth=0):
    if tree.lesser_child is not None:
        # Find which subtree the position resides in.
        if pos[depth % 2] < tree.median:
            return get_cluster_id(tree.lesser_child, pos, depth + 1)
        else:
            return get_cluster_id(tree.greater_child, pos, depth + 1)
    else:
        # Report the id.
        return tree.id


# For each of the data files, replace the position with the appropriate cluster id.
def process_taxi_file(_input, _output):
    print("Setting clusters for", _input)

    with open(_output, "w") as output_file:
        with open(_input, "r") as input_file:
            is_first = True

            # Write the header of the file.
            output_file.write("taxi_id,start_time,end_time,from_cluster,to_cluster,passenger_count,"
                              "trip_duration,fare_amount,tip_amount")

            for line in input_file:
                if is_first:
                    is_first = False
                    continue

                # Select the fields we are interested in.
                data_fields = line.strip().split(",")

                # Find the clusters and reconstruct the entry.
                target_fields = data_fields[0:3]
                target_fields += [get_cluster_id(median_tree, (float(data_fields[3]), float(data_fields[4])))]
                target_fields += [get_cluster_id(median_tree, (float(data_fields[5]), float(data_fields[6])))]
                target_fields += data_fields[7:]

                # Add the entry to the pre-processing file.
                output_file.write('\n' + ",".join([str(s) for s in target_fields]))


def process_bike_file(_input, _output):
    print("Setting clusters for", _input)

    with open(_output, "w") as output_file:
        with open(_input, "r") as input_file:
            is_first = True

            # Write the header of the file.
            output_file.write("bike_id,start_time,end_time,from_cluster,to_cluster,trip_duration")

            for line in input_file:
                if is_first:
                    is_first = False
                    continue

                # Select the fields we are interested in.
                data_fields = line.strip().split(",")

                # Find the clusters and reconstruct the entry.
                target_fields = data_fields[0:3]
                target_fields += [get_cluster_id(median_tree, (float(data_fields[3]), float(data_fields[4])))]
                target_fields += [get_cluster_id(median_tree, (float(data_fields[5]), float(data_fields[6])))]
                target_fields += data_fields[7:]

                # Add the entry to the pre-processing file.
                output_file.write('\n' + ",".join([str(s) for s in target_fields]))


def process_taxi_files():
    for i in range(0, len(taxi_data_files)):
        process_taxi_file(taxi_data_files[i], taxi_output_files[i])


def process_bike_files():
    for i in range(0, len(bike_data_files)):
        process_bike_file(bike_data_files[i], bike_output_files[i])


def process_files():
    if any(os.path.isfile(filename) for filename in taxi_output_files):
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Clustered taxi files already exist. Are you sure you want to continue [Y/N]? ").lower()
        if answer == "y":
            process_taxi_files()
    else:
        process_taxi_files()

    if any(os.path.isfile(filename) for filename in bike_output_files):
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Clustered bike files already exist. Are you sure you want to continue [Y/N]? ").lower()
        if answer == "y":
            process_bike_files()
    else:
        process_bike_files()


process_files()
