import csv
import json
from collections import namedtuple
import fiona
import os

from shapely.geometry import Point, shape
from dbfread import DBF

# We start with constructing the nodes file.
# Import the desired cluster tree.
k = 10
median_cluster_tree_folder = "Z:\\data_engineering"
median_cluster_tree_file = "median_cluster_tree_k_" + str(k) + ".json"

with open(median_cluster_tree_folder + "\\" + median_cluster_tree_file, 'r') as median_tree_file:
    data = median_tree_file.read()
median_tree = json.loads(data, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))


# Convert the median tree to the list of nodes.
#   0 - node_id
#   1 - lon
#   2 - lat
#   3 - district
#   4 - nr_taxi_starts
#   5 - nr_bike_starts
#   6 - nr_taxi_ends
#   7 - nr_bike_ends
class TGNode:
    def __init__(self, _id, lon, lat):
        self.node_id = _id
        self.lon = lon
        self.lat = lat
        self.district = ""
        self.nr_taxi_starts = 0
        self.nr_bike_starts = 0
        self.nr_taxi_ends = 0
        self.nr_bike_ends = 0


def get_all_nodes(tree, _nodes=None):
    if _nodes is None:
        _nodes = []

    # If we have children, pass the command down.
    if tree.lesser_child is not None:
        get_all_nodes(tree.lesser_child, _nodes)
        get_all_nodes(tree.greater_child, _nodes)
    else:
        # Create a node and add it to the nodes list.
        _nodes += [TGNode(tree.id, float(tree.center[0]), float(tree.center[1]))]


# We know that the ids are given in incremental order, starting by 0.
nodes = []
get_all_nodes(median_tree, nodes)

# Find the district the cluster is part of, by checking which district the center point is in.
neighborhood_data_folder = "Z:\\data_engineering\\neighborhood_data"

with fiona.open(neighborhood_data_folder + "\\neighbourhoods.shp") as f:
    db = DBF(neighborhood_data_folder + "\\neighbourhoods.dbf")

    for multi, record in zip(f, db):
        for i, node in enumerate(nodes):
            point = Point(node.lon, node.lat)
            if point.within(shape(multi['geometry'])):
                nodes[i].district = record["Name"]

# Location of the clustered taxi files.
taxi_folder_location = "Z:\\data_engineering\\taxi_clustered_trip_data"
taxi_data_files = [taxi_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# Location of the clustered taxi files.
bike_folder_location = "Z:\\data_engineering\\bike_clustered_trip_data"
bike_data_files = [bike_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(6, 13)]


# For each of the data files, replace the position with the appropriate cluster id.
def process_taxi_file(_input):
    with open(_input, "r") as input_file:
        is_first = True

        for line in input_file:
            if is_first:
                is_first = False
                continue

            # Select the fields we are interested in.
            data_fields = line.strip().split(",")

            from_cluster = int(data_fields[3])
            to_cluster = int(data_fields[4])

            nodes[from_cluster].nr_taxi_starts += 1
            nodes[to_cluster].nr_taxi_ends += 1


def process_bike_file(_input):
    with open(_input, "r") as input_file:
        is_first = True

        for line in input_file:
            if is_first:
                is_first = False
                continue

            # Select the fields we are interested in.
            data_fields = line.strip().split(",")

            from_cluster = int(data_fields[3])
            to_cluster = int(data_fields[4])

            nodes[from_cluster].nr_bike_starts += 1
            nodes[to_cluster].nr_bike_ends += 1


# The location where the node file is located.
temporal_graph_folder = "Z:\\data_engineering\\temporal_graph"
temporal_nodes_file = "nodes.csv"


def _process_files():
    for i in range(0, len(taxi_data_files)):
        process_taxi_file(taxi_data_files[i])

    for i in range(0, len(bike_data_files)):
        process_bike_file(bike_data_files[i])

    # TODO output files to csv.
    with open(temporal_graph_folder + "\\" + temporal_nodes_file, "w", newline='') as output_file:
        writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["node_id", "lon", "lat", "district", "nr_taxi_starts", "nr_bike_starts", "nr_taxi_ends",
                         "nr_bike_ends"])

        for node in nodes:
            writer.writerow([node.node_id, node.lon, node.lat, node.district, node.nr_taxi_starts, node.nr_bike_starts,
                             node.nr_taxi_ends, node.nr_bike_ends])


def process_files():
    if os.path.isfile(temporal_graph_folder + "\\" + temporal_nodes_file):
        answer = ""
        while answer not in ["y", "n"]:
            answer = input("Temporal nodes file already exists. Are you sure you want to continue [Y/N]? ").lower()
        if answer == "y":
            _process_files()
    else:
        _process_files()


process_files()


