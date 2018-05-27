import json
from collections import namedtuple

taxi_folder_location = "Z:\\trip_data"
taxi_data_files = [taxi_folder_location + "\\trip_data_pp_" + str(i) + ".csv" for i in range(1, 13)]
taxi_output_files = [taxi_folder_location + "\\trip_data_c_" + str(i) + ".csv" for i in range(1, 13)]

# First import the two clustering trees.
with open(taxi_folder_location + '\\source_clustering_tree.json', 'r') as median_tree_source_file:
    data = median_tree_source_file.read()

source_median_tree = json.loads(data, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))

with open(taxi_folder_location + '\\source_clustering_tree.json', 'r') as median_tree_source_file:
    data = median_tree_source_file.read()

target_median_tree = json.loads(data, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))


# Get the appropriate cluster using the appropriate tree.
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
def process_file(_input, _output):
    print("Setting clusters for", _input)

    with open(_output, "w") as output_file:
        with open(_input, "r") as input_file:
            is_first = True

            # Write the header of the file.
            output_file.write("medallion,pickup_datetime,dropoff_datetime,trip_distance,"
                              "pickup_cluster,dropoff_cluster")

            for line in input_file:
                if is_first:
                    is_first = False
                    continue

                # Select the fields we are interested in.
                data_fields = line.strip().split(",")

                # Find the clusters and reconstruct the entry.
                target_fields = data_fields[0:5]
                target_fields += [get_cluster_id(source_median_tree, (float(data_fields[-4]), float(data_fields[-3])))]
                target_fields += [get_cluster_id(source_median_tree, (float(data_fields[-2]), float(data_fields[-1])))]

                # Add the entry to the pre-processing file.
                output_file.write('\n' + ",".join([str(s) for s in target_fields]))


def process_files():
    for i in range(0, 12):
        process_file(taxi_data_files[i], taxi_output_files[i])


process_files()
# i = get_cluster_id(source_median_tree, (-73.91635466037592, 40.847389451422174))
# print(i)
