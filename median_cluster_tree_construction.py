# The partitioning into clusters is represented as a tree.
import json

# The folder and files we plan to take data from.
from collections import namedtuple

taxi_folder_location = "Z:\\trip_data"
taxi_output_files = [taxi_folder_location + "\\trip_data_pp_" + str(i) + ".csv" for i in range(1, 13)]

# Unique id counter for each node.
id_counter = 0


class Node:
    # Initialize the node to have no children and zero valued fields.
    def __init__(self):
        self.lesser_child = None
        self.greater_child = None
        self.median = 0
        self.center = (0, 0)
        self.id = 0
        self.entries = []

    def register_entry(self, pos):
        self.entries.append(pos)

    def register_child_level(self, depth=0):
        # If we have children, pass the command down.
        if self.lesser_child is not None:
            self.lesser_child.register_child_level(depth + 1)
            self.greater_child.register_child_level(depth + 1)
        else:
            # Create two new children to work with.
            self.lesser_child = Node()
            self.greater_child = Node()

            # Calculate the median, and redistribute the entries.
            self.entries.sort(key=lambda x: x[depth % 2])
            i = len(self.entries) // 2
            self.median = self.entries[i][depth % 2]
            self.lesser_child.entries = self.entries[:i]
            self.greater_child.entries = self.entries[i:]

            # Reset the entries in this node.
            self.entries = None

    def finalize(self):
        # Assign an unique id and point location to the leaf nodes.
        if self.lesser_child is None:
            # Find the center point, by taking the average of both the longitudes and latitudes.
            global id_counter
            self.id = id_counter
            id_counter += 1

            # Find the center point.
            n = len(self.entries)
            self.center = (sum(i[0] for i in self.entries) / n, sum(i[1] for i in self.entries) / n)

        # Delete any mention of entries.
        del self.entries

        # If we have children, pass the command down.
        if self.lesser_child is not None:
            # Internal nodes do not have a center or an id.
            del self.center
            del self.id

            self.lesser_child.finalize()
            self.greater_child.finalize()

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

    def write_to_file(self, filename):
        self.finalize()
        with open(taxi_folder_location + "\\" + filename, "w") as output_file:
            output_file.write(self.to_json())


def build_tree(depth, longitude_field, latitude_field):
    # The root of our tree.
    tree_root = Node()

    for filename in taxi_output_files:
        print("Processing file", filename)
        process_file(filename, tree_root, longitude_field, latitude_field)

    print("The tree has", len(tree_root.entries), "entries in total.")

    for i in range(0, depth):
        print("Calculating depth", i)
        tree_root.register_child_level()

    # Return the tree.
    return tree_root


def process_file(filename, tree_root, longitude_field, latitude_field):
    with open(filename, "r") as input_file:
        is_first = True

        for line in input_file:
            if is_first:
                is_first = False
                continue

            # Select the fields we are interested in.
            data_fields = line.strip().split(",")

            # Add the interesting data to the tree.
            tree_root.register_entry((float(data_fields[longitude_field]), float(data_fields[latitude_field])))


tree = build_tree(10, -4, -3)
tree.write_to_file("source_clustering_tree.json")

tree = build_tree(10, -2, -1)
tree.write_to_file("target_clustering_tree.json")

# data = tree.to_json()
# x = json.loads(data, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))

i = 0
