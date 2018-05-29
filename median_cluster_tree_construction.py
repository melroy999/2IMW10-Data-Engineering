# The partitioning into clusters is represented as a tree.
import json
import os

# The folder and files we plan to take data from.
taxi_folder_folder = "Z:\\data_engineering\\taxi_pre_processed_trip_data"
taxi_output_files = [taxi_folder_folder + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# The depth of the chosen tree.
k = 10

# The folder we want to store the result in.
median_cluster_tree_folder = "Z:\\data_engineering"
median_cluster_tree_file = "median_cluster_tree_k_" + str(k) + ".json"

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

    def write_to_file(self):
        self.finalize()
        with open(median_cluster_tree_folder + "\\" + median_cluster_tree_file, "w") as output_file:
            output_file.write(self.to_json())


def build_tree(depth):
    # The root of our tree.
    tree_root = Node()

    for filename in taxi_output_files:
        print("Processing file", filename)
        process_file(filename, tree_root)

    print("The tree is based on", len(tree_root.entries), "start position entries.")

    for i in range(0, depth):
        print("Calculating depth", i)
        tree_root.register_child_level()

    # Return the tree.
    return tree_root


def process_file(filename, tree_root):
    with open(filename, "r") as input_file:
        is_first = True

        for line in input_file:
            if is_first:
                is_first = False
                continue

            # Select the fields we are interested in.
            data_fields = line.strip().split(",")

            # Add the interesting data to the tree.
            tree_root.register_entry((float(data_fields[3]), float(data_fields[4])))


if os.path.isfile(median_cluster_tree_folder + "\\" + median_cluster_tree_file):
    answer = ""
    while answer not in ["y", "n"]:
        answer = input("The median cluster tree file already exist. Are you sure you want to continue [Y/N]? ").lower()
    if answer == "y":
        tree = build_tree(k)
        tree.write_to_file()
else:
    tree = build_tree(k)
    tree.write_to_file()


