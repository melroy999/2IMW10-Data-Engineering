# The partitioning into clusters is represented as a tree.
class Node:

    # Initialize the node to have no children and zero valued fields.
    def __init__(self):
        self.lesser_child = None
        self.greater_child = None
        self.count = 0
        self.sum = 0
        self.mean = 0

    def register_entry(self, pos, depth=0):
        # If we are at a leaf node, update the sum and count accordingly.
        if self.lesser_child is None:
            self.count += 1
            self.sum += pos[depth % 2]
        else:
            # If the depth is even, we partition vertically on the longitude.
            # Otherwise, we partition horizontally on the latitude.
            if pos[depth % 2] < self.mean:
                self.lesser_child.register_entry(pos, depth + 1)
            else:
                self.greater_child.register_entry(pos, depth + 1)

    def register_child_level(self):
        # If we have children, pass the command down.
        if self.lesser_child is not None:
            self.lesser_child.register_child_level()
            self.greater_child.register_child_level()
        else:
            # Calculate the mean, which will determine which child to use in the register phase.
            self.mean = self.sum / self.count

            # Create two new children to work with.
            self.lesser_child = Node()
            self.greater_child = Node()


# The folder and files we plan to take data from.
taxi_folder_location = "Z:\\trip_data"
taxi_output_files = [taxi_folder_location + "\\trip_data_pp_" + str(i) + ".csv" for i in range(1, 13)]


def build_tree(no_levels):
    # The root of our tree.
    tree_root = Node()

    for i in range(0, no_levels):
        for filename in taxi_output_files:
            print("Processing level", i, "for file", filename)
            process_file(filename, tree_root)

        # Add a new level of nodes.
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
            tree_root.register_entry((float(data_fields[-4]), float(data_fields[-3])))


# A more inefficient implementation, which can handle medians.
def build_2():
    # The root of our tree.
    _positions = []

    for filename in taxi_output_files:
        print("Processing file", filename)
        process_file_2(filename, _positions)

    # Return the positions.
    return _positions


def process_file_2(filename, _positions):
    with open(filename, "r") as input_file:
        is_first = True

        for line in input_file:
            if is_first:
                is_first = False
                continue

            # Select the fields we are interested in.
            data_fields = line.strip().split(",")

            # Add the interesting data to the tree.
            _positions.append((float(data_fields[-4]), float(data_fields[-3])))


# tree = build_tree(1)
positions = build_2()
i = 0
