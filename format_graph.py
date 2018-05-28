# Formats a csv file with properties listed on the first line and values on the remaining lines into the order required:
# NODES: NodeID, Property-1, Value-1, …, Property-N, Value-N
# EDGES: EdgeID, FromNodeID, ToNodeID, Property-1, Value-1, …, Property-N, Value-N

node_file = ""  # Fill in file location here
node_output_files = "formated_node_output.csv"

edge_file = ""  # Fill in file location here
edge_output_files = "formated_edge_output.csv"


# Format the node file
def format_node_csv():
    first_line = True
    properties = []
    with open(node_output_files, "a") as output_file:

        for line in node_file:
            if first_line:
                first_line = False

                # Get all the property names. I assume that the nodeID is the first thing on a line
                data_fields = line.strip().split(",")
                properties = data_fields[1:]
                continue

            # Get the line and update it
            fields = line.strip().split(",")
            output = []
            output[0] = fields[0]
            counter = 0
            try :
                for item in fields[1:]:
                    output.append(properties[counter])
                    output.append(item)
                    counter += 1

                output_file.write('\n' + ",".join(output))

            except IndexError:
                print("Index out of bound on line\"" + line.strip() + "\"")


# Format the edge file
def format_edge_csv():
    first_line = True
    properties = []
    with open(edge_output_files, "a") as output_file:

        for line in edge_file:
            if first_line:
                first_line = False

                # Get all the property names. I assume that the first three items are EdgeID, FromNodeID, ToNodeID
                data_fields = line.strip().split(",")
                properties = data_fields[3:]
                continue

            # Get the line and update it. I assume that the first three items are EdgeID, FromNodeID, ToNodeID
            fields = line.strip().split(",")
            output = []
            output[0] = fields[:3]
            counter = 0
            try :
                for item in fields[3:]:
                    output.append(properties[counter])
                    output.append(item)
                    counter += 1

                output_file.write('\n' + ",".join(output))

            except IndexError:
                print("Index out of bound on line\"" + line.strip() + "\"")