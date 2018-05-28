import time

taxi_trip_data_folder_location = "Z:\\trip_data"
taxi_trip_fare_folder_location = "Z:\\trip_fare"
data_trip_data_output_files = [taxi_trip_data_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]
data_trip_fare_output_files = [taxi_trip_fare_folder_location + "\\trip_fare_" + str(i) + ".csv" for i in range(1, 13)]


def file_len(fname):
    with open(fname) as f:
        i = 0
        for line in f:
            i += 1
        return i


# now = time.time()
# print(sum(file_len(f) for f in data_trip_data_output_files), "in", int(time.time() - now), "seconds")
#
# now = time.time()
# print(sum(file_len(f) for f in data_trip_fare_output_files), "in", int(time.time() - now), "seconds")


def print_first_entry(fname):
    with open(fname) as f:
        i = 0
        for line in f:
            i += 1
            print(line)

            if i == 2:
                break

#
print_first_entry(data_trip_data_output_files[0])
print_first_entry(data_trip_fare_output_files[0])
