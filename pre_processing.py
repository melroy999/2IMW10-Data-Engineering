from datetime import datetime

# What do we want to do in the pre-processing step?
#
# We start with the removal of data that does not fit our requirements:
#   - Eliminate entries that have an invalid start or end position:
#       - The longitude or the latitude of the start or end position is zero
#       - The distance between the start or end position to the center of NY is too large
#         Position of New York, NY, USA is: 40.71278 -74.00597
#   - Eliminate entries that are not within the correct time window:
#       - The earliest start time in the bike data is 2013-06-01 00:00:01
#       - The latest end time in the bike data is 2014-01-01 00:09:33
#   - Eliminate entries do not have the required fields set:
#       - Certain entries miss the dropoff location.
#       - Certain entries do not have (non-zero) values for passenger count, trip time and trip distance.
#
# Next to that we want to do a selection, as not all fields are useful to us.
# In the taxi dataset, we have the following set of fields:
#   0 - medallion
#   1 - hack_license
#   2 - vendor_id
#   3 - rate_code
#   4 - store_and_fwd_flag
#   5 - pickup_datetime
#   6 - dropoff_datetime
#   7 - passenger_count
#   8 - trip_time_in_secs
#   9 - trip_distance
#   10 - pickup_longitude
#   11 - pickup_latitude
#   12 - dropoff_longitude
#   13 - dropoff_latitude
# We are only interested in the fields 0, 5, 6, 9, 10, 11, 12 and 13.

# The location of the taxi trip data.

taxi_folder_location = "Z:\\trip_data"
taxi_data_files = [taxi_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]
taxi_output_files = [taxi_folder_location + "\\trip_data_pp_" + str(i) + ".csv" for i in range(1, 13)]

# Data used to determine the validity of entries.
time_format = '%Y-%m-%d %H:%M:%S'
start_time = datetime.strptime('2013-06-01 00:00:01', time_format)
end_time = datetime.strptime('2014-01-01 00:09:33', time_format)


# Check whether the given position is close to New York.
def is_valid_position(lon, lat):
    return (float(lon) - -74.00597) ** 2 + (float(lat) - 40.71278) ** 2 <= 4


# Do all pre-processing cleanup on each separate data file.
def pre_process_taxi_files():
    for i in range(5, 12):
        pre_process_taxi_file(taxi_data_files[i], taxi_output_files[i])


def pre_process_taxi_file(_input, _output):
    print("Pre-processing", _input)

    with open(_output, "w") as output_file:
        with open(_input, "r") as input_file:
            is_first = True

            # Write the header of the file.
            output_file.write("medallion,pickup_datetime,dropoff_datetime,trip_distance,"
                              "pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude")

            for line in input_file:
                if is_first:
                    is_first = False
                    continue

                # Select the fields we are interested in.
                data_fields = line.strip().split(",")
                target_fields = data_fields[0:1] + data_fields[5:7] + data_fields[9:]

                try:
                    # Check whether the given entry is valid or not, starting with the empty entries.
                    if any(data_fields[i] == "" for i in [10, 11, 12, 13]):
                        # One of the positions is not set, so skip.
                        continue

                    if any(data_fields[i] == "0" for i in [7, 8]):
                        # No passengers or a zero-time trip.
                        continue

                    if data_fields[9] == ".00":
                        # No distance traveled.
                        continue

                    # Proceed with the time and distance requirements.
                    if datetime.strptime(target_fields[1], time_format) >= start_time \
                            and datetime.strptime(target_fields[2], time_format) <= end_time \
                            and is_valid_position(target_fields[4], target_fields[5]) \
                            and is_valid_position(target_fields[6], target_fields[7]):

                        # Add the entry to the pre-processing file.
                        output_file.write('\n' + ",".join(target_fields))
                except ValueError:
                    print("Value error on line \"" + line.strip() + "\"")


# pre_process_taxi_files()
