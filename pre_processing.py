from datetime import datetime
import os

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
#       - Certain entries have a zero trip distance.
#
# Next to that we want to do a selection, as not all fields are useful to us.
# In the taxi trip dataset, we have the following set of fields:
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
# We are only interested in the fields 0, 5, 6, 7, 8, 10, 11, 12 and 13.
#
# In the taxi fare dataset, we have the following set of fields:
#   0 - medallion
#   1 - hack_license
#   2 - vendor_id
#   3 - pickup_datetime
#   4 - payment_type
#   5 - fare_amount
#   6 - surcharge
#   7 - mta_tax
#   8 - tip_amount
#   9 - tolls_amount
#   10 - total_amount
# We are only interested in the fields 5, 8.
# The entries in the trip and trip fare data match one to one.

# The location of the taxi trip and taxi trip fare data.
taxi_trip_data_folder_location = "Z:\\data_engineering\\taxi_trip_data"
taxi_trip_fare_folder_location = "Z:\\data_engineering\\taxi_trip_fare"
taxi_trip_files = [taxi_trip_data_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]
taxi_fare_files = [taxi_trip_fare_folder_location + "\\trip_fare_" + str(i) + ".csv" for i in range(1, 13)]

# The location of the output data.
taxi_output_folder_location = "Z:\\data_engineering\\taxi_pre_processed_trip_data"
taxi_output_files = [taxi_output_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# Data used to determine the validity of entries.
time_format = '%Y-%m-%d %H:%M:%S'
start_time = datetime.strptime('2013-06-01 00:00:01', time_format)
end_time = datetime.strptime('2014-01-01 00:09:33', time_format)


# Check whether the given position is close to New York.
def is_valid_position(lon, lat):
    return (float(lon) - -74.00597) ** 2 + (float(lat) - 40.71278) ** 2 <= 4


# Do all pre-processing cleanup on each separate data file.
def pre_process_taxi_files():
    for i in range(0, len(taxi_output_files)):
        pre_process_taxi_file(i)


def pre_process_taxi_file(month_id):
    print("Pre-processing", taxi_trip_files[month_id], "and", taxi_fare_files[month_id], "into", taxi_output_files[month_id])

    with open(taxi_output_files[month_id], "w") as output_file:
        with open(taxi_trip_files[month_id], "r") as data_file, open(taxi_fare_files[month_id], "r") as fare_file:
            is_first = True

            # Write the header of the file.
            # Note that some entries are not in the order specified in the file!
            output_file.write("taxi_id,start_time,end_time,pickup_longitude,pickup_latitude,dropoff_longitude,"
                              "dropoff_latitude,passenger_count,trip_duration,fare_amount,tip_amount")

            if month_id < 5:
                # We know already that these months are excluded, so skip.
                return

            for x, y in zip(data_file, fare_file):
                if is_first:
                    is_first = False
                    continue

                # Select the fields we are interested in.
                fields_x = x.strip().split(",")
                fields_y = y.strip().split(",")

                # x: we are only interested in the fields 0, 5, 6, 7, 8, 10, 11, 12 and 13.
                # y: we are only interested in the fields 5, 8.
                target_fields = fields_x[0:1] + fields_x[5:7] + fields_x[10:] + fields_x[7:9] + fields_y[5:6] + fields_y[8:9]

                try:
                    # Check whether the given entry is valid or not, starting with the empty entries.
                    if any(fields_x[i] == "" for i in [10, 11, 12, 13]):
                        # One of the positions is not set, so skip.
                        continue

                    if fields_x[9] == ".00":
                        # No distance traveled.
                        continue

                    # Proceed with the time and distance requirements.
                    if datetime.strptime(fields_x[5], time_format) >= start_time \
                            and datetime.strptime(fields_x[6], time_format) <= end_time \
                            and is_valid_position(fields_x[10], fields_x[11]) \
                            and is_valid_position(fields_x[12], fields_x[13]):

                        # Add the entry to the pre-processing file.
                        output_file.write('\n' + ",".join(target_fields))
                except ValueError:
                    print("Value error on line \"" + x.strip() + "\"")


if any(os.path.isfile(filename) for filename in taxi_output_files):
    answer = ""
    while answer not in ["y", "n"]:
        answer = input("Pre-processed taxi files already exist. Are you sure you want to continue [Y/N]? ").lower()
    if answer == "y":
        pre_process_taxi_files()
else:
    pre_process_taxi_files()


# In the city bike dataset, we have the following set of fields:
#   0 - tripduration
#   1 - starttime
#   2 - stoptime
#   3 - start station id
#   4 - start station name
#   5 - start station latitude
#   6 - start station longitude
#   7 - end station id
#   8 - end station name
#   9 - end station latitude
#   10 - end station longitude
#   11 - bikeid
#   12 - usertype
#   13 - birth year
#   14 - gender
# We are only interested in the fields 1, 2, 5, 6, 9, 10 and 11.

# The location of the city bike trip data.
bike_trip_data_folder_location = "Z:\\data_engineering\\bike_trip_data"
bike_trip_files = [bike_trip_data_folder_location + "\\2013-" + ("%02d" % i) + " - Citi bike trip data.csv" for i in range(6, 13)]

# The location of the output data.
bike_output_folder_location = "Z:\\data_engineering\\bike_pre_processed_trip_data"
bike_output_files = [bike_output_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(6, 13)]


# Do all pre-processing cleanup on each separate data file.
def pre_process_bike_files():
    for i in range(0, len(bike_output_files)):
        pre_process_bike_file(i)


def pre_process_bike_file(month_id):
    print("Pre-processing", bike_trip_files[month_id], "into", bike_output_files[month_id])

    with open(bike_output_files[month_id], "w") as output_file:
        with open(bike_trip_files[month_id], "r") as data_file:
            is_first = True

            # Write the header of the file.
            # Note that some entries are not in the order specified in the file!
            output_file.write("bike_id,start_time,end_time,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,trip_duration")

            for x in data_file:
                if is_first:
                    is_first = False
                    continue

                # TODO do we have any cleanups for bike data?

                # Select the fields we are interested in.
                # Note that we should remove ", which is present on every line.
                fields_x = x.strip().replace("\"", "").split(",")

                # We are only interested in the fields 0, 1, 2, 5, 6, 9, 10 and 11.
                target_fields = fields_x[11:12] + fields_x[1:3] + fields_x[6:7] + fields_x[5:6] + fields_x[10:11] + \
                                fields_x[9:10] + fields_x[0:1]

                try:
                    # Sometimes, one of the positional data fields is null.
                    # Thus, it is wise to check for NULL fields in our fields of interest, and all associations.
                    if any(fields_x[i] == "NULL" for i in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]):
                        # One of the positions is not set, so skip.
                        continue

                    # We don't want the trip duration to be 0.
                    if fields_x[0] == 0:
                        continue

                    # Check the distance requirements.
                    if is_valid_position(fields_x[6], fields_x[5]) \
                            and is_valid_position(fields_x[10], fields_x[9]):

                        # Add the entry to the pre-processing file.
                        output_file.write('\n' + ",".join(target_fields))
                except ValueError:
                    print("Value error on line \"" + x.strip() + "\"")


if any(os.path.isfile(filename) for filename in bike_output_files):
    answer = ""
    while answer not in ["y", "n"]:
        answer = input("Pre-processed bike files already exist. Are you sure you want to continue [Y/N]? ").lower()
    if answer == "y":
        pre_process_bike_files()
else:
    pre_process_bike_files()
