# The taxi files in which we have positional data.
import random
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
from cartopy.feature import BORDERS
from cartopy.io.img_tiles import StamenTerrain
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter

taxi_folder_location = "Z:\\trip_data"
taxi_data_files = [taxi_folder_location + "\\trip_data_" + str(i) + ".csv" for i in range(1, 13)]

# The bike files in which we have positional data.
bike_folder_location = ""
bike_data_files = []

# Randomness stuff
random.seed(82881)


# Read the start and end points of taxi trips in the given file.
def read_taxi_trips(filename):
    # The trip, encoded as a pair of points.
    # _trips = []

    print("Reading", filename)

    with open(taxi_folder_location + "\\positional_data.csv", "a") as outfile:
        with open(filename, "r") as infile:
            is_first = True

            for line in infile:
                if is_first:
                    is_first = False
                    continue

                data_entries = line.strip().split(",")
                # _trips.append(
                #     (
                #         (int(data_entries[-4]), int(data_entries[-3])),
                #         (int(data_entries[-2]), int(data_entries[-1]))
                #     )
                # )
                outfile.write(",".join(data_entries[-4:]) + '\n')

    # return _trips


def create_scatter_plot(filename, inclusion_probability):
    # plt.style.use('seaborn-whitegrid')
    lons = []
    lats = []
    number_of_points = 0

    with open(filename, "r") as infile:
        is_first = True

        for line in infile:
            if is_first:
                is_first = False
                continue

            if random.random() <= inclusion_probability:
                # print(line.strip())

                # Plot the point at the line in the file.
                data_entries = line.strip().split(",")

                if data_entries[0] == "0":
                    continue

                lon = float(data_entries[0])
                lat = float(data_entries[1])

                if (lon + 74.00597)**2 + (lat - 40.71278)**2 > 3:
                    continue

                lons.append(lon)
                lats.append(lat)
                number_of_points += 1

    print("Number of points in the plot:", number_of_points)
    print("Max lon:", max(lons))
    print("Mean lon:", np.mean(lons))
    print("Min lon:", min(lons))
    print("Max lat:", max(lats))
    print("Mean lat:", np.mean(lats))
    print("Min lat:", min(lats))

    plt.plot(lons, lats, 'o', color='black')
    plt.axis('off')
    plt.show()


def draw_map(filename, inclusion_probability):
    lons = []
    lats = []
    number_of_points = 0

    with open(filename, "r") as infile:
        is_first = True

        for line in infile:
            if is_first:
                is_first = False
                continue

            if random.random() <= inclusion_probability:
                # Plot the point at the line in the file.
                data_entries = line.strip().split(",")

                if data_entries[0] == "0":
                    continue

                lons.append(float(data_entries[0]))
                lats.append(float(data_entries[1]))
                number_of_points += 1

    # What size does the map have?
    extent = min(lons) - 0.005, max(lons) + 0.005, min(lats) - .005, max(lats) + 0.005

    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

    # ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_extent(extents=extent, crs=ccrs.Geodetic())
    # ax.coastlines(resolution='10m')
    # BORDERS.scale = '10m'
    # ax.add_feature(BORDERS)

    # ax.gridlines(color='.5')
    # ax.set_xticks(np.arange(-10, -5, 1), crs=ccrs.PlateCarree())
    # ax.set_yticks(np.arange(36, 43, 1), crs=ccrs.PlateCarree())

    ax.xaxis.set_major_formatter(LongitudeFormatter())
    ax.yaxis.set_major_formatter(LatitudeFormatter())

    st = StamenTerrain()
    ax.add_image(st, 14)

    ax.plot(lons, lats, 'o', transform=ccrs.Geodetic())
    # fig.show()
    fig.savefig('figure_name.svg')


# with open(taxi_folder_location + "\\positional_data.csv", "w") as outfile:
#     outfile.write("latitude_pickup, longitude_pickup, latitude_dropoff, longitude_dropoff")
#
#
# for file in taxi_data_files:
#     read_taxi_trips(file)

create_scatter_plot(taxi_folder_location + "\\positional_data.csv", 0.0001)
# draw_map(taxi_folder_location + "\\positional_data.csv", 0.00001)
