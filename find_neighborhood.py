import fiona
from shapely.geometry import Point, shape
from pprint import pprint
from dbfread import DBF
import csv


def add_neighborhoods(nodes):
    with fiona.open("neighbourhoods.shp") as f:
        db = DBF("neighbourhoods.dbf")

        for multi, record in zip(f, db):
            for node in nodes:
                point = Point(node.lon, node.lat)
                if point.within(shape(multi['geometry'])):
                    nodes.district = record["Name"]