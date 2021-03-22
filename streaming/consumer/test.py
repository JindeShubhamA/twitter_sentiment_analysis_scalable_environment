import shapefile
from typing import Optional as Opt, Tuple

from shapefile import Shapes
from shapely.geometry import shape, Point
from random import randrange


shp = open("/Users/jindeshubham/PycharmProjects/2021_group_06_s3993914_s3479692_s3409023/streaming/consumer/shapefiles/us_states.shp", "rb")
dbf = open("/Users/jindeshubham/PycharmProjects/2021_group_06_s3993914_s3479692_s3409023/streaming/consumer/shapefiles/us_states.dbf", "rb")


def find_by_string(coord_string) :
    coords = coord_string.split(",")
    return  float(coords[0]), float(coords[1])



shp_reader = shapefile.Reader(shp=shp, dbf=dbf)

lat_lon = "34.297694,118.421676"




all_shapes = shp_reader.shapes() # get all the polygons
all_records = shp_reader.records()
j=0
for i in range(len(all_shapes)):

    boundary = all_shapes[i] # get a boundary polygon
    shape_bndry = shape(boundary)
    point = Point(find_by_string(lat_lon))

    if point.within(shape_bndry):
      j=1

if not j:
    k = randrange(49)
    print(all_records[k][3])
else:
    print(all_records[j][3])



