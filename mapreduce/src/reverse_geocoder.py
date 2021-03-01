import shapefile
from shapely.geometry import shape, Point


class ReverseGeocoder(object):

    def __init__(self):
        myshp = open("./shapefiles/us_states.shp", "rb")
        mydbf = open("./shapefiles/us_states.dbf", "rb")
        self.shp_reader = shapefile.Reader(shp=myshp, dbf=mydbf)


    # TODO: optimize the speed of this using some trees
    def get_state_record(self, lat, lon):
        # shapely requires the coordinates in this order
        point = Point(lon, lat)

        for index, shp in enumerate(self.shp_reader.shapes()):
            s = shape(shp)
            if s.contains(point):
                # print("point is in:", self.shp_reader.record(index)["STUSPS"], self.shp_reader.record(index)["NAME"])
                return self.shp_reader.record(index)