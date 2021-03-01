import shapefile
from shapely.geometry import shape, Point


class ReverseGeocoder(object):

    def __init__(self):
        myshp = open("./large-shapefiles/us_states.shp", "rb")
        mydbf = open("./large-shapefiles/us_states.dbf", "rb")
        self.shp_reader = shapefile.Reader(shp=myshp, dbf=mydbf)

        unsorted = [
            {"shape": shape(x), "bounds": shape(x).bounds, "record": self.shp_reader.record(index)}
            for index, x in enumerate(self.shp_reader.shapes())
        ]

        # use a variable to store the index that represents the maximum longitude in a bounding box tuple
        self.max_lon_ind = 2
        # create a sorted array that contains the shape, the bounding boxes and the metadata of all states,
        # sorted by maximum longitude, smallest to largest
        # longitude was selected since the US is wider than it is tall,
        # but experimentation would be the only way to know the best option for sure
        self.max_lon_sorted = sorted(unsorted, key=lambda x: x["bounds"][self.max_lon_ind])
        # print(self.max_lon_sorted)


    def get_state_by_coords(self, lat, lon):
        # shapely requires the coordinates in this order
        point = Point(lon, lat)

        start = 0
        end = len(self.max_lon_sorted)

        # we perform binary search on the maximum longitude
        # the idea is that we can set the start parameter to the item
        # with the bounding box with the smallest maximum longitude
        # that is larger than the longitude we are searching for
        # see representation below, we are looking for the box whose right side is the closest on the right side of X
        # |____|  |____|  |___X|
        # we can't do full binary search since the bounding boxes are different sizes and can overlap,
        # and because we can't sort by min and max lon at the same time,
        # nor can we sort by lat and lon at the same time
        while self.max_lon_sorted[start]["bounds"][self.max_lon_ind] < lon and start < end-1:
            middle = int((start + end) / 2)
            # if the middle is still less than the desired point, we can increase the start
            if self.max_lon_sorted[middle]["bounds"][self.max_lon_ind] < lon:
                start = middle
            # if the middle is larger than the desired point, we can decrease the end
            else: end = middle

        # now that we have the starting index,
        # i.e. the first box that can enclose X according to its right side position,
        # we can start there, and hopefully the real enclosing state shouldn't be too far away in the list
        for ind in range(start, len(self.max_lon_sorted)):
            # print(ind)
            item = self.max_lon_sorted[ind]
            s = item["shape"]
            if s.contains(point):
                # print("point is in:", self.shp_reader.record(index)["STUSPS"], self.shp_reader.record(index)["NAME"])
                return item["record"]


    def get_state_record(self, coord_string):
        coords = coord_string.split(",")
        return self.get_state_by_coords(coords[0], coords[1])

               

if __name__ == "__main__":
    rg = ReverseGeocoder()
    print(rg.get_state_by_coords(37.241979000000015,-115.81718400000003))

