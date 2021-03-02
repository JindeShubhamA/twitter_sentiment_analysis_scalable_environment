from pyspark import SparkFiles, SparkContext

import shapefile
from search_tree import SearchTree


class ReverseGeocoder(object):
    search_tree = None

    # very catchy name, 10/10 would raise exception again
    class UninitializedReverseGeocoderException(Exception):
        pass


    @staticmethod
    def create_tree():
        shp = open(SparkFiles.get("./shapefiles/us_states.shp"), "rb")
        dbf = open(SparkFiles.get("./shapefiles/us_states.dbf"), "rb")

        shp_reader = shapefile.Reader(shp=shp, dbf=dbf)
        state_names = [(x["State_Name"], x["State_Code"]) for x in shp_reader.records()]

        ReverseGeocoder.search_tree = SearchTree(shp_reader.shapes(), state_names)
        return ReverseGeocoder.search_tree


    """
    alias for create_tree
    """
    @staticmethod
    def initialize():
        return ReverseGeocoder.create_tree()


    """
    turns out that the tree is basically the same as the crude binary search :(
    > timeit.timeit(stmt="rg.get_state_by_coords(37.241979000000015,-115.81718400000003)", setup="import reverse_geocoder; rg = reverse_geocoder.ReverseGeocoder()", number=10000)
    2.749085999999977

    > timeit.timeit(stmt="rg.get_from_tree(37.241979000000015,-115.81718400000003)", setup="import reverse_geocoder; rg = reverse_geocoder.ReverseGeocoder()", number=10000)
    2.7873657
    
    of course, for different coordinates it could be a different result, but I think this shows it doesn't really matter
    """
    @staticmethod
    def get_from_tree(lat, lon, tree=None):
        if tree is None:
            if ReverseGeocoder.search_tree is None: raise ReverseGeocoder.UninitializedReverseGeocoderException
            return ReverseGeocoder.search_tree.find_by_coord(lat, lon)
        else: return tree.find_by_coord(float(lat), float(lon))


    @staticmethod
    def get_from_tree_by_string(string, tree=None):
        coords = string.split(",")
        return ReverseGeocoder.get_from_tree(coords[0], coords[1], tree)

               

if __name__ == "__main__":
    s_tree = ReverseGeocoder.create_tree()
    print(ReverseGeocoder.get_from_tree_by_string("37.241979000000015,-115.81718400000003", s_tree))
    # print(rg.search_tree.tree.left_child, rg.search_tree.tree.right_child)

