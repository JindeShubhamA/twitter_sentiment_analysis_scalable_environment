from pyspark import SparkFiles
import shapefile
from geocoding_result import GeocodingResult
from typing import Optional as Opt, Tuple
from search_tree import SearchTree

Record = Tuple[str, str]


class ReverseGeocoder(object):
    search_tree = None

    # very catchy name, 10/10 would raise exception again
    class UninitializedReverseGeocoderException(Exception):
        pass


    @staticmethod
    def create_tree() -> SearchTree:
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
    def initialize() -> SearchTree:
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
    def get_from_tree(lat: float, lon: float, tree: Opt[SearchTree]=None) -> 'GeocodingResult':
        if tree is None:
            if ReverseGeocoder.search_tree is None: raise ReverseGeocoder.UninitializedReverseGeocoderException
            return ReverseGeocoder.search_tree.find_by_coord(lat, lon)
        else: return tree.find_by_coord(lat, lon)


    @staticmethod
    def get_from_tree_by_string(string: str, tree: Opt[SearchTree]=None) -> 'GeocodingResult':
        coords = string.split(",")
        return ReverseGeocoder.get_from_tree(float(coords[0]), float(coords[1]), tree)

               

if __name__ == "__main__":
    s_tree = ReverseGeocoder.create_tree()
    print(ReverseGeocoder.get_from_tree_by_string("37.241979000000015,-115.81718400000003", s_tree))
    # print(rg.search_tree.tree.left_child, rg.search_tree.tree.right_child)

