import math
from typing import Optional as Opt, Tuple, Sequence

from shapefile import Shapes
from shapely.geometry import shape, Point

from geocoding_result import GeocodingResult

Record = Tuple[str, str]


class SearchTree(object):

    class Node(object):
        # for a recursive type like this, we have to enclose the type in ''
        def __init__(self, left_child: Opt['SearchTree.Node'], right_child: Opt['SearchTree.Node'], split: float, axis: int):
            self.left_child = left_child
            self.right_child = right_child
            self.split = split
            self.axis = axis


    class Leaf(Node):
        def __init__(self, items: Sequence[GeocodingResult]):
            super().__init__(None, None, float("inf"), -1)
            self.items = items


    def __init__(self, shapes: Shapes, records: Sequence[Record], max_depth: int=None, max_leaf_size: int=1):
        # we set the depth to the log of the length of shapes, plus 3 to allow the tree to spread a bit more
        # this will happen since we add boxes that are intersected by a split to both sides of the tree
        self.max_depth = math.ceil(math.log(len(shapes), 2)) + 3 if max_depth is None else max_depth
        self.max_leaf_size = max_leaf_size

        items = [
            GeocodingResult(shape(x), shape(x).bounds, list(records[index]))
            # {"shape": shape(x), "bounds": shape(x).bounds, "record": records[index]}
            for index, x in enumerate(shapes)
        ]

        self.tree = self.create_tree(items, 0, 0, self.max_depth, self.max_leaf_size)


    """
    Recursive function to create a search tree.
    
    Has a time complexity of n*log(n)*log(n) = n*log(2n), which isn't exactly great, however,
    since this function only needs to be called once, and the tree will be queried many many times,
    it is worth it to spend some more time to make the tree more efficient
    """
    def create_tree(self, items: Sequence[GeocodingResult], axis: int, depth: int, max_depth: int, max_leaf_size: int):
        # base case, either the amount of items is less than the maximum amount of items per leaf,
        # or the depth of the tree is getting too large
        if len(items) <= max_leaf_size or depth >= max_depth:
            return self.Leaf(items)

        left_items = []
        right_items = []

        # the split is computed by taking the median value on the current axis
        # this will (hopefully) create a fairly balanced tree
        sorted_items = sorted(items, key=lambda x: x.bounds[axis])
        split = sorted_items[int(len(sorted_items) / 2)].bounds[axis]

        for item in items:
            # we explicitly allow the box to be added to the left and right side of this branch
            # if the box is intersected by the split
            if item.bounds[axis] <= split:
                left_items.append(item)
                # axis + 2 will get the other side of the box on the same axis;
                # i.e. it will get the right side if [axis] is the left side,
                # and it will get the top side if [axis] is the bottom side
                if item.bounds[axis + 2] > split:
                    right_items.append(item)
            # item.bounds[axis] > split
            else: right_items.append(item)

        # flip the axis from 0 to 1 or from 1 to 0
        flipped_axis = (axis + 1) % 2

        return self.Node(
            self.create_tree(left_items, flipped_axis, depth+1, max_depth, max_leaf_size),
            self.create_tree(right_items, flipped_axis, depth+1, max_depth, max_leaf_size),
            split,
            axis
        )


    def find_by_coord(self, lat: float, lon: float) -> GeocodingResult:
        point = Point(lon, lat)
        node = self.tree

        while not isinstance(node, self.Leaf):
            # shapely puts the coordinates if the bounding box in lon,lat order
            # so axis 0 is the longitude and 1 is the latitude
            axis_val = lon if node.axis == 0 else lat
            if axis_val <= node.split:
                node = node.left_child
            else: node = node.right_child

        for item in node.items:
            if item.shape.contains(point):
                return item

        # return an empty object instead of None, so we can always access "shape", "bounds" and "record"
        # even if they are useless
        return GeocodingResult(None, None, None)


    def find_by_string(self, coord_string: str) -> GeocodingResult:
        coords = coord_string.split(",")
        return self.find_by_coord(float(coords[0]), float(coords[1]))