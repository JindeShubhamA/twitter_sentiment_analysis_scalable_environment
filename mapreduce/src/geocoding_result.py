from typing import Optional as Opt, Tuple

from shapely.geometry import Polygon

Record = Tuple[str, str]
BoundingBox = Tuple[float, float, float, float]

class GeocodingResult(object):
    def __init__(self,
                 shape: Opt[Polygon],
                 bounds: Opt[BoundingBox],
                 record: Opt[Record]):
        self.shape = shape
        self.bounds = bounds
        self.record = record