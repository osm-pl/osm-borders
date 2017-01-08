import shapely.geometry


class Feature:
    def __init__(self, geometry: shapely.geometry.base.BaseGeometry, tags: dict = None):
        """
        :type geometry: shapely.geometry.base
        """
        assert isinstance(geometry, shapely.geometry.base.BaseGeometry)
        self.geometry = geometry
        if tags:
            self._tags = tags
        else:
            self._tags = {}

    def set_tag(self, key: str, value: str):
        self._tags[key] = value

    def get_tag(self, key: str) -> str:
        return self._tags[key]

    @property
    def geojson(self):
        return {
            'type': "Feature",
            'geometry': shapely.geometry.mapping(self.geometry),
            'properties': self._tags
        }

    @property
    def tags(self):
        return self._tags

    def __str__(self):
        return str(self._tags)

    @staticmethod
    def from_geojson(dct: dict):
        tags = dct['properties']
        geometry = shapely.geometry.shape(dct['geometry'])
        return Feature(geometry, tags)


class ImmutableFeature:
    def __init__(self, feature: Feature):
        self.geometry = feature.geometry
        self.tags = tuple(sorted(list(feature.tags.items())))

    def __eq__(self, other):
        return self.geometry == other.geometry and self.tags == other.tags

    def __hash__(self):
        return hash((self.geometry.wkt, self.tags))

    def to_feature(self):
        return Feature(self.geometry, dict(self.tags))
