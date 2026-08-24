"""Validation helpers shared by geospatial ingestion services."""

import math
from numbers import Real


def coordinates_are_valid(latitude: object, longitude: object) -> bool:
    """Return whether values form a finite WGS84 coordinate pair.

    Zero is a valid value for both axes, so callers must not use truthiness
    checks to distinguish missing coordinates from positions on the Equator or
    prime meridian.
    """
    if isinstance(latitude, bool) or isinstance(longitude, bool):
        return False
    if not isinstance(latitude, Real) or not isinstance(longitude, Real):
        return False

    return (
        math.isfinite(latitude)
        and math.isfinite(longitude)
        and -90 <= latitude <= 90
        and -180 <= longitude <= 180
    )
