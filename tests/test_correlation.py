"""Tests for OrbitHarbor spatial correlation helpers."""

import pandas as pd
import pytest

from dashboard.components.correlation import (
    find_nearby_aircraft,
    find_nearby_vessels,
    haversine_km,
)


def test_vectorized_vessel_distances_match_scalar_haversine():
    tracks = pd.DataFrame(
        {
            "mmsi": [1, 2, 3],
            "latitude": [1.0, 1.1, 5.0],
            "longitude": [103.0, 103.1, 120.0],
        }
    )

    result = find_nearby_vessels(1.0, 103.0, tracks, radius_km=50)

    assert result["mmsi"].tolist() == [1, 2]
    assert result.loc[0, "distance_km"] == pytest.approx(0.0)
    assert result.loc[1, "distance_km"] == pytest.approx(
        haversine_km(1.0, 103.0, 1.1, 103.1)
    )


def test_vectorized_aircraft_distances_ignore_invalid_coordinates():
    tracks = pd.DataFrame(
        {
            "icao24": ["valid", "missing", "text"],
            "latitude": [1.0, None, "invalid"],
            "longitude": [103.0, 103.0, 103.0],
        }
    )

    result = find_nearby_aircraft(1.0, 103.0, tracks, radius_km=20)

    assert result["icao24"].tolist() == ["valid"]
