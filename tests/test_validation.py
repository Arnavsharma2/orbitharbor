"""Tests for shared OrbitHarbor coordinate validation helpers."""

import math

import pytest

from ingestion.validation import coordinates_are_valid


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [
        (0.0, 0.0),
        (-90, -180),
        (90, 180),
        (1.23738, 103.75036),
    ],
)
def test_coordinates_are_valid_accepts_wgs84_bounds(latitude, longitude):
    assert coordinates_are_valid(latitude, longitude)


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [
        (None, 0),
        (0, None),
        ("1.0", 2.0),
        (True, 2.0),
        (91, 0),
        (0, -181),
        (math.nan, 0),
        (0, math.inf),
    ],
)
def test_coordinates_are_valid_rejects_invalid_values(latitude, longitude):
    assert not coordinates_are_valid(latitude, longitude)
