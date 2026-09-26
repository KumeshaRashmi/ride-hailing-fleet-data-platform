from processing.common import zone_from_coordinates


def test_zone_grid_covers_each_simulated_colombo_quadrant():
    assert zone_from_coordinates(6.90, 79.82) == "colombo_south_west"
    assert zone_from_coordinates(6.90, 79.90) == "colombo_south_east"
    assert zone_from_coordinates(6.98, 79.82) == "colombo_north_west"
    assert zone_from_coordinates(6.98, 79.90) == "colombo_north_east"


def test_outside_coordinates_are_flagged():
    assert zone_from_coordinates(7.06, 79.90) == "outside_service_area"
