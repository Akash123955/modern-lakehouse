/*
    Macro: is_valid_coordinates
    Description: Validates that latitude and longitude coordinates fall within the
                 approximate bounding box for the New York City metropolitan area.
                 Used to filter out GPS coordinates that are clearly erroneous
                 (e.g., (0,0) or locations outside NYC).

    NYC Bounding Box:
        Latitude:  40.4 to 41.0  (south of Staten Island to north of Bronx)
        Longitude: -74.3 to -73.6 (west of Staten Island to east of Queens/Nassau border)

    Usage:
        WHERE {{ is_valid_coordinates('pickup_latitude', 'pickup_longitude') }}

    Example:
        SELECT *
        FROM raw_trips
        WHERE {{ is_valid_coordinates('pickup_latitude', 'pickup_longitude') }}

    Arguments:
        lat_column  (str): Column name containing latitude values.
        lon_column  (str): Column name containing longitude values.

    Returns:
        SQL boolean expression that evaluates to TRUE when coordinates are within NYC bounds.
*/

{% macro is_valid_coordinates(lat_column, lon_column) %}
    (
        {{ lat_column }} BETWEEN 40.4 AND 41.0
        AND {{ lon_column }} BETWEEN -74.3 AND -73.6
    )
{% endmacro %}
