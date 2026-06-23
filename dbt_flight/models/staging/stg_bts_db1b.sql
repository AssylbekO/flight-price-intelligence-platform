WITH source_data AS (
    SELECT * FROM {{ source('raw', 'bts_db1b') }}
)
renamed_and_casted AS (
    SELECT DISTINCT
        itin_id ::  BIGINT  as Itinerary ID,
        coupons :: INT as Number of coupons,
        year :: INT as Year,
        quarter :: INT as Quarter,
        origin :: TEXT as Origin airport,
        origin_apt_ind :: INT as Origin airport indicator,
        origin_city_num :: INT as Origin city number,
        origin_country :: TEXT as Origin country,
        origin_state_fips :: TEXT as Origin state FIPS code,
        origin_state_name :: TEXT as Origin state name,
        origin_wac :: INT as Origin WAC,
        dest :: TEXT as Destination airport,
        dest_apt_ind :: INT as Destination airport indicator,
        dest_city_num :: INT as Destination city number,
        dest_country :: TEXT as Destination country,
        dest_state_fips :: TEXT as Destination state FIPS code,
        dest_state_name :: TEXT as Destination state name,
        dest_wac :: INT as Destination WAC,
        passengers :: NUMERIC as Number of passengers,
        itin_fare :: NUMERIC as Itinerary fare,
        bulk_fare :: INT as Bulk fare,
        distance :: NUMERIC as Distance,
        distance_group :: INT as Distance group,
        miles_flown :: NUMERIC as Miles flown,
        itin_geo_type :: INT as Itinerary geographic type,
        MAKE_DATE("Year"::INT, ("Quarter"::INT * 3) - 2, 1) AS quarter_start_date
    FROM source_data
    WHERE itin_fare >= 0
      AND itin_fare < 25000
      AND passengers > 0
      AND distance > 0
)
SELECT * FROM renamed_and_casted