
-- demo user

CREATE SETTINGS PROFILE `demo` SETTINGS readonly = 1 CHANGEABLE_IN_READONLY min=1 max=1, add_http_cors_header = true, max_execution_time = 60., max_rows_to_read = 10000000000, max_bytes_to_read = 1000000000000, max_network_bandwidth = 25000000, max_memory_usage = 20000000000, max_bytes_before_external_group_by = 10000000000, max_result_rows = 1000, max_result_bytes = 10000000, result_overflow_mode = 'break' CHANGEABLE_IN_READONLY, read_overflow_mode = 'break' CHANGEABLE_IN_READONLY, enable_http_compression = true, use_query_cache = false, max_block_size = 65409 CHANGEABLE_IN_READONLY, allow_experimental_analyzer = true CHANGEABLE_IN_READONLY TO demo_role

CREATE ROLE demo_role SETTINGS PROFILE `demo`

CREATE USER demo IDENTIFIED WITH double_sha1_hash BY 'BE1BDEC0AA74B4DCB079943E70528096CCA985F8' DEFAULT ROLE demo_role

GRANT SELECT ON amazon.* TO demo_role
GRANT SELECT ON cell_towers.* TO demo_role
GRANT SELECT ON country.* TO demo_role
GRANT dictGet ON country.country_iso_codes TO demo_role
GRANT dictGet ON country.country_polygons TO demo_role
GRANT SELECT ON covid.* TO demo_role
GRANT SELECT ON defaults.queries TO demo_role
GRANT SELECT ON dns.* TO demo_role
GRANT SELECT ON environmental.* TO demo_role
GRANT SELECT ON food.* TO demo_role
GRANT SELECT ON forex.* TO demo_role
GRANT SELECT ON geo.* TO demo_role
GRANT SELECT ON git.* TO demo_role
GRANT SELECT ON hackernews.* TO demo_role
GRANT SELECT ON imdb.* TO demo_role
GRANT SELECT ON json.* TO demo_role
GRANT SELECT ON logs.* TO demo_role
GRANT SELECT ON metrica.* TO demo_role
GRANT SELECT ON mgbench.* TO demo_role
GRANT SELECT, dictGet ON mta.* TO demo_role
GRANT SELECT ON noaa.* TO demo_role
GRANT dictGet ON noaa.resorts_dict TO demo_role
GRANT dictGet ON noaa.states TO demo_role
GRANT dictGet ON noaa.stations_dict TO demo_role
GRANT SELECT ON nyc_taxi.* TO demo_role
GRANT SELECT ON nypd.* TO demo_role
GRANT SELECT ON ontime.* TO demo_role
GRANT SELECT ON opensky.* TO demo_role
GRANT SELECT ON random.* TO demo_role
GRANT SELECT ON reddit.* TO demo_role
GRANT SELECT ON stackoverflow.* TO demo_role
GRANT SELECT ON star_schema.* TO demo_role
GRANT SELECT ON stock.* TO demo_role
GRANT SELECT ON system.parts TO demo_role
GRANT SELECT ON tw_weather.* TO demo_role
GRANT SELECT ON twitter.* TO demo_role
GRANT SELECT ON uk.* TO demo_role
GRANT dictGet ON uk.uk_codes_dict TO demo_role
GRANT SELECT ON wiki.* TO demo_role
GRANT SELECT ON wikistat_benchmark.* TO demo_role
GRANT SELECT ON words.* TO demo_role
GRANT SELECT ON youtube.* TO demo_role

-- monitor user

CREATE USER monitor IDENTIFIED WITH double_sha1_hash BY 'BE1BDEC0AA74B4DCB079943E70528096CCA985F8' SETTINGS readonly = 1, add_http_cors_header = true, max_execution_time = 1., max_rows_to_read = 1000, max_bytes_to_read = 1000000000, max_network_bandwidth = 25000000, max_memory_usage = 1000000000, max_bytes_before_external_group_by = 10000000000, max_result_rows = 1000, max_result_bytes = 10000000, result_overflow_mode = 'break'

GRANT REMOTE ON *.* TO monitor
GRANT SELECT ON amazon.* TO monitor
GRANT SELECT ON cell_towers.* TO monitor
GRANT SELECT ON country.* TO monitor
GRANT dictGet ON country.country_iso_codes TO monitor
GRANT dictGet ON country.country_polygons TO monitor
GRANT SELECT ON covid.* TO monitor
GRANT SELECT ON default.queries TO monitor
GRANT SELECT ON default.tables TO monitor
GRANT SELECT ON dns.* TO monitor
GRANT SELECT ON environmental.* TO monitor
GRANT SELECT ON food.* TO monitor
GRANT SELECT ON forex.* TO monitor
GRANT SELECT ON geo.* TO monitor
GRANT SELECT ON git.* TO monitor
GRANT SELECT ON hackernews.* TO monitor
GRANT SELECT ON imdb.* TO monitor
GRANT SELECT ON json.* TO monitor
GRANT SELECT ON logs.* TO monitor
GRANT SELECT ON metrica.* TO monitor
GRANT SELECT ON mgbench.* TO monitor
GRANT SELECT ON mta.* TO monitor
GRANT SELECT ON noaa.* TO monitor
GRANT dictGet ON noaa.resorts_dict TO monitor
GRANT dictGet ON noaa.states TO monitor
GRANT dictGet ON noaa.stations_dict TO monitor
GRANT SELECT ON nyc_taxi.* TO monitor
GRANT SELECT ON nypd.* TO monitor
GRANT SELECT ON ontime.* TO monitor
GRANT SELECT ON opensky.opensky TO monitor
GRANT SELECT ON random.* TO monitor
GRANT SELECT ON reddit.* TO monitor
GRANT SELECT ON stackoverflow.* TO monitor
GRANT SELECT ON star_schema.* TO monitor
GRANT SELECT ON stock.* TO monitor
GRANT SELECT ON system.columns TO monitor
GRANT SELECT(elapsed, initial_user, query_id, read_bytes, read_rows, total_rows_approx) ON system.processes TO monitor
GRANT SELECT ON system.settings_profile_elements TO monitor
GRANT SELECT ON system.settings_profiles TO monitor
GRANT SELECT ON tw_weather.* TO monitor
GRANT SELECT ON twitter.* TO monitor
GRANT SELECT ON uk.* TO monitor
GRANT dictGet ON uk.uk_codes_dict TO monitor
GRANT SELECT ON wiki.* TO monitor
REVOKE SELECT ON wiki.`.inner_id.4637f083-f896-4c10-bf5c-7640be538c79` FROM monitor
GRANT SELECT ON words.* TO monitor
GRANT SELECT ON youtube.* TO monitor
GRANT monitor TO monitor

-- monitor_admin

CREATE USER monitor_admin IDENTIFIED WITH sha256_password --password omitted

GRANT KILL QUERY ON *.* TO monitor_admin;
GRANT SELECT(initial_address, query_id) ON system.processes TO monitor_admin;
GRANT SELECT ON system.settings_profile_elements TO monitor_admin;
GRANT SELECT ON system.settings_profiles TO monitor_admin;

-- quotas

CREATE QUOTA demo KEYED BY ip_address FOR INTERVAL 1 hour MAX queries = 60, result_rows = 3000000000, read_rows = 3000000000000, execution_time = 6000 TO demo
CREATE QUOTA monitor KEYED BY ip_address FOR INTERVAL 1 hour MAX queries = 1200 TO monitor
