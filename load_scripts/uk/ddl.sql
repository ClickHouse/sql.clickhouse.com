CREATE MATERIALIZED VIEW uk_price_paid_simple_updater REFRESH EVERY 1 WEEK OFFSET 2 HOUR TO uk_price_paid_simple AS
SELECT
    date,
    town,
    street,
    price
FROM uk_price_paid;

CREATE MATERIALIZED VIEW uk_price_paid_simple_partitioned_updater REFRESH EVERY 1 WEEK OFFSET 2 HOUR TO uk_price_paid_simple_partitioned AS
SELECT
    date,
    town,
    street,
    price
FROM uk_price_paid SETTINGS throw_on_max_partitions_per_insert_block=0;

 CREATE MATERIALIZED VIEW uk_price_paid_updater REFRESH EVERY 1 WEEK TO uk_price_paid AS
WITH
   splitByChar(' ', postcode) AS p
SELECT
    toUInt32(price_string) AS price,
    parseDateTimeBestEffortUS(time) AS date,
    p[1] AS postcode1,
    p[2] AS postcode2,
    transform(a, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other']) AS type,
    b = 'Y' AS is_new,
    transform(c, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown']) AS duration,
    addr1,
    addr2,
    street,
    locality,
    town,
    district,
    county
FROM url('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv', CSV, '
    uuid_string String,
    price_string String,
    time String,
    postcode String,
    a String,
    b String,
    c String,
    addr1 String,
    addr2 String,
    street String,
    locality String,
    town String,
    district String,
    county String,
    d String,
    e String'
) SETTINGS max_http_get_redirects = 10;
