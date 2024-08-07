CREATE
OR REPLACE TABLE dsongcp.flights AS
SELECT
    FlightDate AS FL_DATE,
    Reporting_Airline AS UNIQUE_CARRIER,
    OriginAirportSeqID AS ORIGIN_AIRPORT_SEQ_ID,
    Origin AS ORIGIN,
    DestAirportSeqID AS DEST_AIRPORT_SEQ_ID,
    Dest AS DEST,
    CRSDepTime AS CRS_DEP_TIME,
    DepTime AS DEP_TIME,
    CAST(DepDelay AS FLOAT64) AS DEP_DELAY,
    CAST(TaxiOut AS FLOAT64) AS TAXI_OUT,
    WheelsOff AS WHEELS_OFF,
    WheelsOn AS WHEELS_ON,
    CAST(TaxiIn AS FLOAT64) AS TAXI_IN,
    CRSArrTime AS CRS_ARR_TIME,
    ArrTime AS ARR_TIME,
    CAST(ArrDelay AS FLOAT64) AS ARR_DELAY,
    IF(Cancelled = '1.00', True, False) AS CANCELLED,
    IF(Diverted = '1.00', True, False) AS DIVERTED,
    -- para flights_auto
    -- IF(Cancelled = 1.00, True, False) AS CANCELLED,
    -- IF(Diverted = 1.00, True, False) AS DIVERTED,
    DISTANCE
FROM
    dsongcp.flights_raw;

-- Desde la tabla flights_auto ya que no podemos crear tabla temporal
-- en el sandbox

-- Resultado de consultas entre 01-Nov-2022 y 31-Oct-2023
-- 1.743.880 Vuelos de un total de 6.817.233
CREATE
OR REPLACE TABLE dsongcp.delayed_10 AS
SELECT
    *
FROM
    dsongcp.flights
WHERE
    dep_delay >= 10;

-- 1.457.459 Vuelos de un total de 6.817.233
CREATE
OR REPLACE TABLE dsongcp.delayed_15 AS
SELECT
    *
FROM
    dsongcp.flights
WHERE
    dep_delay >= 15;

-- 1.243.758 Vuelos de un total de 6.817.233
CREATE
OR REPLACE TABLE dsongcp.delayed_20 AS
SELECT
    *
FROM
    dsongcp.flights
WHERE
    dep_delay >= 20;