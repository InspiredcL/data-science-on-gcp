SELECT
    ORIGIN,
    DEP_TIME,
    DEST,
    ARR_TIME,
    ARR_DELAY,
    EVENT_TIME,
    EVENT_TYPE
FROM
    dsongcp.flights_simevents
WHERE
    (
        DEP_DELAY > 15
        and ORIGIN = 'SEA'
    )
    or (
        ARR_DELAY > 15
        and DEST = 'SEA'
    )
ORDER BY
    EVENT_TIME ASC
LIMIT
    5
