CREATE OR REPLACE TABLE
    dsongcp.trainday AS
SELECT
    FL_DATE,
    IF(
        ABS(MOD(FARM_FINGERPRINT(CAST(FL_DATE AS STRING)), 100)) < 70,
        'True',
        'False'
    ) AS is_train_day
FROM
    (
        SELECT DISTINCT
            (FL_DATE) AS FL_DATE
        FROM
            dsongcp.flights_tzcorr
    )
ORDER BY
    FL_DATE