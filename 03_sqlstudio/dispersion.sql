SELECT t0.UNIQUE_CARRIER,
    COUNT(t0.DEP_DELAY) AS t0___LEGO_COUNT_qt_0kt3mjyled,
    COUNT(t0.ARR_DELAY) AS t0___LEGO_COUNT_qt_1kt3mjyled,
    SUM(t0.DEP_DELAY) AS t0___LEGO_SUM_qt_0kt3mjyled,
    SUM(t0.ARR_DELAY) AS t0___LEGO_SUM_qt_1kt3mjyled
FROM `bigquery-manu-407202.dsongcp.flights` AS t0
WHERE (
        t0.FL_DATE >= DATE '2022-11-01'
        AND t0.FL_DATE <= DATE '2023-03-31'
    )
GROUP BY t0.UNIQUE_CARRIER
LIMIT 2000001;