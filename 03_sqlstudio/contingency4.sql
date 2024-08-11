WITH contingency_table AS (
    SELECT 
        THRESH,
        COUNTIF(dep_delay < THRESH AND arr_delay < 15) AS true_positives,
        COUNTIF(dep_delay < THRESH AND arr_delay >= 15) AS false_positives,
        COUNTIF(dep_delay >= THRESH AND arr_delay < 15) AS false_negatives,
        COUNTIF(dep_delay >= THRESH AND arr_delay >= 15) AS true_negatives,
        COUNT(*) AS total
    FROM dsongcp.flights, UNNEST([5, 10, 11, 12, 13, 15, 20]) AS THRESH
    WHERE arr_delay IS NOT NULL AND dep_delay IS NOT NULL
    GROUP BY THRESH
)
-- Los valores comentados son complementarios a los que siguen ejemplo
-- tpr+fnr=1
SELECT
    ROUND((true_positives + true_negatives) / total, 2) AS accuracy,
    -- ROUND(true_positives /(true_positives + false_negatives),2) AS tpr,
    ROUND(false_negatives /(true_positives + false_negatives),2) AS fnr,
    -- ROUND(true_negatives /(true_negatives + false_positives),2) AS tnr,
    ROUND(false_positives /(true_negatives + false_positives),2) AS fpr,
    -- ROUND(true_positives /(true_positives + false_positives),2) AS ppv,
    ROUND(false_positives /(true_positives + false_positives),2) AS fdr,
    -- ROUND(true_negatives /(true_negatives + false_negatives),2) AS npv,
    ROUND(false_negatives /(false_negatives + true_negatives),2) AS fo_r,
    *
FROM contingency_table
