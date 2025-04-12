WITH date_spine AS (
  SELECT 
    DATEADD(DAY, SEQ4(), '2017-01-01') AS cal_date
  FROM TABLE(GENERATOR(ROWCOUNT => 1500))  -- Generates 1200 rows (you can adjust this)
)
SELECT cal_date,
EXTRACT(YEAR FROM cal_date) AS year,
  EXTRACT(MONTH FROM cal_date) AS month,
  EXTRACT(DAY FROM cal_date) AS day,  
  EXTRACT(QUARTER FROM cal_date) AS quarter  
FROM date_spine
ORDER BY cal_date