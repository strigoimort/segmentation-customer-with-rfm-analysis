--A.1 Top 5 Category
SELECT cat_name, 
COUNT(order_id) AS total_order, 
FROM bitlabs-dab.G_CID_01.rfm_analysis
WHERE rejected_datetime IS NULL
GROUP BY cat_name
ORDER BY total_order DESC
LIMIT 5



--A.2 Year of Year Trends  
WITH tabel_pre_yoy AS(
  SELECT 
  CASE
    WHEN cat_name = 'Agriculture & Food' THEN 'F&B'
    WHEN cat_name = 'Horeca' THEN 'F&B'
    WHEN cat_name = 'Machinery & Industrial Parts' THEN 'MRO'
    WHEN cat_name = 'Building Materials' THEN 'MRO'
    WHEN cat_name = 'Automotive & Transportation' THEN'MRO'
    WHEN cat_name = 'Health & Medical' THEN 'Health & Beauty'
    WHEN cat_name = 'Beauty, Sport & Fashion' THEN 'Health & Beauty'
    WHEN cat_name = 'Computer & Communication' THEN '3C & Others'
    ELSE '3C & Others'
  END AS segment, 
  FORMAT_DATE('%Y', order_datetime) AS year,
  SUM (SAFE_CAST(gmv AS INT)) AS total_gmv,
  FROM `bitlabs-dab.G_CID_01.rfm_analysis`
  WHERE user_last_login_datetime BETWEEN '2018-01-01' AND '2021-12-31'
  GROUP BY 1,2
)
SELECT *,
((total_gmv - LAG(total_gmv) OVER (PARTITION BY segment ORDER BY year)) * 100.0 / LAG(total_gmv) OVER (PARTITION BY segment ORDER BY year)) AS pct_change_yoy
FROM tabel_pre_yoy
ORDER BY 1,2 ASC



--B.1 Success Order Rate based on Category   
WITH
  tabel AS (
  SELECT
  cat_name,
  COUNT(order_id) AS total_order,
  SUM(CASE
        WHEN rejected_datetime IS NULL THEN 1
        END
        ) AS successed_order,
  SUM(CASE
        WHEN rejected_datetime IS NOT NULL THEN 1
        END
        ) AS canceled_order
  FROM bitlabs-dab.G_CID_01.rfm_analysis
  Where order_id IS NOT NULL
  GROUP BY 1
  )
SELECT
cat_name,
total_order,
successed_order,
canceled_order,
FORMAT('%s%%', CAST(ROUND((successed_order / tabel.total_order) * 100, 2) AS STRING)) AS success_order_rate
FROM tabel
ORDER BY success_order_rate DESC



-- B.2 Success Order Rate based on Category and Order Platform Source Class 
SELECT
DISTINCT cat_name,
CASE
  WHEN platform_source = 'website' OR platform_source = 'CMS' OR platform_source = 'PWA' THEN 'Website'
  WHEN platform_source = 'agent' OR platform_source = 'ios' OR platform_source = 'Android' THEN 'Mobile'
  ELSE 'Website'
END AS platform_source_class,
COUNT (order_id) total_order, 
COUNT(CASE 
        WHEN rejected_datetime IS NULL THEN order_id 
        END
        ) AS successed_order,
COUNT(CASE
        WHEN rejected_datetime IS NOT NULL THEN order_id 
        END
        ) AS cancelled_order,
ROUND(COUNT(CASE 
              WHEN rejected_datetime IS NULL THEN order_id 
              END) / COUNT (order_id)*100, 1) || '%' AS success_order_rate
FROM `bitlabs-dab.G_CID_01.rfm_analysis`
GROUP BY 1,2
ORDER BY 1



-- C.1 Buyer Count based on Bucket Size GMV and Total Transaction
WITH transaction_counts AS (
  SELECT
    user_id,
    COUNT(order_id) AS total_transactions
  FROM bitlabs-dab.G_CID_01.rfm_analysis
  WHERE user_last_login_datetime BETWEEN '2018-01-01 00:00:00 UTC' and '2022-12-31 23:59:59 UTC'
  GROUP BY user_id
)

SELECT
  segment,
  lifetime_gmv_category,
  SUM(CASE WHEN total_transactions < 10 THEN 1 ELSE 0 END) AS trx1,
  SUM(CASE WHEN total_transactions BETWEEN 10 AND 20 THEN 1 ELSE 0 END) AS trx2,
  SUM(CASE WHEN total_transactions BETWEEN 21 AND 30 THEN 1 ELSE 0 END) AS trx3,
  SUM(CASE WHEN total_transactions BETWEEN 31 AND 40 THEN 1 ELSE 0 END) AS trx4,
  SUM(CASE WHEN total_transactions > 40 THEN 1 ELSE 0 END) AS trx5
FROM (
  SELECT
    CASE
      WHEN cat_name = 'Agriculture & Food' THEN 'F&B'
      WHEN cat_name = 'Horeca' THEN 'F&B'
      WHEN cat_name = 'Machinery & Industrial Parts' THEN 'MRO'
      WHEN cat_name = 'Building Materials' THEN 'MRO'
      WHEN cat_name = 'Automotive & Transportation' THEN 'MRO'
      WHEN cat_name = 'Health & Medical' THEN 'Health & Beauty'
      WHEN cat_name = 'Beauty, Sport & Fashion' THEN 'Health & Beauty'
      WHEN cat_name = 'Computer & Communication' THEN '3C & Others'
      ELSE '3C & Others' 
    END AS segment,
    CASE 
      WHEN (SAFE_CAST(gmv AS INT)) < 500000000 THEN '<500 Million IDR'
      WHEN (SAFE_CAST(gmv AS INT)) BETWEEN 500000000 AND 1000000000 THEN '500 Million - 1 Billion IDR'
      WHEN (SAFE_CAST(gmv AS INT)) BETWEEN 1000000000 AND 2000000000 THEN '1 - 2 Billion IDR'
      WHEN (SAFE_CAST(gmv AS INT)) BETWEEN 2000000000 AND 3000000000 THEN '2 - 3 Billion IDR'
      ELSE '>3 Billion IDR'
    END AS lifetime_gmv_category,
    user_id
  FROM bitlabs-dab.G_CID_01.rfm_analysis
) AS data
JOIN transaction_counts ON data.user_id = transaction_counts.user_id
GROUP BY segment, lifetime_gmv_category
ORDER BY segment, lifetime_gmv_category;



-- -- D.1 Active Buyer List in Recent 6 Months
WITH lastest_order as(
  SELECT
    user_id,
    MAX(cast(order_datetime as date)) AS last_order_date,
    cat_name as top_frequently_category_order_lifetime,
    COUNT(DISTINCT order_id) total_order,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(DISTINCT order_id) DESC) AS category_rank,
    sum(safe_cast(gmv as numeric)) total_gmv_lifetime
  FROM bitlabs-dab.G_CID_01.rfm_analysis
  GROUP BY 1,3
  -- qualify category_rank = 1
  -- order by 1
),
category_table as(
  SELECT
    user_id,
    order_datetime,
    cat_name as last_category_order,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_datetime DESC) AS category_order
  FROM bitlabs-dab.G_CID_01.rfm_analysis
  group by 1,2,3
  -- qualify category_order = 1
  -- order by 2
)
SELECT
  distinct ord.user_id,
  last_order_date,
CASE
  WHEN DATE_SUB(DATE('2022-12-31'), INTERVAL 7 DAY) < last_order_date THEN '< 1 week'
  WHEN DATE_SUB(DATE('2022-12-31'), INTERVAL 1 MONTH) < last_order_date THEN '< 1 month'
  WHEN DATE_SUB(DATE('2022-12-31'), INTERVAL 2 MONTH) < last_order_date THEN '< 2 months'
  WHEN DATE_SUB(DATE('2022-12-31'), INTERVAL 3 MONTH) < last_order_date THEN '< 3 months'
  ELSE '< 6 months'
END AS last_order_date_class,
  top_frequently_category_order_lifetime,
  last_category_order,
  total_gmv_lifetime
FROM lastest_order ord
join category_table cat on ord.user_id = cat.user_id
where category_rank = 1 and category_order = 1
order by 5 desc



-- D.2 Inactive Buyer List in Recent 6 Months
WITH lastest_order AS(
  SELECT
    user_id,
    CAST(user_last_login_datetime AS date) last_login_datetime,
    MAX(CAST(order_datetime AS date)) AS last_order_date,
    cat_name AS top_frequently_category_order_lifetime,
    COUNT(DISTINCT order_id) total_order,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(DISTINCT order_id) DESC) AS cat_rank,
    SUM(SAFE_CAST(gmv AS numeric)) total_gmv_lifetime
  FROM `bitlabs-dab.G_CID_01.rfm_analysis`
  WHERE CAST(user_last_login_datetime AS date) < DATE_SUB('2022-12-31', INTERVAL 6 MONTH)
  GROUP BY 1,2,4
),
category_table AS(
  SELECT
    user_id,
    order_datetime,
    cat_name AS last_category_order,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_datetime DESC) AS cat_order
  FROM `bitlabs-dab.G_CID_01.rfm_analysis`
  GROUP BY 1,2,3
)
SELECT
  ord.user_id,
  last_login_datetime,
  CASE
    WHEN last_login_datetime < DATE_SUB('2022-12-31', INTERVAL 24 month) THEN '> 24 months'
    WHEN last_login_datetime < DATE_SUB('2022-12-31', INTERVAL 21 month) THEN '> 21 months'
    WHEN last_login_datetime < DATE_SUB('2022-12-31', INTERVAL 18 month) THEN '> 18 months'
    WHEN last_login_datetime < DATE_SUB('2022-12-31', INTERVAL 15 month) THEN '> 15 months'
    WHEN last_login_datetime < DATE_SUB('2022-12-31', INTERVAL 12 month) THEN '> 12 months'
    WHEN last_login_datetime < DATE_SUB('2022-12-31', INTERVAL 9 month) THEN '> 9 months'
  ELSE '> 6 months'
  END AS last_login_date_class,
  last_order_date,
  top_frequently_category_order_lifetime,
  last_category_order,
  total_gmv_lifetime
FROM lastest_order ord
JOIN category_table cat ON ord.user_id = cat.user_id
WHERE cat_rank = 1 AND cat_order = 1
ORDER BY 7 DESC