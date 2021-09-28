-- Project to on BigQuery based on Google Analytics dataset
-- Table Schema on BigQuery: https://support.google.com/analytics/answer/3437719?hl=en


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

SELECT
  FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',date)) AS month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  SUM(totals.transactionRevenue)/1000000 AS revenue,
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _table_suffix BETWEEN '20170101'
  AND '20170331'
GROUP BY
  month
ORDER BY
  month;


-- Query 02: Bounce rate per traffic source in July 2017

SELECT
  trafficSource.source,
  SUM(totals.visits) AS total_visits,
  SUM(totals.bounces) AS total_no_of_bounces,
  (SUM(totals.bounces)/SUM(totals.visits)*100) AS bounce_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE
  _table_suffix BETWEEN '01'
  AND '31'
GROUP BY
  trafficSource.source
ORDER BY
  total_visits DESC;


-- Query 3: Revenue by traffic source by week, by month in June 2017

SELECT
    'Week' as time_type,
    format_date('%Y%W', parse_date('%Y%m%d', date)) AS time,
    trafficSource.source,
    sum(totals.totalTransactionRevenue)/10000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
Where _table_suffix between '01' and '30'
group by time, trafficSource.source
UNION ALL
SELECT
    'Month' as time_type,
    format_date('%Y%m', parse_date('%Y%m%d', date)) AS time,
    trafficSource.source,
    sum(totals.totalTransactionRevenue)/10000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
Where _table_suffix between '01' and '31'
group by time, trafficSource.source
order by revenue desc;


---Another way with CTEs


with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY time_type,month,source
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY time_type,date,source
order by revenue DESC
)

select * from month_data
union all
select * from week_data



-- --Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

WITH
  purchaser_view AS (
  SELECT
    FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',
        date)) AS month,
    (SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId)) AS avg_pageviews_purchase,
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE
    _table_suffix BETWEEN '0601'
    AND '0731'
    AND totals.transactions IS NOT NULL
  GROUP BY
    month),
  non_purchaser_view AS (
  SELECT
    FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',
        date)) AS month,
    (SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId)) AS avg_pageviews_non_purchase,
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE
    _table_suffix BETWEEN '0601'
    AND '0731'
    AND totals.transactions IS NULL
  GROUP BY
    month)
SELECT
  purchaser_view.month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
FROM
  purchaser_view
LEFT JOIN
  non_purchaser_view
ON
  purchaser_view.month=non_purchaser_view.month
ORDER BY
  month

-- Query 05: Average number of transactions per user that made a purchase in July 2017

SELECT
  FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',
      date)) AS month,
  SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE
  _table_suffix BETWEEN '01'
  AND '31'
  AND totals.transactions>=1
GROUP BY
  month


-- Query 06: Average amount of money spent per session. Only include purchaser data in July 2017

SELECT
  FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',
      date)) AS month,
  SUM(totals.transactionRevenue)/SUM(totals.visits) AS avg_revenue_by_user_per_visit
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE
  _table_suffix BETWEEN '01'
  AND '31'
  AND totals.transactions IS NOT NULL
GROUP BY
  month

-- Result output:
Month	avg_revenue_by_user_per_visit
201707	1.56E+08

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)

SELECT
  product.v2ProductName AS other_pruchased_products,
  SUM(product.productQuantity) AS productQuantity
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
WHERE
  _table_suffix BETWEEN '01'
  AND '31'
  AND fullVisitorId IN (
  SELECT
    fullVisitorId
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE
    _table_suffix BETWEEN '01'
    AND '31'
    AND product.v2ProductName= "YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL )
  AND product.productRevenue IS NOT NULL
  AND product.v2ProductName<>"YouTube Men's Vintage Henley"
GROUP BY
  other_pruchased_products
ORDER BY
  productQuantity DESC


-- Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

WITH
  cte1 AS (
  SELECT
    FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',
        date)) AS month,
    COUNT(hits.eCommerceAction.action_type) AS num_product_view
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _table_suffix BETWEEN '20170101'
    AND '20170331'
    AND hits.eCommerceAction.action_type='2'
  GROUP BY
    month
  ORDER BY
    month),
  cte1_1 AS (
  SELECT
    RANK() OVER(ORDER BY cte1.month) AS row_num,
    cte1.month,
    cte1.num_product_view
  FROM
    cte1),
  cte2 AS (
  SELECT
    ROW_NUMBER() OVER() AS row_num,
    FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',
        date)) AS month,
    COUNT(hits.eCommerceAction.action_type) AS num_addtocard
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
  WHERE
    _table_suffix BETWEEN '20170101'
    AND '20170331'
    AND hits.eCommerceAction.action_type='3'
  GROUP BY
    month
  ORDER BY
    month),
  cte2_1 AS (
  SELECT
    RANK() OVER(ORDER BY cte2.month) AS row_num,
    cte2.month,
    cte2.num_addtocard
  FROM
    cte2),
  cte3 AS (
  SELECT
    FORMAT_DATE('%Y%m', parse_DATE('%Y%m%d',
        date)) AS month,
    COUNT(product.productSKU) AS num_purchase,
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE
    _table_suffix BETWEEN '20170101'
    AND '20170331'
    AND hits.eCommerceAction.action_type='6'
  GROUP BY
    month),
  cte3_1 AS (
  SELECT
    RANK() OVER(ORDER BY cte3.month) AS row_num,
    cte3.month,
    cte3.num_purchase
  FROM
    cte3)
SELECT
  cte1_1.month,
  cte1_1.num_product_view,
  cte2_1.num_addtocard,
  cte3_1.num_purchase,
  ROUND((cte2_1.num_addtocard)/cte1_1.num_product_view*100,2) AS add_to_cart_rate,
  ROUND((cte3_1.num_purchase)/cte1_1.num_product_view*100,2) AS add_to_cart_rate
FROM
  cte1_1
LEFT JOIN
  cte2_1
ON
  cte1_1.row_num=cte2_1.row_num
LEFT JOIN
  cte3_1
ON
  cte1_1.row_num=cte3_1.row_num
ORDER BY
  cte1_1.month
