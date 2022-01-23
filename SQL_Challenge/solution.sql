-- LIDL-CASE-STUDY(SQL Challenge)

-- 1. Given an IMPRESSIONS table with product_id, click (an indicator that the product was clicked), and date,
--    write a query that will tell you the click-through-rate of each product by month

SELECT product_id,
       Month(date) AS month,
       Round(( Sum(CASE
                     WHEN click = 'true' THEN 1.0
                     ELSE 0
                   END) * 100 / Count(click) ), 2) AS click_through_rate
FROM   impressions
GROUP  BY product_id,
          Month(date)
ORDER  BY product_id


-- 2. Given the above tables write a query that depict the top 3 performing categories in terms of click through rate.

SELECT category_id,
       click_through_rate
FROM   (SELECT category_id,
               click_through_rate,
               Row_number()
                 OVER (
                   ORDER BY click_through_rate DESC) AS row_num
        FROM   (SELECT product_id,
                       Round(( Sum(CASE
                                     WHEN click = 'true' THEN 1.0
                                     ELSE 0
                                   END) * 100 / Count(click) ), 2) AS
                       click_through_rate
                FROM   impressions
                GROUP  BY product_id
                ORDER  BY product_id) AS a
               JOIN products AS b
                 ON a.product_id = b.product_id
        ORDER  BY click_through_rate DESC) c
WHERE  row_num < 4


-- 3. Click-through-rate by price tier (0-5, 5-10, 10-15, >15)

SELECT price_tier,
       Round(( Sum(CASE
                     WHEN click = 'true' THEN 1.0
                     ELSE 0
                   END) * 100 / Count(click) ), 2) AS click_through_rate
FROM   (SELECT price_tier,
               click
        FROM   impressions a
               JOIN (SELECT product_id,
                            CASE
                              WHEN price <= 5 THEN '0-5'
                              WHEN price > 5
                                   AND price <= 10 THEN '6-10'
                              WHEN price > 10
                                   AND price <= 15 THEN '10-15'
                              ELSE '>15'
                            END AS price_tier
                     FROM   products) b
                 ON a.product_id = b.product_id) c
GROUP  BY price_tier
ORDER  BY price_tier