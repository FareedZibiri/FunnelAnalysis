WITH unique_query AS (

SELECT 
user_pseudo_id,
event_name,
country,
MIN(event_timestamp) AS event_timestamp,
FROM `turing_data_analytics.raw_events`
WHERE event_name IN ('session_start', 'add_to_cart','begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase' ) 
GROUP BY 
user_pseudo_id, 
event_name,
country
ORDER BY user_pseudo_id
),

top3_countries AS(
SELECT 
country, 
COUNT (event_name) number_of_events
FROM `tc-da-1.turing_data_analytics.raw_events` 
GROUP BY country
ORDER BY COUNT(event_name) DESC
LIMIT 3
)

SELECT
  CASE 
    WHEN event_name = 'session_start' THEN 1
    WHEN event_name = 'add_to_cart' THEN 2
    WHEN event_name = 'begin_checkout' THEN 3
    WHEN event_name = 'add_shipping_info' THEN 4
    WHEN event_name = 'add_payment_info' THEN 5
    WHEN event_name = 'purchase'THEN 6
    END 
    AS 
event_order,
event_name,
COUNT(CASE WHEN country = 'United States' THEN event_name ELSE NULL END) AS number_of_events_US,
COUNT(CASE WHEN country = 'India' THEN event_name ELSE NULL END) AS number_of_events_India,
COUNT(CASE WHEN country = 'Canada' THEN event_name ELSE NULL END) AS number_of_events_Canada,
FROM unique_query 
GROUP BY event_name
ORDER BY event_order