-- BUSINESS ANALYSIS QUERIES

-- EXECUTIVE SUMMARY - KEY METRICS
SELECT 
    -- Overall Performance
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as Abandoned_Carts,
    -- Conversion Metrics (SAFE DIVISION)
    CAST(
        SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2)
    ) as Conversion_Rate,
    CAST(
        SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2)
    ) as Abandonment_Rate,
    -- Financial Metrics (FIXED)
    SUM(CASE WHEN abandoned = 0 THEN revenue ELSE 0 END) as Total_Revenue,
    SUM(CASE WHEN abandoned = 1 THEN potential_revenue ELSE 0 END) as Lost_Revenue,
    SUM(revenue) + SUM(potential_revenue) as Total_Potential_Revenue,
    -- Revenue Opportunity (SAFE)
    CAST(
        SUM(CASE WHEN abandoned = 1 THEN potential_revenue ELSE 0 END) * 100.0 
        / NULLIF(SUM(revenue) + SUM(potential_revenue), 0)
        AS DECIMAL(5,2)
    ) as Revenue_Loss_Percentage,
    -- Average Metrics
    AVG(cart_value) as Avg_Cart_Value,
    AVG(CASE WHEN abandoned = 0 THEN cart_value END) as Avg_Completed_Cart_Value,
    AVG(CASE WHEN abandoned = 1 THEN cart_value END) as Avg_Abandoned_Cart_Value,
    -- User Metrics (CLEAR NAMING)
    COUNT(DISTINCT user_id) as Total_Unique_Users,
    -- These are CART counts (correct naming)
    SUM(CASE WHEN is_registered_user = 1 THEN 1 ELSE 0 END) as Registered_User_Carts,
    SUM(CASE WHEN is_registered_user = 0 THEN 1 ELSE 0 END) as Guest_User_Carts
FROM dbo.CartData_Cleaned
WHERE has_valid_data = 1;

select * from dbo.CartData_Cleaned;


-- DAILY TREND ANALYSIS
SELECT 
    transaction_date,
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as Abandoned_Carts,
    CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as Abandonment_Rate,
    SUM(revenue) as Daily_Revenue,
    SUM(potential_revenue) as Daily_Lost_Revenue,
    AVG(cart_value) as Avg_Cart_Value,
    AVG(CAST(session_duration_minutes AS DECIMAL(10,2))) as Avg_Session_Duration
FROM dbo.CartData_Cleaned
WHERE has_valid_data = 1
GROUP BY transaction_date
ORDER BY transaction_date;


-- ABANDONMENT REASON BREAKDOWN
SELECT 
    abandonment_category,
    COUNT(*) as Abandoned_Carts,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) as Percentage,
    SUM(potential_revenue) as Lost_Revenue,
    AVG(potential_revenue) as Avg_Lost_Value,
    AVG(CAST(session_duration_minutes AS DECIMAL(10,2))) as Avg_Session_Duration,
    AVG(CAST(total_products AS DECIMAL(10,2))) as Avg_Products
FROM dbo.CartData_Cleaned
WHERE abandoned = 1 AND abandonment_category != 'Other'
GROUP BY abandonment_category
ORDER BY Lost_Revenue DESC;


-- DEVICE TYPE PERFORMANCE
SELECT 
    device_type,
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as Abandoned_Carts,
    CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as Abandonment_Rate,
    CAST(SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as Conversion_Rate,
    SUM(revenue) as Total_Revenue,
    SUM(potential_revenue) as Lost_Revenue,
    AVG(cart_value) as Avg_Cart_Value,
    AVG(CAST(session_duration_minutes AS DECIMAL(10,2))) as Avg_Session_Duration
FROM dbo.CartData_Cleaned
WHERE has_valid_data = 1
GROUP BY device_type
ORDER BY Total_Revenue DESC;


-- CATEGORY PERFORMANCE ANALYSIS
SELECT 
    category_group,
    primary_category,
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as Abandonment_Rate,
    SUM(revenue) as Total_Revenue,
    SUM(potential_revenue) as Lost_Revenue,
    AVG(cart_value) as Avg_Cart_Value,
    AVG(avg_product_price) as Avg_Product_Price,
    SUM(revenue) / NULLIF(SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END), 0) as Revenue_Per_Completed_Cart
FROM dbo.CartData_Cleaned
WHERE has_valid_data = 1 AND category_group != 'Other'
GROUP BY category_group, primary_category
ORDER BY Total_Revenue DESC;


-- HOURLY PERFORMANCE ANALYSIS
SELECT 
    transaction_hour,
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as Abandoned_Carts,
    CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as Abandonment_Rate,
    SUM(revenue) as Revenue,
    AVG(CAST(session_duration_minutes AS DECIMAL(10,2))) as Avg_Session_Duration
FROM dbo.CartData_Cleaned
WHERE has_valid_data = 1
GROUP BY transaction_hour
ORDER BY transaction_hour;


-- USER DEMOGRAPHICS PERFORMANCE
SELECT 
    CASE 
        WHEN user_age < 25 THEN '18-24'
        WHEN user_age BETWEEN 25 AND 34 THEN '25-34'
        WHEN user_age BETWEEN 35 AND 44 THEN '35-44'
        WHEN user_age >= 45 THEN '45+'
        ELSE 'Unknown'
    END as Age_Group,
    user_gender,
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as Abandonment_Rate,
    SUM(revenue) as Total_Revenue,
    AVG(cart_value) as Avg_Cart_Value
FROM dbo.CartData_Cleaned
WHERE has_valid_data = 1
GROUP BY 
    CASE 
        WHEN user_age < 25 THEN '18-24'
        WHEN user_age BETWEEN 25 AND 34 THEN '25-34'
        WHEN user_age BETWEEN 35 AND 44 THEN '35-44'
        WHEN user_age >= 45 THEN '45+'
        ELSE 'Unknown'
    END,
    user_gender
ORDER BY Age_Group, user_gender;


-- CART SIZE CATEGORY ANALYSIS
SELECT 
    cart_size_category,
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) as Abandoned_Carts,
    CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as Abandonment_Rate,
    SUM(revenue) as Total_Revenue,
    SUM(potential_revenue) as Lost_Revenue,
    AVG(cart_value) as Avg_Cart_Value,
    MIN(cart_value) as Min_Cart_Value,
    MAX(cart_value) as Max_Cart_Value
FROM dbo.CartData_Cleaned
WHERE has_valid_data = 1
GROUP BY cart_size_category
ORDER BY 
    CASE cart_size_category
        WHEN 'Small' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'Large' THEN 3
        WHEN 'Extra Large' THEN 4
    END;


-- REVENUE RECOVERY OPPORTUNITY ANALYSIS
SELECT 
    abandonment_category,
    device_type,
    cart_size_category,
    COUNT(*) as Abandoned_Carts,
    SUM(potential_revenue) as Total_Recoverable_Revenue,
    AVG(potential_revenue) as Avg_Cart_Value,
    -- Priority Score (higher = more important to fix)
    CAST(
        (COUNT(*) * 0.3) +  -- Volume weight
        ((SUM(potential_revenue) / 1000) * 0.5) +  -- Revenue weight
        ((AVG(potential_revenue) / 100) * 0.2)  -- Avg value weight
    AS DECIMAL(10,2)) as Priority_Score,
    -- Estimated Recovery (if we improve by 25%)
    SUM(potential_revenue) * 0.25 as Estimated_25pct_Recovery,
    -- Recommended Action
    CASE 
        WHEN abandonment_category = 'Price/Cost Related' THEN 'Offer discount coupons or free shipping'
        WHEN abandonment_category = 'Checkout Issues' THEN 'Simplify checkout process'
        WHEN abandonment_category = 'Security/Payment Concerns' THEN 'Add trust badges and security info'
        WHEN abandonment_category = 'Just Browsing' THEN 'Send reminder emails with cart'
        ELSE 'Generic follow-up email'
    END as Recommended_Action
FROM dbo.CartData_Cleaned
WHERE abandoned = 1 AND has_valid_data = 1
GROUP BY abandonment_category, device_type, cart_size_category
HAVING COUNT(*) >= 1
ORDER BY Priority_Score DESC;



-- SESSION DURATION IMPACT ANALYSIS
WITH SessionData AS (
    SELECT 
        CASE 
            WHEN session_duration_minutes <= 5 THEN '0-5 mins'
            WHEN session_duration_minutes <= 15 THEN '6-15 mins'
            WHEN session_duration_minutes <= 30 THEN '16-30 mins'
            ELSE '30+ mins'
        END as Session_Duration_Range,

        CASE 
            WHEN session_duration_minutes <= 5 THEN 1
            WHEN session_duration_minutes <= 15 THEN 2
            WHEN session_duration_minutes <= 30 THEN 3
            ELSE 4
        END as Sort_Order,
        *
    FROM dbo.CartData_Cleaned
    WHERE has_valid_data = 1
)
SELECT 
    Session_Duration_Range,
    COUNT(*) as Total_Carts,
    SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) as Completed_Carts,
    CAST(
        SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(COUNT(*),0)
        AS DECIMAL(5,2)
    ) as Conversion_Rate,

    AVG(cart_value) as Avg_Cart_Value,
    SUM(revenue) as Total_Revenue,
    AVG(CAST(total_products AS DECIMAL(10,2))) as Avg_Products
FROM SessionData
GROUP BY Session_Duration_Range, Sort_Order
ORDER BY Sort_Order;