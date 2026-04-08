-- DATA QUALITY ASSESSMEN

SELECT 
    'user_name' as Column_Name,
    COUNT(*) as Null_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dbo.main_cart_events) AS DECIMAL(5,2)) as Null_Percentage
FROM dbo.main_cart_events
WHERE user_name = 'Unknown' OR user_name IS NULL
UNION ALL
SELECT 
    'user_age',
    COUNT(*),
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dbo.main_cart_events) AS DECIMAL(5,2))
FROM dbo.main_cart_events
WHERE user_age IS NULL
UNION ALL
SELECT 
    'user_gender',
    COUNT(*),
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dbo.main_cart_events) AS DECIMAL(5,2))
FROM dbo.main_cart_events
WHERE user_gender IS NULL
UNION ALL
SELECT 
    'primary_category',
    COUNT(*),
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dbo.main_cart_events) AS DECIMAL(5,2))
FROM dbo.main_cart_events
WHERE primary_category = 'Unknown' OR primary_category IS NULL;


-- CREATE CLEANED DATA TABLE
CREATE TABLE dbo.CartData_Cleaned (
    cart_id INT PRIMARY KEY,
    user_id INT,
    -- Cleaned user information
    user_name NVARCHAR(100),
    user_age INT,  -- Rounded age
    user_gender NVARCHAR(20),
    is_registered_user BIT,  -- New: Whether user is registered
    -- Cart metrics
    total_products INT,
    total_quantity INT,
    cart_value DECIMAL(18, 2),
    product_count INT,
    avg_product_price DECIMAL(18, 2),
    -- Session information
    device_type NVARCHAR(50),
    session_duration_minutes INT,
    timestamp DATETIME2,
    -- Derived date/time fields
    transaction_date DATE,
    transaction_hour INT,
    day_of_week NVARCHAR(20),
    is_weekend BIT,
    -- Abandonment information
    abandoned BIT,
    abandonment_reason NVARCHAR(255),
    abandonment_category NVARCHAR(50),  -- New: Categorized reasons
    -- Category information
    cart_size_category NVARCHAR(50),
    primary_category NVARCHAR(100),
    category_group NVARCHAR(50),  -- New: Grouped categories
    -- Financial metrics
    revenue DECIMAL(18, 2),
    potential_revenue DECIMAL(18, 2),
    -- Data quality flags
    has_valid_data BIT,
    data_quality_score INT,
    -- Metadata
    LoadDate DATETIME2 DEFAULT GETDATE()
);

select * from dbo.CartData_Cleaned


-- CREATE DAILY SUMMARY TABLE
-- Purpose: Daily aggregated metrics for dashboard
CREATE TABLE dbo.Daily_Summary (
    summary_date DATE PRIMARY KEY,
    -- Volume metrics
    total_carts INT,
    total_completed_carts INT,
    total_abandoned_carts INT,
    -- Rate metrics
    abandonment_rate DECIMAL(5, 2),
    completion_rate DECIMAL(5, 2),
    -- Financial metrics
    total_revenue DECIMAL(18, 2),
    total_potential_revenue DECIMAL(18, 2),
    lost_revenue DECIMAL(18, 2),
    avg_cart_value DECIMAL(18, 2),
    avg_completed_cart_value DECIMAL(18, 2),
    -- User metrics
    total_users INT,
    registered_users INT,
    guest_users INT,
    -- Average metrics
    avg_session_duration DECIMAL(10, 2),
    avg_products_per_cart DECIMAL(10, 2),
    LoadDate DATETIME2 DEFAULT GETDATE()
);




-- CREATE CATEGORY PERFORMANCE TABLE
CREATE TABLE dbo.Category_Performance (
    category_group NVARCHAR(50),
    primary_category NVARCHAR(100),
    -- Volume metrics
    total_carts INT,
    completed_carts INT,
    abandoned_carts INT,
    abandonment_rate DECIMAL(5, 2),
    -- Financial metrics
    total_revenue DECIMAL(18, 2),
    potential_revenue DECIMAL(18, 2),
    lost_revenue DECIMAL(18, 2),
    revenue_per_cart DECIMAL(18, 2),
    -- Product metrics
    avg_products_per_cart DECIMAL(10, 2),
    avg_product_price DECIMAL(18, 2),
    -- Session metrics
    avg_session_duration DECIMAL(10, 2),
    LoadDate DATETIME2 DEFAULT GETDATE(),
    PRIMARY KEY (category_group, primary_category)
);


-- CREATE ABANDONMENT ANALYSIS TABLE
CREATE TABLE dbo.Abandonment_Analysis (
    abandonment_category NVARCHAR(50) PRIMARY KEY,
    -- Volume metrics
    total_abandoned_carts INT,
    percentage_of_abandonments DECIMAL(5, 2),
    -- Financial impact
    total_lost_revenue DECIMAL(18, 2),
    avg_lost_cart_value DECIMAL(18, 2),
    -- Cart characteristics
    avg_products_in_cart DECIMAL(10, 2),
    avg_session_duration DECIMAL(10, 2),
    -- Device breakdown
    mobile_count INT,
    desktop_count INT,
    tablet_count INT,
    -- User type breakdown
    registered_users INT,
    guest_users INT,
    LoadDate DATETIME2 DEFAULT GETDATE()
);

CREATE PROCEDURE sp_refresh_all_tables
AS
BEGIN
    SET NOCOUNT ON;

    -- Clear existing data
    TRUNCATE TABLE dbo.CartData_Cleaned;
    TRUNCATE TABLE dbo.Daily_Summary;
    TRUNCATE TABLE dbo.Category_Performance;
    TRUNCATE TABLE dbo.Abandonment_Analysis;

    -- Rebuild CartData_Cleaned
    INSERT INTO dbo.CartData_Cleaned
    SELECT 
        cart_id, user_id,
        CASE WHEN user_name = 'Unknown' THEN 'Guest User' ELSE user_name END,
        ROUND(user_age, 0),
        CASE WHEN user_gender IS NULL THEN 'Unknown' ELSE user_gender END,
        CASE WHEN user_name = 'Unknown' THEN 0 ELSE 1 END,
        total_products, total_quantity, cart_value, product_count, avg_product_price,
        device_type, session_duration_minutes, event_timestamp,
        CAST(event_timestamp AS DATE),
        DATEPART(HOUR, event_timestamp),
        DATENAME(WEEKDAY, event_timestamp),
        CASE WHEN DATEPART(WEEKDAY, event_timestamp) IN (1,7) THEN 1 ELSE 0 END,
        abandoned, abandonment_reason,
        CASE 
            WHEN abandonment_reason LIKE '%price%' OR abandonment_reason LIKE '%cost%' THEN 'Price/Cost Related'
            WHEN abandonment_reason LIKE '%checkout%' OR abandonment_reason LIKE '%complicated%' THEN 'Checkout Issues'
            WHEN abandonment_reason LIKE '%browsing%' THEN 'Just Browsing'
            WHEN abandonment_reason LIKE '%compare%' THEN 'Comparison Shopping'
            WHEN abandonment_reason LIKE '%security%' OR abandonment_reason LIKE '%payment%' THEN 'Security/Payment Concerns'
            WHEN abandonment_reason LIKE '%account%' THEN 'Account Issues'
            ELSE 'Other'
        END,
        cart_size_category,
        CASE WHEN primary_category = 'Unknown' THEN 'Uncategorized' ELSE primary_category END,
        CASE 
            WHEN primary_category IN ('laptops','mobile-accessories') THEN 'Electronics'
            WHEN primary_category IN ('mens-shirts','mens-watches','beauty') THEN 'Fashion & Beauty'
            WHEN primary_category IN ('groceries') THEN 'Groceries'
            WHEN primary_category IN ('kitchen-accessories','home-decoration') THEN 'Home & Kitchen'
            ELSE 'Other'
        END,
        revenue, potential_revenue,
        CASE WHEN cart_value > 0 AND product_count > 0 THEN 1 ELSE 0 END,
        (
            CASE WHEN user_name != 'Unknown' THEN 20 ELSE 0 END +
            CASE WHEN user_age IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN cart_value > 0 THEN 20 ELSE 0 END +
            CASE WHEN product_count > 0 THEN 20 ELSE 0 END +
            CASE WHEN primary_category != 'Unknown' THEN 20 ELSE 0 END
        ),
        GETDATE()
    FROM dbo.main_cart_events;

    -- Rebuild Daily_Summary
    INSERT INTO dbo.Daily_Summary
    SELECT 
        transaction_date,
        COUNT(*), 
        SUM(CASE WHEN abandoned=0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN abandoned=1 THEN 1 ELSE 0 END),
        CAST(SUM(CASE WHEN abandoned=1 THEN 1 ELSE 0 END)*100.0/COUNT(*) AS DECIMAL(5,2)),
        CAST(SUM(CASE WHEN abandoned=0 THEN 1 ELSE 0 END)*100.0/COUNT(*) AS DECIMAL(5,2)),
        SUM(CASE WHEN abandoned=0 THEN revenue ELSE 0 END),
        SUM(potential_revenue),
        SUM(CASE WHEN abandoned=1 THEN potential_revenue ELSE 0 END),
        AVG(cart_value),
        AVG(CASE WHEN abandoned=0 THEN cart_value END),
        COUNT(DISTINCT user_id),
        COUNT(DISTINCT CASE WHEN is_registered_user=1 THEN user_id END),
        COUNT(DISTINCT CASE WHEN is_registered_user=0 THEN user_id END),
        AVG(CAST(session_duration_minutes AS DECIMAL(10,2))),
        AVG(CAST(total_products AS DECIMAL(10,2))),
        GETDATE()
    FROM dbo.CartData_Cleaned
    WHERE has_valid_data = 1
    GROUP BY transaction_date;

    -- Rebuild Category_Performance
    INSERT INTO dbo.Category_Performance
    SELECT 
        category_group, primary_category,
        COUNT(*),
        SUM(CASE WHEN abandoned=0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN abandoned=1 THEN 1 ELSE 0 END),
        CAST(SUM(CASE WHEN abandoned=1 THEN 1 ELSE 0 END)*100.0/COUNT(*) AS DECIMAL(5,2)),
        SUM(revenue), SUM(potential_revenue),
        SUM(CASE WHEN abandoned=1 THEN potential_revenue ELSE 0 END),
        AVG(revenue),
        AVG(CAST(total_products AS DECIMAL(10,2))),
        AVG(avg_product_price),
        AVG(CAST(session_duration_minutes AS DECIMAL(10,2))),
        GETDATE()
    FROM dbo.CartData_Cleaned
    WHERE has_valid_data = 1
    GROUP BY category_group, primary_category;

    -- Rebuild Abandonment_Analysis
    INSERT INTO dbo.Abandonment_Analysis
    SELECT 
        abandonment_category,
        COUNT(*),
        CAST(COUNT(*)*100.0/SUM(COUNT(*)) OVER() AS DECIMAL(5,2)),
        SUM(potential_revenue),
        AVG(potential_revenue),
        AVG(CAST(total_products AS DECIMAL(10,2))),
        AVG(CAST(session_duration_minutes AS DECIMAL(10,2))),
        SUM(CASE WHEN device_type='Mobile' THEN 1 ELSE 0 END),
        SUM(CASE WHEN device_type='Desktop' THEN 1 ELSE 0 END),
        SUM(CASE WHEN device_type='Tablet' THEN 1 ELSE 0 END),
        COUNT(DISTINCT CASE WHEN is_registered_user=1 THEN user_id END),
        COUNT(DISTINCT CASE WHEN is_registered_user=0 THEN user_id END),
        GETDATE()
    FROM dbo.CartData_Cleaned
    WHERE abandoned=1 AND abandonment_category IS NOT NULL AND has_valid_data=1
    GROUP BY abandonment_category;

END;