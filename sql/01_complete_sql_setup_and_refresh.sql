/*
Cart Abandonment Analytics Pipeline - SQL Setup + Procedures
------------------------------------------------------------
Run this script in Azure SQL Database / SQL Server.

Purpose:
1. Create staging, main, analytics, and pipeline log tables safely.
2. Upsert new cart events from staging into the main table.
3. Rebuild analytical reporting tables from main_cart_events.
4. Log row counts so you can debug pipeline refresh issues.

Recommended pipeline order in ADF:
Copy JSON to dbo.cart_events
    -> EXEC dbo.sp_upsert_main_cart_event
    -> EXEC dbo.sp_refresh_all_tables
*/

/* 
   1. STAGING TABLE
   ADF Copy Data loads JSON files here first.
   */
IF OBJECT_ID('dbo.cart_events', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.cart_events (
        cart_id INT NULL,
        user_id INT NULL,
        total_products INT NULL,
        total_quantity INT NULL,
        cart_value DECIMAL(18,2) NULL,
        product_count INT NULL,
        avg_product_price DECIMAL(18,2) NULL,
        user_name NVARCHAR(100) NULL,
        user_age FLOAT NULL,
        user_gender NVARCHAR(20) NULL,
        device_type NVARCHAR(50) NULL,
        session_duration_minutes INT NULL,
        event_timestamp DATETIME2 NULL,
        abandoned BIT NULL,
        abandonment_reason NVARCHAR(255) NULL,
        cart_size_category NVARCHAR(50) NULL,
        primary_category NVARCHAR(100) NULL,
        revenue DECIMAL(18,2) NULL,
        potential_revenue DECIMAL(18,2) NULL,
        staging_load_date DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

/* 
   2. MAIN TABLE
   Holds deduplicated cart events.
   */
IF OBJECT_ID('dbo.main_cart_events', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.main_cart_events (
        cart_id INT NOT NULL PRIMARY KEY,
        user_id INT NULL,
        total_products INT NULL,
        total_quantity INT NULL,
        cart_value DECIMAL(18,2) NULL,
        product_count INT NULL,
        avg_product_price DECIMAL(18,2) NULL,
        user_name NVARCHAR(100) NULL,
        user_age FLOAT NULL,
        user_gender NVARCHAR(20) NULL,
        device_type NVARCHAR(50) NULL,
        session_duration_minutes INT NULL,
        event_timestamp DATETIME2 NULL,
        abandoned BIT NULL,
        abandonment_reason NVARCHAR(255) NULL,
        cart_size_category NVARCHAR(50) NULL,
        primary_category NVARCHAR(100) NULL,
        revenue DECIMAL(18,2) NULL,
        potential_revenue DECIMAL(18,2) NULL,
        first_loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        last_updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- If your old table already existed, add missing load-tracking columns safely.
IF COL_LENGTH('dbo.main_cart_events', 'first_loaded_at') IS NULL
    ALTER TABLE dbo.main_cart_events ADD first_loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME();
GO

IF COL_LENGTH('dbo.main_cart_events', 'last_updated_at') IS NULL
    ALTER TABLE dbo.main_cart_events ADD last_updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME();
GO

/* 
   3. ANALYTICS TABLES
   */
IF OBJECT_ID('dbo.CartData_Cleaned', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.CartData_Cleaned (
        cart_id INT,
        user_id INT,
        user_name NVARCHAR(100),
        user_age INT,
        user_gender NVARCHAR(20),
        is_registered_user BIT,
        total_products INT,
        total_quantity INT,
        cart_value DECIMAL(18,2),
        product_count INT,
        avg_product_price DECIMAL(18,2),
        device_type NVARCHAR(50),
        session_duration_minutes INT,
        event_timestamp DATETIME2,
        transaction_date DATE,
        transaction_hour INT,
        day_of_week NVARCHAR(20),
        is_weekend BIT,
        abandoned BIT,
        abandonment_reason NVARCHAR(255),
        abandonment_category NVARCHAR(50),
        cart_size_category NVARCHAR(50),
        primary_category NVARCHAR(100),
        category_group NVARCHAR(50),
        revenue DECIMAL(18,2),
        potential_revenue DECIMAL(18,2),
        has_valid_data BIT,
        data_quality_score INT,
        LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID('dbo.Daily_Summary', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Daily_Summary (
        summary_date DATE,
        total_carts INT,
        total_completed_carts INT,
        total_abandoned_carts INT,
        abandonment_rate DECIMAL(5,2),
        completion_rate DECIMAL(5,2),
        total_revenue DECIMAL(18,2),
        total_potential_revenue DECIMAL(18,2),
        lost_revenue DECIMAL(18,2),
        avg_cart_value DECIMAL(18,2),
        avg_completed_cart_value DECIMAL(18,2),
        total_users INT,
        registered_user_carts INT,
        guest_user_carts INT,
        avg_session_duration DECIMAL(10,2),
        avg_products_per_cart DECIMAL(10,2),
        LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID('dbo.Category_Performance', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Category_Performance (
        category_group NVARCHAR(50),
        primary_category NVARCHAR(100),
        total_carts INT,
        completed_carts INT,
        abandoned_carts INT,
        abandonment_rate DECIMAL(5,2),
        total_revenue DECIMAL(18,2),
        potential_revenue DECIMAL(18,2),
        lost_revenue DECIMAL(18,2),
        revenue_per_completed_cart DECIMAL(18,2),
        avg_products_per_cart DECIMAL(10,2),
        avg_product_price DECIMAL(18,2),
        avg_session_duration DECIMAL(10,2),
        LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID('dbo.Abandonment_Analysis', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Abandonment_Analysis (
        abandonment_category NVARCHAR(50),
        total_abandoned_carts INT,
        percentage_of_abandonments DECIMAL(5,2),
        total_lost_revenue DECIMAL(18,2),
        avg_lost_cart_value DECIMAL(18,2),
        avg_products_in_cart DECIMAL(10,2),
        avg_session_duration DECIMAL(10,2),
        mobile_count INT,
        desktop_count INT,
        tablet_count INT,
        registered_user_carts INT,
        guest_user_carts INT,
        LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

/* 
   4. PIPELINE RUN LOG
   Helps debug whether records reached each layer.
   */
IF OBJECT_ID('dbo.pipeline_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.pipeline_run_log (
        run_id INT IDENTITY(1,1) PRIMARY KEY,
        run_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        staging_rows INT NULL,
        main_rows_before INT NULL,
        main_rows_after INT NULL,
        inserted_or_updated_rows INT NULL,
        cleaned_rows INT NULL,
        daily_summary_rows INT NULL,
        category_rows INT NULL,
        abandonment_rows INT NULL,
        status NVARCHAR(50) NULL,
        message NVARCHAR(4000) NULL
    );
END;
GO

/* 
   5. UPSERT PROCEDURE
   Moves rows from staging into main_cart_events.*/

CREATE OR ALTER PROCEDURE dbo.sp_upsert_main_cart_event
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @staging_rows INT = 0;
    DECLARE @main_before INT = 0;
    DECLARE @main_after INT = 0;
    DECLARE @affected_rows INT = 0;

    SELECT @staging_rows = COUNT(*) FROM dbo.cart_events;
    SELECT @main_before = COUNT(*) FROM dbo.main_cart_events;

    ;WITH DedupSource AS (
        SELECT
            cart_id,
            user_id,
            total_products,
            total_quantity,
            cart_value,
            product_count,
            avg_product_price,
            user_name,
            user_age,
            user_gender,
            device_type,
            session_duration_minutes,
            event_timestamp,
            abandoned,
            abandonment_reason,
            cart_size_category,
            primary_category,
            revenue,
            potential_revenue,
            ROW_NUMBER() OVER (
                PARTITION BY cart_id
                ORDER BY event_timestamp DESC, staging_load_date DESC
            ) AS rn
        FROM dbo.cart_events
        WHERE cart_id IS NOT NULL
    )
    MERGE dbo.main_cart_events AS Target
    USING (
        SELECT *
        FROM DedupSource
        WHERE rn = 1
    ) AS Source
    ON Target.cart_id = Source.cart_id
    WHEN MATCHED THEN
        UPDATE SET
            Target.user_id = Source.user_id,
            Target.total_products = Source.total_products,
            Target.total_quantity = Source.total_quantity,
            Target.cart_value = Source.cart_value,
            Target.product_count = Source.product_count,
            Target.avg_product_price = Source.avg_product_price,
            Target.user_name = Source.user_name,
            Target.user_age = Source.user_age,
            Target.user_gender = Source.user_gender,
            Target.device_type = Source.device_type,
            Target.session_duration_minutes = Source.session_duration_minutes,
            Target.event_timestamp = Source.event_timestamp,
            Target.abandoned = Source.abandoned,
            Target.abandonment_reason = Source.abandonment_reason,
            Target.cart_size_category = Source.cart_size_category,
            Target.primary_category = Source.primary_category,
            Target.revenue = Source.revenue,
            Target.potential_revenue = Source.potential_revenue,
            Target.last_updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            cart_id, user_id, total_products, total_quantity,
            cart_value, product_count, avg_product_price,
            user_name, user_age, user_gender,
            device_type, session_duration_minutes,
            event_timestamp, abandoned, abandonment_reason,
            cart_size_category, primary_category,
            revenue, potential_revenue,
            first_loaded_at, last_updated_at
        )
        VALUES (
            Source.cart_id, Source.user_id, Source.total_products, Source.total_quantity,
            Source.cart_value, Source.product_count, Source.avg_product_price,
            Source.user_name, Source.user_age, Source.user_gender,
            Source.device_type, Source.session_duration_minutes,
            Source.event_timestamp, Source.abandoned, Source.abandonment_reason,
            Source.cart_size_category, Source.primary_category,
            Source.revenue, Source.potential_revenue,
            SYSUTCDATETIME(), SYSUTCDATETIME()
        );

    SET @affected_rows = @@ROWCOUNT;
    SELECT @main_after = COUNT(*) FROM dbo.main_cart_events;

    INSERT INTO dbo.pipeline_run_log (
        staging_rows, main_rows_before, main_rows_after,
        inserted_or_updated_rows, status, message
    )
    VALUES (
        @staging_rows, @main_before, @main_after,
        @affected_rows, 'UPSERT_COMPLETED',
        'Staging rows merged into main_cart_events.'
    );
END;
GO

/*
   6. REFRESH PROCEDURE
   Rebuilds the four analytics tables from main_cart_events. */

CREATE OR ALTER PROCEDURE dbo.sp_refresh_all_tables
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @cleaned_rows INT = 0;
    DECLARE @daily_rows INT = 0;
    DECLARE @category_rows INT = 0;
    DECLARE @abandonment_rows INT = 0;

    TRUNCATE TABLE dbo.CartData_Cleaned;
    TRUNCATE TABLE dbo.Daily_Summary;
    TRUNCATE TABLE dbo.Category_Performance;
    TRUNCATE TABLE dbo.Abandonment_Analysis;

    INSERT INTO dbo.CartData_Cleaned (
        cart_id, user_id, user_name, user_age, user_gender, is_registered_user,
        total_products, total_quantity, cart_value, product_count, avg_product_price,
        device_type, session_duration_minutes, event_timestamp, transaction_date,
        transaction_hour, day_of_week, is_weekend, abandoned, abandonment_reason,
        abandonment_category, cart_size_category, primary_category, category_group,
        revenue, potential_revenue, has_valid_data, data_quality_score, LoadDate
    )
    SELECT
        cart_id,
        user_id,
        CASE WHEN user_name IS NULL OR user_name = 'Unknown' THEN 'Guest User' ELSE user_name END AS user_name,
        TRY_CONVERT(INT, ROUND(user_age, 0)) AS user_age,
        COALESCE(user_gender, 'Unknown') AS user_gender,
        CASE WHEN user_name IS NULL OR user_name = 'Unknown' THEN 0 ELSE 1 END AS is_registered_user,
        total_products,
        total_quantity,
        cart_value,
        product_count,
        avg_product_price,
        COALESCE(device_type, 'Unknown') AS device_type,
        session_duration_minutes,
        event_timestamp,
        CAST(COALESCE(event_timestamp, first_loaded_at) AS DATE) AS transaction_date,
        DATEPART(HOUR, COALESCE(event_timestamp, first_loaded_at)) AS transaction_hour,
        DATENAME(WEEKDAY, COALESCE(event_timestamp, first_loaded_at)) AS day_of_week,
        CASE WHEN DATEPART(WEEKDAY, COALESCE(event_timestamp, first_loaded_at)) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
        COALESCE(abandoned, 0) AS abandoned,
        abandonment_reason,
        CASE
            WHEN COALESCE(abandoned, 0) = 0 THEN 'Completed Cart'
            WHEN abandonment_reason LIKE '%price%' OR abandonment_reason LIKE '%cost%' THEN 'Price/Cost Related'
            WHEN abandonment_reason LIKE '%checkout%' OR abandonment_reason LIKE '%complicated%' THEN 'Checkout Issues'
            WHEN abandonment_reason LIKE '%browsing%' THEN 'Just Browsing'
            WHEN abandonment_reason LIKE '%compare%' THEN 'Comparison Shopping'
            WHEN abandonment_reason LIKE '%security%' OR abandonment_reason LIKE '%payment%' THEN 'Security/Payment Concerns'
            WHEN abandonment_reason LIKE '%account%' THEN 'Account Issues'
            ELSE 'Other'
        END AS abandonment_category,
        COALESCE(cart_size_category, 'Unknown') AS cart_size_category,
        CASE WHEN primary_category IS NULL OR primary_category = 'Unknown' THEN 'Uncategorized' ELSE primary_category END AS primary_category,
        CASE
            WHEN primary_category IN ('smartphones','laptops','tablets','mobile-accessories') THEN 'Electronics'
            WHEN primary_category IN ('mens-shirts','mens-shoes','mens-watches','womens-bags','womens-dresses','womens-jewellery','womens-shoes','womens-watches','tops','sunglasses','beauty','fragrances','skin-care') THEN 'Fashion & Beauty'
            WHEN primary_category IN ('groceries') THEN 'Groceries'
            WHEN primary_category IN ('kitchen-accessories','home-decoration','furniture') THEN 'Home & Kitchen'
            WHEN primary_category IN ('vehicle','motorcycle') THEN 'Automotive'
            WHEN primary_category IS NULL OR primary_category = 'Unknown' THEN 'Uncategorized'
            ELSE 'Other'
        END AS category_group,
        COALESCE(revenue, 0) AS revenue,
        COALESCE(potential_revenue, 0) AS potential_revenue,
        CASE
            WHEN cart_id IS NOT NULL
             AND COALESCE(cart_value, 0) > 0
             AND COALESCE(product_count, 0) > 0
             AND COALESCE(total_products, 0) > 0
            THEN 1 ELSE 0
        END AS has_valid_data,
        (
            CASE WHEN user_name IS NOT NULL AND user_name <> 'Unknown' THEN 20 ELSE 0 END +
            CASE WHEN user_age IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN COALESCE(cart_value, 0) > 0 THEN 20 ELSE 0 END +
            CASE WHEN COALESCE(product_count, 0) > 0 THEN 20 ELSE 0 END +
            CASE WHEN primary_category IS NOT NULL AND primary_category <> 'Unknown' THEN 20 ELSE 0 END
        ) AS data_quality_score,
        SYSUTCDATETIME() AS LoadDate
    FROM dbo.main_cart_events;

    SET @cleaned_rows = @@ROWCOUNT;

    INSERT INTO dbo.Daily_Summary (
        summary_date, total_carts, total_completed_carts, total_abandoned_carts,
        abandonment_rate, completion_rate, total_revenue, total_potential_revenue,
        lost_revenue, avg_cart_value, avg_completed_cart_value, total_users,
        registered_user_carts, guest_user_carts, avg_session_duration,
        avg_products_per_cart, LoadDate
    )
    SELECT
        transaction_date,
        COUNT(*) AS total_carts,
        SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) AS total_completed_carts,
        SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) AS total_abandoned_carts,
        CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS abandonment_rate,
        CAST(SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS completion_rate,
        SUM(revenue) AS total_revenue,
        SUM(revenue + potential_revenue) AS total_potential_revenue,
        SUM(CASE WHEN abandoned = 1 THEN potential_revenue ELSE 0 END) AS lost_revenue,
        AVG(cart_value) AS avg_cart_value,
        AVG(CASE WHEN abandoned = 0 THEN cart_value END) AS avg_completed_cart_value,
        COUNT(DISTINCT user_id) AS total_users,
        SUM(CASE WHEN is_registered_user = 1 THEN 1 ELSE 0 END) AS registered_user_carts,
        SUM(CASE WHEN is_registered_user = 0 THEN 1 ELSE 0 END) AS guest_user_carts,
        AVG(CAST(session_duration_minutes AS DECIMAL(10,2))) AS avg_session_duration,
        AVG(CAST(total_products AS DECIMAL(10,2))) AS avg_products_per_cart,
        SYSUTCDATETIME() AS LoadDate
    FROM dbo.CartData_Cleaned
    WHERE has_valid_data = 1
      AND transaction_date IS NOT NULL
    GROUP BY transaction_date;

    SET @daily_rows = @@ROWCOUNT;

    INSERT INTO dbo.Category_Performance (
        category_group, primary_category, total_carts, completed_carts,
        abandoned_carts, abandonment_rate, total_revenue, potential_revenue,
        lost_revenue, revenue_per_completed_cart, avg_products_per_cart,
        avg_product_price, avg_session_duration, LoadDate
    )
    SELECT
        category_group,
        primary_category,
        COUNT(*) AS total_carts,
        SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END) AS completed_carts,
        SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) AS abandoned_carts,
        CAST(SUM(CASE WHEN abandoned = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS abandonment_rate,
        SUM(revenue) AS total_revenue,
        SUM(potential_revenue) AS potential_revenue,
        SUM(CASE WHEN abandoned = 1 THEN potential_revenue ELSE 0 END) AS lost_revenue,
        SUM(revenue) / NULLIF(SUM(CASE WHEN abandoned = 0 THEN 1 ELSE 0 END), 0) AS revenue_per_completed_cart,
        AVG(CAST(total_products AS DECIMAL(10,2))) AS avg_products_per_cart,
        AVG(avg_product_price) AS avg_product_price,
        AVG(CAST(session_duration_minutes AS DECIMAL(10,2))) AS avg_session_duration,
        SYSUTCDATETIME() AS LoadDate
    FROM dbo.CartData_Cleaned
    WHERE has_valid_data = 1
      AND category_group IS NOT NULL
      AND primary_category IS NOT NULL
    GROUP BY category_group, primary_category;

    SET @category_rows = @@ROWCOUNT;

    INSERT INTO dbo.Abandonment_Analysis (
        abandonment_category, total_abandoned_carts, percentage_of_abandonments,
        total_lost_revenue, avg_lost_cart_value, avg_products_in_cart,
        avg_session_duration, mobile_count, desktop_count, tablet_count,
        registered_user_carts, guest_user_carts, LoadDate
    )
    SELECT
        abandonment_category,
        COUNT(*) AS total_abandoned_carts,
        CAST(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0) AS DECIMAL(5,2)) AS percentage_of_abandonments,
        SUM(potential_revenue) AS total_lost_revenue,
        AVG(potential_revenue) AS avg_lost_cart_value,
        AVG(CAST(total_products AS DECIMAL(10,2))) AS avg_products_in_cart,
        AVG(CAST(session_duration_minutes AS DECIMAL(10,2))) AS avg_session_duration,
        SUM(CASE WHEN device_type = 'Mobile' THEN 1 ELSE 0 END) AS mobile_count,
        SUM(CASE WHEN device_type = 'Desktop' THEN 1 ELSE 0 END) AS desktop_count,
        SUM(CASE WHEN device_type = 'Tablet' THEN 1 ELSE 0 END) AS tablet_count,
        SUM(CASE WHEN is_registered_user = 1 THEN 1 ELSE 0 END) AS registered_user_carts,
        SUM(CASE WHEN is_registered_user = 0 THEN 1 ELSE 0 END) AS guest_user_carts,
        SYSUTCDATETIME() AS LoadDate
    FROM dbo.CartData_Cleaned
    WHERE abandoned = 1
      AND abandonment_category IS NOT NULL
      AND has_valid_data = 1
    GROUP BY abandonment_category;

    SET @abandonment_rows = @@ROWCOUNT;

    INSERT INTO dbo.pipeline_run_log (
        cleaned_rows, daily_summary_rows, category_rows,
        abandonment_rows, status, message
    )
    VALUES (
        @cleaned_rows, @daily_rows, @category_rows,
        @abandonment_rows, 'REFRESH_COMPLETED',
        'Analytics tables rebuilt from main_cart_events.'
    );
END;
GO

/*
   7. TEST COMMANDS
   Run these manually after ADF pipeline finishes. */

-- EXEC dbo.sp_upsert_main_cart_event;
-- EXEC dbo.sp_refresh_all_tables;

-- SELECT COUNT(*) AS staging_rows FROM dbo.cart_events;
-- SELECT COUNT(*) AS main_rows FROM dbo.main_cart_events;
-- SELECT COUNT(*) AS cleaned_rows FROM dbo.CartData_Cleaned;
-- SELECT COUNT(*) AS daily_rows FROM dbo.Daily_Summary;
-- SELECT COUNT(*) AS category_rows FROM dbo.Category_Performance;
-- SELECT COUNT(*) AS abandonment_rows FROM dbo.Abandonment_Analysis;
-- SELECT TOP 20 * FROM dbo.pipeline_run_log ORDER BY run_id DESC;
