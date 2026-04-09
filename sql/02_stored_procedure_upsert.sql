CREATE TABLE dbo.main_cart_events (
    cart_id INT PRIMARY KEY,
    user_id INT,
    total_products INT,
    total_quantity INT,
    cart_value DECIMAL(10,2),
    product_count INT,
    avg_product_price DECIMAL(10,2),
    user_name NVARCHAR(100),
    user_age FLOAT NULL,
    user_gender NVARCHAR(10) NULL,
    device_type NVARCHAR(20),
    session_duration_minutes INT,
    event_timestamp DATETIME2 NULL,
    abandoned BIT NULL,
    abandonment_reason NVARCHAR(255) NULL,
    cart_size_category NVARCHAR(50),
    primary_category NVARCHAR(100),
    revenue DECIMAL(10,2),
    potential_revenue DECIMAL(10,2)
);

CREATE PROCEDURE sp_upsert_main_cart_event
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Remove duplicates from source (keep 1 row per cart_id)
    WITH DedupSource AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cart_id
                   ORDER BY (SELECT NULL)
               ) AS rn
        FROM dbo.cart_events
    )

    -- Step 2: Insert ONLY new cart_id (not already in main table)
    INSERT INTO dbo.main_cart_events (
        cart_id, user_id, total_products, total_quantity,
        cart_value, product_count, avg_product_price,
        user_name, user_age, user_gender,
        device_type, session_duration_minutes,
        event_timestamp, abandoned, abandonment_reason,
        cart_size_category, primary_category,
        revenue, potential_revenue
    )
    SELECT 
        Source.cart_id, Source.user_id, Source.total_products, Source.total_quantity,
        Source.cart_value, Source.product_count, Source.avg_product_price,
        Source.user_name, Source.user_age, Source.user_gender,
        Source.device_type, Source.session_duration_minutes,
        Source.event_timestamp, Source.abandoned, Source.abandonment_reason,
        Source.cart_size_category, Source.primary_category,
        Source.revenue, Source.potential_revenue
    FROM DedupSource AS Source
    WHERE Source.rn = 1   -- remove duplicates in source
      AND NOT EXISTS (
          SELECT 1 
          FROM dbo.main_cart_events Target
          WHERE Target.cart_id = Source.cart_id
      );
END;
