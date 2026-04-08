CREATE TABLE dbo.main_cart_events (
    cart_id INT,
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
    event_timestamp DATETIME2 NOT NULL,
    abandoned BIT NOT NULL,
    abandonment_reason NVARCHAR(255) NULL,
    cart_size_category NVARCHAR(50),
    primary_category NVARCHAR(100),
    revenue DECIMAL(10,2),
    potential_revenue DECIMAL(10,2),
    -- ✅ Composite Primary Key (IMPORTANT)
    CONSTRAINT PK_main_cart_event 
    PRIMARY KEY (cart_id, event_timestamp)
);
go;

CREATE PROCEDURE sp_upsert_main_cart_event
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Remove duplicates from temp table
    WITH DedupSource AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY cart_id
                   ORDER BY event_timestamp DESC
               ) AS rn
        FROM dbo.cart_events
    )

    MERGE dbo.main_cart_events AS Target
    USING (
        SELECT * FROM DedupSource WHERE rn = 1
    ) AS Source
    ON Target.cart_id = Source.cart_id

    -- ✅ UPDATE only if newer timestamp
    WHEN MATCHED 
         AND Source.event_timestamp > Target.event_timestamp
    THEN UPDATE SET
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
        Target.potential_revenue = Source.potential_revenue

    -- ✅ INSERT if new cart_id
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (
        cart_id, user_id, total_products, total_quantity,
        cart_value, product_count, avg_product_price,
        user_name, user_age, user_gender,
        device_type, session_duration_minutes,
        event_timestamp, abandoned, abandonment_reason,
        cart_size_category, primary_category,
        revenue, potential_revenue
    )
    VALUES (
        Source.cart_id, Source.user_id, Source.total_products, Source.total_quantity,
        Source.cart_value, Source.product_count, Source.avg_product_price,
        Source.user_name, Source.user_age, Source.user_gender,
        Source.device_type, Source.session_duration_minutes,
        Source.event_timestamp, Source.abandoned, Source.abandonment_reason,
        Source.cart_size_category, Source.primary_category,
        Source.revenue, Source.potential_revenue
    );

END;