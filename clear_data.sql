CREATE DATABASE IF NOT EXISTS clean_dwh;

CREATE TABLE IF NOT EXISTS clean_dwh.etl_log (
    table_name LowCardinality(String),
    check_time DateTime,
    total_rows UInt64,
    duplicate_rows UInt64,
    null_or_empty_rows UInt64,
    invalid_date_rows UInt64
) ENGINE = MergeTree()
ORDER BY (table_name, check_time);

-- Магазины
CREATE TABLE IF NOT EXISTS clean_dwh.stores (
    store_id String,
    store_name String,
    store_network LowCardinality(String),
    store_type_description String,
    city LowCardinality(String),
    street String,
    manager_name String,
    manager_phone String,
    last_inventory_date Date
) ENGINE = ReplacingMergeTree()
PRIMARY KEY store_id ORDER BY store_id;

-- Товары
CREATE TABLE IF NOT EXISTS clean_dwh.products (
    product_id String,
    name String,
    category LowCardinality(String),
    price Float32,
    calories Float32,
    barcode String,
    manufacturer_inn String
) ENGINE = ReplacingMergeTree()
PRIMARY KEY product_id ORDER BY product_id;

-- Покупатели
CREATE TABLE IF NOT EXISTS clean_dwh.customers (
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone String,
    birth_date Date,
    gender LowCardinality(String),
    registration_date DateTime,
    payment_method LowCardinality(String)
) ENGINE = ReplacingMergeTree()
PRIMARY KEY customer_id ORDER BY customer_id;

-- Покупки (Таблица фактов)
CREATE TABLE IF NOT EXISTS clean_dwh.purchases (
    purchase_id String,
    customer_id String,
    store_id String,
    product_id String,
    quantity UInt16,
    price_per_unit Float32,
    total_item_price Float32,
    total_order_amount Float32,
    payment_method LowCardinality(String),
    is_delivery UInt8,
    purchase_datetime DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(purchase_datetime)
ORDER BY (store_id, customer_id, purchase_datetime);

-- 1. Стриминг-очистка для Магазинов
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_etl_stores TO clean_dwh.stores AS
SELECT 
    store_id,
    lower(trim(store_name)) AS store_name,
    lower(trim(store_network)) AS store_network,
    lower(trim(store_type_description)) AS store_type_description,
    lower(trim(city)) AS city,
    lower(trim(street)) AS street,
    lower(trim(manager_name)) AS manager_name,
    manager_phone, -- Телефон нормализован и захеширован на продюсере
    last_inventory_date
FROM raw.stores
WHERE store_id != '' 
  AND isNotNull(store_name) AND trim(store_name) != ''
  AND last_inventory_date <= today();

-- 2. Стриминг-очистка для Товаров
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_etl_products TO clean_dwh.products AS
SELECT 
    product_id,
    lower(trim(name)) AS name,
    lower(trim(category)) AS category,
    price,
    calories,
    barcode,
    manufacturer_inn
FROM raw.products
WHERE product_id != '' 
  AND isNotNull(name) AND trim(name) != ''
  AND price > 0;


-- 3. Стриминг-очистка для Покупателей
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_etl_customers TO clean_dwh.customers AS
SELECT 
    customer_id,
    lower(trim(first_name)) AS first_name,
    lower(trim(last_name)) AS last_name,
    lower(trim(email)) AS email,
    phone, -- Почта и телефон захешированы
    birth_date,
    lower(gender) AS gender,
    registration_date,
    lower(payment_method) AS payment_method
FROM raw.customers
WHERE customer_id != ''
  AND isNotNull(first_name) AND trim(first_name) != ''
  AND birth_date <= today()
  -- Проверка адекватности: возраст не может быть менее 0 лет и более 120 лет
  AND birth_date >= (today() - INTERVAL 120 YEAR)
  AND registration_date <= (now() + INTERVAL 5 HOUR);

-- 4. Стриминг-очистка для Покупок
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_etl_purchases TO clean_dwh.purchases AS
SELECT 
    purchase_id,
    customer_id,
    store_id,
    product_id,
    quantity,
    price_per_unit,
    total_item_price,
    total_order_amount,
    lower(payment_method) AS payment_method,
    is_delivery,
    purchase_datetime
FROM raw.purchases
WHERE purchase_id != ''
  AND customer_id != ''
  AND store_id != ''
  AND product_id != ''
  AND quantity > 0
  AND purchase_datetime <= now();

-- Мониторинг Магазинов
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_monitor_stores TO clean_dwh.etl_log AS
SELECT
	'stores' AS table_name,
	now() AS check_time, 
	count() AS total_rows,
    count() - uniqExact(store_id) AS duplicate_rows,
    countIf(store_id = '' OR store_name = '') AS null_or_empty_rows,
    countIf(last_inventory_date > today()) AS invalid_date_rows
FROM raw.stores;

-- Мониторинг Товаров
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_monitor_products TO clean_dwh.etl_log AS
SELECT 'products' AS table_name, now() AS check_time, count() AS total_rows,
    count() - uniqExact(product_id) AS duplicate_rows,
    countIf(product_id = '' OR name = '') AS null_or_empty_rows,
    0 AS invalid_date_rows
FROM raw.products;

-- Мониторинг Покупателей
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_monitor_customers TO clean_dwh.etl_log AS
SELECT 'customers' AS table_name, now() AS check_time, count() AS total_rows,
    count() - uniqExact(customer_id) AS duplicate_rows,
    countIf(customer_id = '' OR first_name = '') AS null_or_empty_rows,
    countIf(birth_date > today() OR registration_date > now()) AS invalid_date_rows
FROM raw.customers;

-- Мониторинг Покупок
CREATE MATERIALIZED VIEW IF NOT EXISTS clean_dwh.mv_monitor_purchases TO clean_dwh.etl_log AS
SELECT 'purchases' AS table_name, now() AS check_time, count() AS total_rows,
    count() - uniqExact(purchase_id, product_id) AS duplicate_rows,
    countIf(purchase_id = '' OR customer_id = '' OR product_id = '') AS null_or_empty_rows,
    countIf(purchase_datetime > now()) AS invalid_date_rows
FROM raw.purchases;


SELECT 
    check_time AS time,
    table_name,
    (duplicate_rows / total_rows) * 100 AS duplicate_percentage
FROM clean_dwh.etl_log
WHERE total_rows > 0 
  -- Берем только самые свежие записи для каждой таблицы за последние 5 минут
  AND check_time >= now() - INTERVAL 5 MINUTE
ORDER BY check_time DESC;