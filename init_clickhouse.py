import clickhouse_connect


def init_clickhouse_architecture():
    # Подключение к ClickHouse
    client = clickhouse_connect.get_client(
        host='localhost', port=8123, username='default')

    client.command("CREATE DATABASE IF NOT EXISTS raw")

    client.command("""
    CREATE TABLE IF NOT EXISTS raw.stg_stores (
        data String
    ) ENGINE = MergeTree() ORDER BY tuple();
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS raw.stg_products (
        data String
    ) ENGINE = MergeTree() ORDER BY tuple();
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS raw.stg_customers (
        data String
    ) ENGINE = MergeTree() ORDER BY tuple();
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS raw.stg_purchases (
        data String
    ) ENGINE = MergeTree() ORDER BY tuple();
    """)

    # Таблица Магазинов
    client.command("""
    CREATE TABLE IF NOT EXISTS raw.stores (
        store_id String,
        store_name String,
        store_network String,
        store_type_description String,
        city String,
        street String,
        manager_name String,
        manager_phone String,
        last_inventory_date Date
    ) ENGINE = ReplacingMergeTree()
    PRIMARY KEY store_id ORDER BY store_id;
    """)

    # Таблица Товаров
    client.command("""
    CREATE TABLE IF NOT EXISTS raw.products (
        product_id String,
        name String,
        category String,
        price Float32,
        calories Float32,
        barcode String,
        manufacturer_inn String
    ) ENGINE = ReplacingMergeTree()
    PRIMARY KEY product_id ORDER BY product_id;
    """)

    # Таблица Покупателей
    client.command("""
    CREATE TABLE IF NOT EXISTS raw.customers (
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
    """)

    # Таблица Покупок (Денормализованная по товарам в чеке)
    client.command("""
    CREATE TABLE IF NOT EXISTS raw.purchases (
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
    """)

    # MV для Магазинов
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_stores TO raw.stores AS
    SELECT
        JSONExtractString(data, 'store_id') AS store_id,
        JSONExtractString(data, 'store_name') AS store_name,
        JSONExtractString(data, 'store_network') AS store_network,
        JSONExtractString(data, 'store_type_description') AS store_type_description,
        JSONExtractString(data, 'location', 'city') AS city,
        JSONExtractString(data, 'location', 'street') AS street,
        JSONExtractString(data, 'manager', 'name') AS manager_name,
        JSONExtractString(data, 'manager', 'phone') AS manager_phone,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'last_inventory_date')) AS last_inventory_date
    FROM raw.stg_stores;
    """)

    # MV для Товаров
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_products TO raw.products AS
    SELECT
        JSONExtractString(data, 'id') AS product_id,
        JSONExtractString(data, 'name') AS name,
        JSONExtractString(data, 'group') AS category,
        JSONExtractFloat(data, 'price') AS price,
        JSONExtractFloat(data, 'kbju', 'calories') AS calories,
        JSONExtractString(data, 'barcode') AS barcode,
        JSONExtractString(data, 'manufacturer', 'inn') AS manufacturer_inn
    FROM raw.stg_products;
    """)

    # MV для Покупателей
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_customers TO raw.customers AS
    SELECT
        JSONExtractString(data, 'customer_id') AS customer_id,
        JSONExtractString(data, 'first_name') AS first_name,
        JSONExtractString(data, 'last_name') AS last_name,
        JSONExtractString(data, 'email') AS email,
        JSONExtractString(data, 'phone') AS phone,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'birth_date')) AS birth_date,
        JSONExtractString(data, 'gender') AS gender,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'registration_date')) AS registration_date,
        JSONExtractString(data, 'preferences', 'preferred_payment_method') AS payment_method
    FROM raw.stg_customers;
    """)

    # MV для Покупок (Разворачиваем массив items с помощью arrayJoin)
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_purchases TO raw.purchases AS
    WITH
        JSONExtractArrayRaw(data, 'items') AS raw_items
    SELECT
        JSONExtractString(data, 'purchase_id') AS purchase_id,
        JSONExtractString(data, 'customer', 'customer_id') AS customer_id,
        JSONExtractString(data, 'store', 'store_id')AS store_id,

        JSONExtractString(arrayJoin(raw_items), 'product_id') AS product_id,
        toInt16OrZero(JSONExtractString(arrayJoin(raw_items), 'quantity')) AS quantity,
        JSONExtractFloat(arrayJoin(raw_items), 'price_per_unit') AS price_per_unit,
        JSONExtractFloat(arrayJoin(raw_items), 'total_price') AS total_item_price,

        JSONExtractFloat(data, 'total_amount') AS total_order_amount,
        JSONExtractString(data, 'payment_method') AS payment_method,
        JSONExtractBool(data, 'is_delivery') AS is_delivery,
        parseDateTimeBestEffortOrZero(JSONExtractString(data, 'purchase_datetime')) AS purchase_datetime
    FROM raw.stg_purchases;
    """)


if __name__ == "__main__":
    init_clickhouse_architecture()
