import json
import time
import re
import hashlib
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
import clickhouse_connect


COLLECTIONS_MAP = {
    "customers": "raw.stg_customers",
    "products": "raw.stg_products",
    "purchases": "raw.stg_purchases",
    "stores": "raw.stg_stores"
}
TOPICS = list(COLLECTIONS_MAP.keys())


def normal_hash_phone(phone: str) -> str:
    """Приводит телефон к формату 7XXXXXXXXXX и хэширует."""
    if not phone:
        return ''
    digits = re.sub(r'\D', '', str(phone))

    if len(digits) == 11:
        if digits.startswith('8') or digits.startswith('7'):
            digits = '7' + digits[1:]

    return hashlib.sha256(digits.encode('utf-8')).hexdigest()


def normal_email(email: str) -> str:
    """Приводит email к нижнему регистру, убирает пробелы и хэширует."""
    if not email:
        return ''
    clean_email = str(email).strip().lower()

    return hashlib.sha256(clean_email.encode('utf-8')).hexdigest()


def run_producer():
    client = MongoClient("mongodb://admin:secret_password@localhost:27017/")
    db = client["my_database"]

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

    for col_name in TOPICS:
        collection = db[col_name]
        documents = list(collection.find({}))
        print(f"Считано {len(documents)} документов из Mongo [{col_name}]")

        for doc in documents:
            # Удаляем внутренний ObjectId MongoDB, так как он мешает стандартному JSON
            if '_id' in doc:
                doc.pop('_id')

            if col_name == "customers":
                if "phone" in doc:
                    doc["phone"] = normal_hash_phone(doc["phone"])
                if "email" in doc:
                    doc["email"] = normal_email(doc["email"])

            elif col_name == "stores" and "manager" in doc:
                if isinstance(doc["manager"], dict):
                    if "phone" in doc["manager"]:
                        doc["manager"]["phone"] = normal_hash_phone(doc["manager"]["phone"])
                    if "email" in doc["manager"]:
                        doc["manager"]["email"] = normal_email(doc["manager"]["email"])

            producer.send(topic=col_name, value=doc)

    producer.flush()
    client.close()


def run_consumer():
    ch_client = clickhouse_connect.get_client(host='localhost', port=8123)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='clickhouse-json-loader-group'
    )

    batch_data = {topic: [] for topic in TOPICS}
    last_flush_time = time.time()

    print("Ожидание стриминга сообщений из Kafka(нажмите Ctrl+C для выхода)")
    try:
        for message in consumer:
            topic = message.topic
            # Декодируем байты из Kafka обратно в строку (сырой JSON)
            raw_json_string = message.value.decode('utf-8')

            # Добавляем в буфер как кортеж с одним элементом, так как в stg_* таблицах всего одна колонка `data`
            batch_data[topic].append((raw_json_string,))

            # Условие сброса пакета: накопилось 200 записей суммарно или прошло более 3 секунд
            total_buffered = sum(len(v) for v in batch_data.values())
            if total_buffered >= 200 or (time.time() - last_flush_time) > 3:
                for t, rows in batch_data.items():
                    if rows:
                        ch_table = COLLECTIONS_MAP[t]
                        # Вставляем данные пакетно в колонку `data`
                        ch_client.insert(ch_table, rows, column_names=['data'])
                        print(f"Записан пакет {len(rows)} строк в [{ch_table}]")
                        batch_data[t] = [] # Очищаем буфер для этого топика

                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("Стриминг остановлен пользователем.")
    finally:
        consumer.close()


if __name__ == "__main__":
    # Вызываем последовательно: сначала наполняем брокер, затем стримим в базу
    run_producer()
    run_consumer()
