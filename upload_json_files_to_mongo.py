import json
from pathlib import Path
from pymongo import MongoClient


BASE_DIR = Path("./data")
EXPECTED_FOLDERS = ["customers", "products", "purchases", "stores"]


def upload_data():
    client = MongoClient("mongodb://admin:secret_password@localhost:27017/")
    db = client["my_database"]

    if not BASE_DIR.exists():
        print(f"Ошибка корневая папка '{BASE_DIR}' не найдена")
        return

    for folder in EXPECTED_FOLDERS:
        folder_path = BASE_DIR / folder

        if not folder_path.exists():
            print(f"Подпапка {folder} отсутсвует")
            continue

        collection = db[folder]
        json_files = list(folder_path.glob("*.json"))

        if not json_files:
            print(f"Папка {folder}: json файлы не найдены")
            continue

        for file_path in json_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Приводим к списку документов
                if isinstance(data, dict):
                    documents = [data]
                elif isinstance(data, list):
                    documents = data
                else:
                    print(f"Пропуск {file_path.name}: неподдерживаемый формат JSON.")
                    continue

                if documents:
                    result = collection.insert_many(documents)
                    print(f"Файл {file_path.name} -> Загружено документов: {len(result.inserted_ids)}")
            except json.JSONDecodeError:
                print(f"Ошибка файл {file_path.name} содержит битый JSON")
            except Exception as e:
                print(f"Ошибка при обработке {file_path.name}: {e}")

    print("Все папки успешно обработаны!")
    client.close()


if __name__ == "__main__":
    upload_data()
