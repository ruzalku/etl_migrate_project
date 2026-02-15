# ETL-process to migrate DB

## Запуск
### На своем хосте:
```bash
python3 main.py
```

### Через docker:
Сборка образа:
```bash
docker build -t etl:1.0 .
```
Запуск docker-контейнера:
```bash
docker run etl:1.0
```


## Настройка mapping.json

Файл mapping.json должен лежать в корне проекта.

Пример mapping.json лежит в каталоге examples.

mapping.json имеет такую структуру:
```json
{
    "table_name": {
        "old_table_name": "table name",
        "fields": {
            "column_name": {
                "options": {
                    "$op": ...
                }
            }
        },
        "options": {
            "$op": ...
        }
    }
}
```

Маппинг состоит из словаря с названиями таблиц, в которые мигрируют данные ("table_name").

Каждая таблица состоит из полей options, old_table_name, fields.

- *options* - указываются параметры фильтрации данных по полям

- *old_table_name* - старое название таблицы (название таблицы источника данных)

- *fields* - названия новых полей с настройками этих данных

---

### Options для таблиц:
- ```$and: [...]``` - оператор "и", указываются в массиве другие операторы

- ```$or [...]``` - оператор "или", указываются в массиве другие операторы

- *Оператор больше или равно:*
```
$ge: {
    "column_name": "name",
    "value": value
}
```

- *Оператор больше:*
```
$gt: {
    "column_name": "name",
    "value": value
}
```

- *Оператор меньше или равно:*
```
$le: {
    "column_name": "name",
    "value": value
}
```

- *Оператор меньше:*
```
$lt: {
    "column_name": "name",
    "value": value
}
```

- *Оператор равенства:*
```
$equal: {
    "column_name": "name",
    "value": value
}
```

---
### Options для колонок
- *Оператор минимум:*
```
"$min": ["column1", "column2", ...]
```

- *Оператор максимум:*
```
"$max": ["column1", "column2", ...]
```

- *Оператор перевода в словарь данных:*
```
"$to_json": ["column1", "column2", ...]
```

- *Оператор копирования (просто копирует данные в нужную колонку):*
```
"$copy": "column1"
```

## Использование:

Пока не реализована более понятная конфигурация (см. TODO.md), она настраивается в файле main.py.

- *Переменная configs:*

    - *Настройка для extractor:*
        В ```config``` указать данные для подключения к БД.

        - ```"update_row": "created_at"``` - Колонка для Timestamp CDC
        - ```"pk_col": "ctid"``` - PK для столбца (по умолчанию стоит для postgresql системный)
        - ```"cdc": True``` - Включение/выключени CDC
        - ```"cdc_mode": Mode.TIMESTAMP``` - Режим работы CDC

    - *Настройка для loader:*
        В ```config``` указать данные для подключения к БД.

        - ```"db_name": "db name"``` - Название БД

    - *Настройка для transformer:*
        ```config``` - указать пустым (то есть '{}')

---

- *Переменные в run_pipeline*
    - ```loader_module="src.loaders.mongodb"``` - Путь до модуля loader, где содержится нужный плагин
    - ```extractor_module="src.extractors.postgresql"``` - Путь до модуля extractor, где содержится нужный плагин
    - ```transform_module="src.transaformers.default"``` - Путь до модуля transformer, где содержится нужный плагин
    - ```configs=configs``` - Тут указывается конфиг для БД
    - ```mapping_path="mapping.json"``` - Путь до mapping-а
