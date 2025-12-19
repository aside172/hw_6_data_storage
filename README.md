# 1. Запуск Trino
```bash
docker run --name trino -d -p 8080:8080 trinodb/trino
```
# 2. Создание таблиц DDS
```bash
docker exec -i trino trino < 01_dds_create.sql
```

# 3. Проверка созданных таблиц
```bash
docker exec -it trino trino --execute "show tables from memory.dds;"
```

# 4. Полная загрузка данных
```bash
docker exec -i trino trino < 02_dds_full_load.sql
```

# 5. Инкрементальная загрузка (ежедневная)
# Дать права на выполнение скрипта
```bash
chmod +x 03_dds_incremental_load.sh
```

# Загрузка за конкретный день:
```bash
./03_dds_incremental_load.sh 1997-10-10
```

# Загрузка за сегодня (без параметров):
```bash
./03_dds_incremental_load.sh
```