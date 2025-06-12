# NYC Taxi Trip Data Analysis with PySpark and HDFS

## Описание проекта
Проект загружает данные о поездках такси Нью-Йорка за 2019-2021 годы, обрабатывает их с помощью PySpark и сохраняет в HDFS с партиционированием по дате.

## Структура проекта
```
nyc-taxi-hdfs/
├── notebooks/
│ └── kt4_solution.ipynb # Jupyter Notebook с решением
├── README.md # Инструкция 
└── requirements.txt # Зависимости Python
```

## Установка и запуск

### 1. Установка Docker контейнера

```bash
# Клонируем репозиторий
git clone https://github.com/jupyterhub/jupyterhub-on-hadoop
cd jupyterhub-on-hadoop/docker-demo

# Запускаем контейнеры
docker-compose up -d
```
После запуска откройте Jupyter Notebook в браузере:
  - Адрес: http://localhost:8888
  - Логин: ```alice```
  - Пароль: ```testpass```

### 2. Настройка окружения
Перед запуском ноутбука установите зависимости:
```pip install -r requirements.txt```
Содержимое ```requirements.txt```:
```
pyspark>=3.0.0
jupyter>=1.0.0
pandas>=1.0.0
```
### 3. Запуск решения
 - Откройте ```notebooks/kt4_solution.ipynb``` в Jupyter
 - Выполните все ячейки последовательно

## Этапы работы
1. Загрузка данных
   - Данные скачиваются с NYC Open Data:
       - 2019: https://data.cityofnewyork.us/api/views/2upf-qytp/rows.csv
       - 2020: https://data.cityofnewyork.us/api/views/kxp8-n2sj/rows.csv
       - 2021: https://data.cityofnewyork.us/api/views/m6nq-qud6/rows.csv
2. Обработка данных
  - Преобразование форматов даты
  - Фильтрация по году
  - Очистка и нормализация данных
3. Сохранение в HDFS
Данные сохраняются в формате Parquet с партиционированием по дате:
```
hdfs://master:9000/user/[username]/data/yellow_taxi/
├── 2019/
├── 2020/
└── 2021/
```
4. Проверка результатов
Выводится информация о количестве записей:
```
2019: [количество] записей
2020: [количество] записей
2021: [количество] записей
```

## Результаты
После успешного выполнения:
  - Данные сохранены в HDFS в Parquet-формате
  - Созданы партиции по датам
  - Выведена статистика по количеству записей

