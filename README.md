# PostgreSQL CDC → Debezium → Kafka → Consumers

Практическая работа по настройке Change Data Capture (CDC): изменения строк в PostgreSQL считываются Debezium из WAL, передаются через Kafka Connect в Kafka-топики и обрабатываются Python-консьюмерами.

Проект построен вокруг существующего трёхузлового Kafka KRaft-кластера. Kafka, PostgreSQL, Kafka Connect, Prometheus, Grafana и приложения-консьюмеры запускаются в общей Docker-сети `kafka-internal`.

## Содержание


- [Состав проекта](#состав-проекта)
- [Требования](#требования)
- [Быстрый запуск](#быстрый-запуск)
- [Настройка Debezium](#настройка-debezium)
- [Проверка CDC](#проверка-cdc)
- [Работа консьюмеров](#работа-консьюмеров)
- [Мониторинг](#мониторинг)
- [Полезные команды](#полезные-команды)
- [Остановка и очистка](#остановка-и-очистка)

## Состав проекта

```text
.
├── docker-compose-kafka.yml                  # Kafka KRaft cluster + Kafka UI
├── docker-compose-cdc.yml                    # PostgreSQL, Kafka Connect, Prometheus, Grafana
├── docker-compose-app.yml                    # Python consumers
├── debezium-postgres-connector.json          # Конфигурация Debezium PostgreSQL Connector
├── consumer.py                                # Базовый Kafka consumer
├── consumer_analytics.py                      # Consumer со счётчиками событий
├── requirements.txt                           # Python dependencies
├── init-db/
│   └── 01_init.sql                            # Таблицы users/orders и тестовые данные
├── jmx_exporter/
│   ├── connect-jmx-config.yml                 # Правила экспорта JMX Kafka Connect
│   └── jmx_prometheus_javaagent.jar           # Prometheus JMX Java Agent
├── prometheus/
│   └── prometheus.yml                         # Prometheus scrape config
└── grafana/
    └── provisioning/
        ├── datasources/
        │   └── datasource.yml                 # Prometheus datasource
        └── dashboards/
            ├── dashboard.yml                  # Dashboard provisioning
            └── debezium-kafka-connect.json    # Grafana dashboard
```

## Требования

- Docker Engine 24+.
- Docker Compose v2.
- `curl` для регистрации и проверки Kafka Connect API.
- `jq` — опционально, для форматирования JSON в терминале.
- Свободные порты: `5432`, `8080`, `8083`, `9090`, `9092`, `9093`, `9094`, `9095`, `9081`, `9083`, `3000`.

Проверка окружения:

```bash
docker --version
docker compose version
curl --version
```

## Быстрый запуск

### 1. Клонирование

```bash
git clone https://github.com/beezzlot/yandex-kafka-practic2.git
cd yandex-kafka-practic2
```

### 2. Создание Docker-сети

Сеть объявлена как внешняя (`external: true`), поэтому создаётся вручную один раз:

```bash
docker network create kafka-internal
```

Если сеть уже есть, Docker вернёт ошибку. Проверить её наличие можно так:

```bash
docker network inspect kafka-internal
```

### 3. Запуск Kafka-кластера

```bash
docker compose -f docker-compose-kafka.yml up -d
```

Проверьте, что три брокера запущены:

```bash
docker compose -f docker-compose-kafka.yml ps
```

Kafka UI доступен по адресу: <http://localhost:8080>.

### 4. Запуск CDC и мониторинга

```bash
docker compose -f docker-compose-cdc.yml up -d
```

Дождитесь запуска PostgreSQL и Kafka Connect:

```bash
docker compose -f docker-compose-cdc.yml ps
docker logs -f connect
```

REST API Kafka Connect будет доступен по адресу: <http://localhost:8083>.

### 5. Регистрация Debezium Connector

```bash
curl -i -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium-postgres-connector.json
```

Проверить список зарегистрированных коннекторов:

```bash
curl -s http://localhost:8083/connectors | jq .
```

### 6. Запуск приложений-консьюмеров

```bash
docker compose -f docker-compose-app.yml up -d
```

Проверка состояния:

```bash
docker compose -f docker-compose-app.yml ps
```

## Настройка Debezium

Конфигурация расположена в файле `debezium-postgres-connector.json`.

| Параметр | Значение | Назначение |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Используемый источник Kafka Connect |
| `database.hostname` | `postgres` | DNS-имя PostgreSQL в Docker-сети |
| `database.port` | `5432` | Порт PostgreSQL |
| `database.dbname` | `postgres` | Отслеживаемая база данных |
| `plugin.name` | `pgoutput` | Плагин PostgreSQL logical replication |
| `table.include.list` | `public.users,public.orders` | Белый список отслеживаемых таблиц |
| `slot.name` | `debezium_slot` | Logical replication slot |
| `publication.name` | `dbz_publication` | PostgreSQL publication для `pgoutput` |
| `database.server.name` | `dbserver1` | Префикс имён Kafka-топиков |
| `transforms.unwrap.type` | `ExtractNewRecordState` | Извлечение актуального состояния строки из Debezium envelope |

Ключевой параметр работы — `table.include.list`. Он ограничивает CDC только таблицами `public.users` и `public.orders`; изменения в других таблицах не публикуются.

После запуска коннектора Debezium создаёт топики:

```text
dbserver1.public.users
dbserver1.public.orders
```

## Проверка CDC

### Статус Kafka Connect

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status | jq .
```

Нормальное состояние:

```json
{
  "name": "postgres-connector",
  "connector": {
    "state": "RUNNING"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING"
    }
  ]
}
```

### Проверка таблиц

```bash
docker exec -it postgres psql -U postgres -d postgres -c '\dt'
```

Проверка исходных тестовых данных:

```bash
docker exec -it postgres psql -U postgres -d postgres -c 'SELECT * FROM users;'
docker exec -it postgres psql -U postgres -d postgres -c 'SELECT * FROM orders;'
```

### Проверка топиков

```bash
docker exec -it kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 \
  --list
```

Ожидаемые служебные топики Kafka Connect:

```text
connect_configs
connect_offsets
connect_statuses
```

Ожидаемые CDC-топики:

```text
dbserver1.public.users
dbserver1.public.orders
```

### Тест INSERT

Откройте логи базового консьюмера:

```bash
docker logs -f consumer
```

В отдельном терминале добавьте пользователя:

```bash
docker exec -it postgres psql -U postgres -d postgres -c \
  "INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');"
```

В логах `consumer` появится JSON-сообщение из `dbserver1.public.users`.

### Тест INSERT в orders

```bash
docker exec -it postgres psql -U postgres -d postgres -c \
  "INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Mechanical Keyboard', 1);"
```

Ожидаемое событие будет опубликовано в `dbserver1.public.orders`.

### Тест UPDATE

```bash
docker exec -it postgres psql -U postgres -d postgres -c \
  "UPDATE users SET email = 'john.doe.updated@example.com' WHERE id = 1;"
```

### Тест DELETE

```bash
docker exec -it postgres psql -U postgres -d postgres -c \
  "DELETE FROM orders WHERE id = 1;"
```

## Работа консьюмеров

В `docker-compose-app.yml` определены консьюмеры.

| Сервис | Скрипт | Consumer group | Назначение |
|---|---|---|---|
| `consumer` | `consumer.py` | `demo-consumer-group` | Выводит каждое Kafka-событие в stdout в JSON-формате 

### Просмотр логов

```bash
docker logs -f consumer
```

### Перезапуск с чтением с начала

Kafka хранит offsets отдельно для каждой consumer group. Чтобы прочитать доступную историю заново, остановите сервис и удалите consumer group:

```bash
docker compose -f docker-compose-app.yml stop consumer

docker exec -it kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --delete \
  --group demo-consumer-group

docker compose -f docker-compose-app.yml up -d consumer
```

## Мониторинг

### Prometheus

Prometheus собирает метрики с JMX Exporter Kafka Connect по адресу `connect:8080/metrics`.

Откройте <http://localhost:9090/targets>. Цель `kafka-connect` должна иметь состояние `UP`.

Примеры запросов PromQL:

```promql
up{job="kafka-connect"}
```

```promql
kafka_connect_worker_connector_count
```

```promql
kafka_connect_worker_task_count
```

```promql
kafka_connect_connector_running_tasks
```

```promql
kafka_connect_connector_failed_tasks
```

### Grafana

Grafana доступна по адресу <http://localhost:3000>.

```text
login:    admin
password: admin
```

Datasource Prometheus и дашборд `Debezium & Kafka Connect Metrics` создаются автоматически через provisioning. На дашборде отображаются:

- Количество коннекторов Kafka Connect
- Количество задач Kafka Connect
- Число работающих задач (`running tasks`)
- Число упавших задач (`failed tasks`)

## Полезные команды

### Состояние контейнеров

```bash
docker ps
docker compose -f docker-compose-kafka.yml ps
docker compose -f docker-compose-cdc.yml ps
docker compose -f docker-compose-app.yml ps
```

### Логи сервисов

```bash
docker logs -f kafka1
docker logs -f postgres
docker logs -f connect
docker logs -f prometheus
docker logs -f grafana
docker logs -f consumer
```

### Информация о коннекторе

```bash
curl -s http://localhost:8083/connectors/postgres-connector | jq .
curl -s http://localhost:8083/connectors/postgres-connector/config | jq .
curl -s http://localhost:8083/connectors/postgres-connector/status | jq .
```

### Удаление коннектора

```bash
curl -i -X DELETE http://localhost:8083/connectors/postgres-connector
```

### Повторная регистрация

```bash
curl -i -X DELETE http://localhost:8083/connectors/postgres-connector

curl -i -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @debezium-postgres-connector.json
```

### Ручное чтение топика без Python

```bash
docker exec -it kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:9092 \
  --topic dbserver1.public.users \
  --from-beginning \
  --property print.key=true \
  --property key.separator=' | '
```

### Проверка consumer groups

```bash
docker exec -it kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --list


docker exec -it kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --describe \
  --group demo-consumer-group
```

## Остановка и очистка

Остановить все сервисы:

```bash
docker compose -f docker-compose-app.yml down
docker compose -f docker-compose-cdc.yml down
docker compose -f docker-compose-kafka.yml down
```

Остановить и удалить данные PostgreSQL, Kafka, Prometheus и Grafana:

```bash
docker compose -f docker-compose-app.yml down -v
docker compose -f docker-compose-cdc.yml down -v
docker compose -f docker-compose-kafka.yml down -v
```