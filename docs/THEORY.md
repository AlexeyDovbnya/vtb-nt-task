# Задание 2. Теория

## 2.1. Что влияет на производительность взаимодействия через Kafka

### Топик и брокер
| Параметр | На что влияет |
|---|---|
| **Число партиций** (`num.partitions`, `--partitions`) | Партиция — единица параллелизма. В одной группе потребителей одну партицию читает только один потребитель, поэтому потребителей больше, чем партиций, не бывает. В нашем стенде 12 партиций = 12 потоков заглушки. Слишком много партиций — больше файлов, дольше выбор лидера и ребалансировка. |
| **Replication factor** и `min.insync.replicas` | Надёжность против задержки: при `acks=all` продюсер ждёт подтверждения от всех ISR-реплик. |
| `num.network.threads`, `num.io.threads` | Потоки брокера для сети и для работы с диском. Узкое место при большом числе клиентов. |
| `log.segment.bytes`, `log.retention.*`, `cleanup.policy` | Размер сегментов, сколько храним, delete или compact. Влияют на диск и фоновую нагрузку. |
| `log.flush.interval.*` | Принудительный fsync. По умолчанию Kafka полагается на page cache ОС и репликацию, частый flush резко снижает пропускную способность. |
| `message.max.bytes` / `max.request.size` | Предельный размер сообщения/запроса. |
| `socket.send/receive.buffer.bytes` | TCP-буферы, важны на сетях с большой задержкой. |
| Железо и ОС | Диски (последовательная запись), page cache (свободная RAM), сеть, GC JVM брокера (heap обычно 4–6 ГБ, остальное — page cache). |

### Продюсер
| Параметр | На что влияет |
|---|---|
| `acks` (0 / 1 / all) | 0 — максимальная скорость, возможны потери; all — надёжно, но дольше. |
| `batch.size`, `linger.ms` | Пакетирование: продюсер ждёт до `linger.ms`, чтобы набрать пакет. Больше пакет — выше пропускная способность, выше задержка. |
| `compression.type` (none/gzip/snappy/lz4/zstd) | Меньше сеть и диск, больше CPU. |
| `buffer.memory`, `max.block.ms` | Буфер неотправленных сообщений. Переполнился — `send()` блокируется. |
| `max.in.flight.requests.per.connection`, `enable.idempotence`, `retries` | Параллельные запросы к брокеру, гарантия порядка и отсутствия дублей при ретраях. |
| Ключ сообщения и partitioner | Распределение по партициям. Перекос ключей = «горячая» партиция. |
| Сериализация | JSON медленнее и тяжелее Avro/Protobuf. |

### Потребитель
| Параметр | На что влияет |
|---|---|
| Число потребителей в группе (concurrency) | Не больше числа партиций, иначе лишние простаивают. |
| `max.poll.records` | Сколько записей отдаёт один `poll()`. Медленная обработка + много записей = риск превысить `max.poll.interval.ms`. |
| `max.poll.interval.ms` | Сколько можно не вызывать `poll()`; превышение — потребителя исключают из группы, начинается ребалансировка, возможны повторы. |
| `session.timeout.ms`, `heartbeat.interval.ms` | Как быстро обнаруживается «мёртвый» потребитель. |
| `fetch.min.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes` | Пакетирование на чтении: задержка против пропускной способности. |
| `enable.auto.commit` / стратегия коммита | Автокоммит может закоммитить необработанное — потеря при падении. Ручной коммит после обработки — at-least-once. |
| `partition.assignment.strategy` | `CooperativeStickyAssignor` — ребалансировка без остановки всей группы. |
| `auto.offset.reset` | С какого места читать новой группе. |
| Время обработки сообщения | Главное в нашей заглушке: 350 мс на сообщение = 2,86 сообщ/с на поток. |

**Ключевая метрика** — consumer lag (разница между последним оффсетом и закоммиченным). Растёт — потребители не успевают.

---

## 2.2. Что влияет на производительность PostgreSQL

### Память
- `shared_buffers` — кэш страниц самого Postgres (обычно ~25% RAM).
- `effective_cache_size` — подсказка планировщику, сколько кэша есть всего (shared_buffers + page cache ОС), ~50–75% RAM. Память не выделяет.
- `work_mem` — память на одну операцию сортировки/хеша **в каждом узле плана и в каждом процессе**. Мало — сортировки уходят на диск (temp-файлы), много — риск OOM при большом числе соединений.
- `maintenance_work_mem` — для VACUUM, CREATE INDEX.
- `huge_pages` — для больших shared_buffers.

### WAL и контрольные точки
- `wal_buffers`, `max_wal_size`, `min_wal_size`, `checkpoint_timeout`, `checkpoint_completion_target` — частота и «размазанность» checkpoint. Частые checkpoint = пики записи на диск и рост full page writes.
- `synchronous_commit` — ждать ли сброса WAL на диск при COMMIT. `off` ускоряет запись, рискуя последними транзакциями при сбое.
- `commit_delay` / `commit_siblings` — групповой коммит.
- `fsync`, `full_page_writes` — отключать нельзя на проде.

### Планировщик
- `random_page_cost` (для SSD ~1.1 вместо 4), `seq_page_cost`, `cpu_tuple_cost` и др. — влияют на выбор индекс/seq scan.
- `effective_io_concurrency` — параллельное чтение для bitmap heap scan.
- `default_statistics_target` и актуальная статистика (`ANALYZE`) — без неё оценки строк ошибаются, план плохой.
- `max_parallel_workers_per_gather`, `max_parallel_workers`, `max_worker_processes` — параллельные запросы.
- `jit` — на коротких OLTP-запросах может замедлять.

### Соединения
- `max_connections` — каждое соединение = отдельный процесс. Сотни соединений хуже, чем пул (PgBouncer / HikariCP).
- `idle_in_transaction_session_timeout`, `statement_timeout`, `lock_timeout` — защита от зависших транзакций и долгих блокировок.

### Autovacuum и bloat
- `autovacuum_naptime`, `autovacuum_vacuum_scale_factor` / `_threshold`, `autovacuum_vacuum_cost_limit` / `_cost_delay`, `autovacuum_max_workers`.
- Из-за MVCC UPDATE/DELETE оставляют мёртвые версии строк. Не вычищаются — таблицы и индексы раздуваются (bloat), запросы читают больше страниц.
- `fillfactor` < 100 — место для HOT-обновлений без обновления индексов.

### Схема и запросы
Индексы (и их избыток — каждый замедляет INSERT), партиционирование, типы данных, долгие транзакции (мешают vacuum), блокировки, N+1 запросы, отсутствие batch-вставок.

---

## 2.3. Прикладные метрики

### JVM: дедлоки и блокировки потоков

**Как обнаружить:**
1. **Thread dump** — основной инструмент:
   - `jstack -l <pid>` или `jcmd <pid> Thread.print -l`;
   - в Spring Boot — `GET /actuator/threaddump` (подключён в заглушке);
   - `kill -3 <pid>` — дамп в stdout процесса.
   В конце дампа JVM сама пишет **`Found one Java-level deadlock:`** и перечисляет потоки цикла.
2. Программно: `ThreadMXBean.findDeadlockedThreads()` (видит и `synchronized`, и `java.util.concurrent` локи) / `findMonitorDeadlockedThreads()`.
3. GUI: VisualVM, JConsole (вкладка Threads → Detect Deadlock), Java Mission Control / JFR (события `jdk.JavaMonitorEnter`, `jdk.ThreadPark`).
4. Метрики: `jvm_threads_states_threads{state="blocked"}` (есть на дашборде). Устойчивый рост BLOCKED — повод снять дамп.

**Как понять, какой поток и чем заблокирован.** Снимаем 3–5 дампов с интервалом 5–10 с: если поток в одном и том же месте во всех — он действительно висит.

Блокировка на `synchronized`:
```
"worker-2" #31 prio=5 ... java.lang.Thread.State: BLOCKED (on object monitor)
    at ru.app.Service.transfer(Service.java:42)
    - waiting to lock <0x000000076b2c1a30> (a java.lang.Object)
    - locked <0x000000076b2c1a40> (a java.lang.Object)
```
`waiting to lock <адрес>` — какой монитор ждёт. Ищем в дампе `- locked <тот же адрес>` — это поток-владелец. Если владелец сам ждёт монитор, который держит первый, — дедлок.

Блокировка на `ReentrantLock` и других `j.u.c`:
```
java.lang.Thread.State: WAITING (parking)
    - parking to wait for <0x...> (a java.util.concurrent.locks.ReentrantLock$NonfairSync)
```
Владельца ищем по разделу `Locked ownable synchronizers: - <0x...>` (нужен флаг `-l`).

Состояния потоков: RUNNABLE (работает или в нативном I/O), BLOCKED (ждёт монитор), WAITING / TIMED_WAITING (`wait()`, `park()`, `sleep()`, `join()`). В заглушке 12 consumer-потоков большую часть времени в TIMED_WAITING — это `Thread.sleep` эмуляции, не проблема.

---

### PostgreSQL

**Rows fetched / returned** (`pg_stat_database`: `tup_returned`, `tup_fetched`):
- `tup_returned` — сколько строк **прочитано**: строки, пройденные последовательными сканами, плюс записи индексов, возвращённые индексными сканами;
- `tup_fetched` — сколько живых строк **реально выбрано** из таблиц индексными сканами (heap fetch).
- Если `returned` на порядки больше `fetched` — много seq scan, читаем больше, чем нужно: смотрим `pg_stat_user_tables.seq_scan / seq_tup_read` и думаем об индексах.
- Рядом: `tup_inserted / updated / deleted`, `xact_commit / rollback`, `blks_hit / blks_read` (cache hit ratio, норма > 99%), `temp_files / temp_bytes` (не хватает work_mem), `deadlocks`.

**Блокировки:**
- `pg_locks` — все удерживаемые и ожидаемые блокировки (`granted = false` — ждёт).
- `pg_stat_activity`: `wait_event_type = 'Lock'`, `state`, `query`, `xact_start` (долгие транзакции).
- `pg_blocking_pids(pid)` — кто именно блокирует:
  ```sql
  SELECT pid, pg_blocking_pids(pid) AS blocked_by, wait_event_type, state, now() - xact_start AS tx_age, query
  FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0;
  ```
- `log_lock_waits = on` + `deadlock_timeout` — ожидания дольше deadlock_timeout пишутся в лог. Дедлоки Postgres разрывает сам (ошибка `deadlock detected`), счётчик — `pg_stat_database.deadlocks`.

**Время работы вакуума:**
- `pg_stat_user_tables`: `last_vacuum`, `last_autovacuum`, `vacuum_count`, `autovacuum_count`, `n_dead_tup`, `n_live_tup`.
- `pg_stat_progress_vacuum` — фаза и прогресс текущего VACUUM.
- `log_autovacuum_min_duration = 0` — в логе длительность каждого прохода autovacuum, сколько страниц/строк обработано и удалено.
- Тревожные признаки: `n_dead_tup` растёт, autovacuum работает постоянно или не запускается, долгие транзакции (`backend_xmin` в `pg_stat_activity`) не дают вычистить строки, приближение к wraparound (`age(datfrozenxid)`).

**Наполняемость разделов** (место на диске и размер таблиц/партиций):
- `pg_database_size()`, `pg_total_relation_size()`, `pg_relation_size()`, `pg_indexes_size()`;
- для партиционированных таблиц: `pg_partition_tree('table')` + размеры каждой партиции, равномерность распределения строк;
- свободное место на файловой системе с PGDATA и WAL (`node_filesystem_avail_bytes` из node_exporter), размер `pg_wal`;
- bloat: `pgstattuple`, оценочные запросы; `n_dead_tup / n_live_tup`.

**Как читать cost в плане выполнения:**
```
Seq Scan on persons  (cost=0.00..431.00 rows=10000 width=96) (actual time=0.010..2.130 rows=10000 loops=1)
```
- `cost=A..B` — оценка планировщика в **условных единицах** (за 1 принята последовательная загрузка страницы, `seq_page_cost = 1`). Это не миллисекунды.
- `A` — startup cost: сколько стоит получить первую строку (для Sort это почти вся работа).
- `B` — total cost: стоимость возврата всех строк.
- Стоимость узла **включает стоимость его дочерних узлов**.
- `rows` — ожидаемое число строк, `width` — средний размер строки в байтах.
- Для Seq Scan: `cost = страниц × seq_page_cost + строк × cpu_tuple_cost (+ строк × cpu_operator_cost на условие)`.
- С `EXPLAIN ANALYZE` появляется факт: `actual time=первая..последняя` в **мс на один проход**, `rows` факт, `loops` — сколько раз выполнялся узел (итоговое время = time × loops). Главное — сравнивать оценку `rows` с фактом: расхождение в десятки раз = устаревшая статистика или корреляция колонок, отсюда неудачный план.
- Читаем снизу вверх и изнутри наружу; ищем узел, где разница в actual time самая большая.

**Shared blocks** (`EXPLAIN (ANALYZE, BUFFERS)`, а также `pg_stat_statements`): блоки (страницы по 8 КБ) в общем буферном кэше `shared_buffers`.
- **hit** — блок уже был в shared_buffers, прочитан из памяти. Дёшево.
- **read** — блока не было в shared_buffers, его прочитали из ОС (с диска или из page cache ОС) и положили в кэш. Дорого, если реально с диска.
- **dirtied** — запрос изменил блок, который до этого был «чистым» (совпадал с диском). Теперь он «грязный» и позже будет записан на диск checkpointer'ом или bgwriter'ом. Бывает и у SELECT — из-за проставления hint-битов.
- **written** — запрос сам записал грязные блоки на диск, вытесняя их из кэша, чтобы освободить место. Плохой знак: значит, фоновые процессы (bgwriter/checkpointer) не успевают, и запись ложится на пользовательский запрос. Лечится размером `shared_buffers`, настройками bgwriter, частотой checkpoint.
- Рядом: **local** — то же для временных таблиц сессии, **temp** — временные файлы для сортировок/хешей, не поместившихся в `work_mem`.

Пример:
```
Buffers: shared hit=120 read=880 dirtied=3 written=1
```
— 120 страниц из кэша, 880 прочитаны из ОС (холодный кэш или таблица не помещается), 3 страницы изменены, 1 записана самим запросом.
