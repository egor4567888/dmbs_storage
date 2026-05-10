# Durable KV-store (учебный проект)

Распределённое durable KV-хранилище на Java 17 с ключами и значениями типа `u64`.
Состоит из трёх процессов: **client**, **server**, **orchestrator** — общаются по
TCP-loopback с собственным бинарным протоколом.

## Статус по этапам

| Этап | Содержание | Статус |
|------|------------|--------|
| 1 | Embedded storage с durability через WAL минимального уровня (как PostgreSQL `wal_level=minimal`) | ✅ готово |
| 2 | Сетевой server, client с верификацией локальной реплики, orchestrator с SIGKILL | ✅ готово |
| 3 | Синхронная и асинхронная репликация, chaos-diff-testing | ☐ не начат |

## Что гарантирует хранилище

> Если `put(k, v)` вернулся без исключения — пара `(k, v)` переживает любую
> аварию (kill -9, отключение питания, OOM-killer) и видна после следующего
> `Storage.open`.

Технически: каждый `put` блокируется до тех пор, пока WAL-запись не сделана
`fsync`. Group commit агрегирует одновременные put-ы из разных потоков в один
`fsync`, поэтому пропускная способность не убивается синхронной записью.

## Быстрый старт

```bash
# Все 42 теста по 6 модулям (~25 сек)
gradle test

# Только интеграционный chaos-тест (15 сек):
# оркестратор + 3 клиента + ~10 SIGKILL-ов сервера, финальная сверка durability
gradle :integration:test
```

## Демо вручную

```bash
# Терминал 1 — сервер
gradle :server:runServer -PstateDir=/tmp/kvdemo -Pport=12345

# Терминал 2 — клиент гоняет 30 секунд (после "LISTENING 12345" в первом окне)
gradle :client:runClient -Pport=12345 -PdurationSec=30 -PkeySpace=4096

# Альтернатива: полный хаос — оркестратор сам форкает и убивает сервер
gradle :orchestrator:runOrchestrator -PstateDir=/tmp/kvdemo -Pport=12345 \
       -PminLifeMs=700 -PmaxLifeMs=1800
```

После прогона можно посмотреть on-disk состояние:

```bash
ls -la /tmp/kvdemo /tmp/kvdemo/wal/
xxd -c 29 /tmp/kvdemo/wal/wal-*.log | head      # по одной WAL-записи на строку
xxd -c 16 -s 28 /tmp/kvdemo/snapshot.bin | head # по одной (key,value) на строку
```

## Структура проекта

```
dbms_BD/
├── storage/         — embedded KV: WAL, снапшот, recovery (этап 1)
├── protocol/        — общий wire-формат запрос/ответ
├── server/          — TCP-сервер поверх storage
├── client/          — KvClient + WorkloadRunner + LocalReplica с верификацией
├── orchestrator/    — форкает сервер, шлёт SIGKILL, перезапускает
├── integration/     — ChaosTest: всё вместе под крэш-тестингом
├── ARCHITECTURE.md  — подробная памятка по архитектуре со сносками
├── CLAUDE.md        — обзор для онбординга, инварианты, история решений
└── task.txt         — оригинальная постановка задания
```

Подробное описание модулей и потоков данных — в `CLAUDE.md`. Глубокий разбор
durability-инвариантов, форматов WAL/snapshot, машины состояний клиента — в
`ARCHITECTURE.md`.

## Метрики

На одном клиенте без оркестратора, mix 50/50 GET/PUT, key space 4096,
дефолтный `StorageConfig`:

```
ops=229,062  duration=10.00s  throughput=22,900 ops/s  failures=0
```

То есть ~11,400 PUT/s на одиночного клиента. Под крэш-тестингом (оркестратор
убивает сервер каждые 700–1800 мс) — ~16,000 ops/s агрегатно на 3 клиента, **0
нарушений верификации**, **11 SIGKILL за 15 секунд** теста.

## Wire-протокол

Бинарный, little-endian, фиксированные размеры фреймов:

```
Запрос  (17 байт):  [u8 op][u64 key][u64 value]
                       op:    1=GET, 2=PUT
Ответ   ( 9 байт):  [u8 status][u64 value]
                       status: 0=OK, 1=NOT_FOUND, 2=UNAVAILABLE
```

## On-disk формат

```
<stateDir>/
├── snapshot.bin                  — последний валидный снапшот (atomic temp+rename)
└── wal/
    ├── wal-00000000000000000001.log    — имя = 20-значный startLsn
    ├── wal-00000000000000000017.log
    └── wal-00000000000000000033.log    — активный сегмент (растёт)
```

**WAL-запись** (29 байт, LE): `u64 lsn | u8 op | u64 key | u64 value | u32 crc32c`.
**Снапшот** (LE): `magic | u64 appliedLsn | u64 count | u32 headerCrc | count×(key,value) | u32 payloadCrc`.

## Требования

- Java 17 (любой LTS-JDK)
- Gradle 8+ (системный; Gradle wrapper не сгенерирован)

## Лицензия

Учебный проект, без формальной лицензии.
