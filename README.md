# dbsync-cli

A database synchronisation CLI powered by **Debezium Embedded** and **Quarkus**.

Supports:
- **MySQL → PostgreSQL**, **PostgreSQL → MySQL**, or same-type sync (MySQL → MySQL, PostgreSQL → PostgreSQL)
- Full snapshot **+** continuous CDC (Change Data Capture) via binlog / logical replication
- Automatic schema synchronisation (DDL type mapping between MySQL and PostgreSQL)
- Real-time **TUI dashboard** showing per-table sync progress

---

## Requirements

| Requirement | Version |
|---|---|
| Java | 21+ |
| MySQL source | 5.7+ (`binlog_format=ROW`) |
| PostgreSQL source | 10+ (`wal_level=logical`) |

---

## Quick Start

### 1. Build

```bash
./gradlew build -x test
```

Runnable JAR at `build/quarkus-app/quarkus-run.jar`.

### 2. Configure

```bash
cp src/main/resources/sync-config-example.yaml sync-config.yaml
# edit sync-config.yaml
```

### 3. Run

```bash
java -jar build/quarkus-app/quarkus-run.jar
# or with inline overrides:
java -jar build/quarkus-app/quarkus-run.jar \
  -c sync-config.yaml \
  --source-host db1.example.com \
  --target-host db2.example.com
```

---

## Configuration (`sync-config.yaml`)

```yaml
source:
  type: mysql           # mysql | postgresql
  host: localhost
  port: 3306
  database: source_db
  username: root
  password: secret
  serverId: 12345       # MySQL: unique ID for Debezium binlog reader

target:
  type: postgresql
  host: localhost
  port: 5432
  database: target_db
  username: postgres
  password: secret

sync:
  tables: []            # empty = all tables
  schemaSync: true      # auto create/alter target tables
  snapshotMode: initial # initial | never | schema_only | always
  batchSize: 500
  offsetStorePath: ./offsets.dat
  schemaHistoryPath: ./schema-history.dat
```

### CLI option overrides

```
-c / --config             YAML config file path
--source-host/port/database/user/password
--target-host/port/database/user/password
-h / --help
-V / --version
```

---

## MySQL Prerequisites

```ini
# my.cnf
log_bin        = ON
binlog_format  = ROW
binlog_row_image = FULL
server_id      = 1
```

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'dbsync'@'%';
```

## PostgreSQL Prerequisites

```ini
# postgresql.conf
wal_level = logical
max_replication_slots = 4
```

```sql
CREATE ROLE dbsync WITH LOGIN REPLICATION PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbsync;
```

---

## TUI Dashboard

Press **`q`** or **`Esc`** to quit gracefully.

```
 ⚡ dbsync-cli  │  14:22:05  │  Press 'q' to quit
 TABLE                          PHASE          SNAPSHOT PROGRESS      INSERTS    UPDATES    DELETES    LAST EVENT
 ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 orders                         CDC            100.0% (12,450/12,450)    3,241        892        104   2s ago
 products                       SNAPSHOT       67.3% (8,042/11,955)          0          0          0   0ms ago
 users                          CDC            100.0% (5,230/5,230)      1,105        437         12   1s ago
 Tables: 3  │  CDC events: 5,791
```

---

## Architecture

```
Main (@QuarkusMain)                    — CLI parsing (picocli)
└─ SyncCommand (@ApplicationScoped)   — orchestration
   ├─ ConfigLoader                     — load & merge YAML + CLI overrides
   ├─ SchemaApplier                    — read source schema, create/alter target tables
   │  ├─ SchemaReader                  — JDBC DatabaseMetaData
   │  └─ DdlTranslator                 — MySQL ↔ PostgreSQL type mapping
   ├─ DebeziumEngineManager            — Debezium EmbeddedEngine (snapshot + CDC)
   │  └─ ChangeEventHandler           — JSON event → TableWriter routing
   │     └─ TableWriter               — UPSERT / DELETE on target DB
   ├─ SyncProgressRegistry            — thread-safe per-table state
   └─ TuiDashboard (Lanterna)         — 200ms refresh terminal UI
```

---

## Offset & Restart

Debezium stores its state in `offsets.dat`. Restart with `snapshotMode: never` to resume from the last offset without re-snapshotting.
