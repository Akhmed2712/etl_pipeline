# Config-Based ETL in Scala — v1

> **Scope of this doc:** Catalog & resource config design, the big-picture ETL architecture, and how to run it.

---

## Table of Contents

1. [Big Idea](#1-big-idea)
2. [Catalog Config](#2-catalog-config)
3. [Resource Config](#3-resource-config)
4. [How It All Fits Together](#4-how-it-all-fits-together)
5. [How to Run](#5-how-to-run)

---

## 1. Big Idea

The goal is a **config-driven ETL framework** where you never touch Scala code to onboard a new pipeline job. Everything a job needs — what to read, how to transform, where to write, which Spark resources to use, and how to connect to databases — is declared in YAML files.

```
YAML configs
  └─ catalog.yml →  what data to move and how
  └─ resource.yml →  how much Spark to give it
  └─ credentials.yml →  database secrets (gitignored)

Scala engine
  └─ reads the configs
  └─ resolves date templates
  └─ loads the right transform script
  └─ reads from source, writes to destination
```

**Why this pattern?**

- **Zero-code onboarding** — a new job is a new YAML block, not a new PR to the engine.
- **Environment promotion** — swap `--env dev` → `--env prod`; configs diverge, engine doesn't.
- **Date portability** — use `{~~~%Y-%m-%d:day=1~~~}` in any string value; the engine resolves it at runtime from `--start_date`.
- **Separation of concerns** — infra (Spark sizing, JDBC URLs) stays out of transform logic.

---

## 2. Catalog Config

**Location:** `catalogs/<env>/catalog.yml`

The catalog is the **job blueprint**. One top-level key = one pipeline job.

### Full example

```yaml
# Anchor for shared MySQL connection settings
_mysql_conf: &mysql_conf
  layer: mysql
  env: dev
  credentials_key: mysqldev # → looks up this key in credentials.yml
  database: transactions # → used in jdbc:mysql://host:port/<database>

my_orders_job:
  <<: *mysql_conf
  table: orders_summary # destination table
  schema: reporting # destination schema  →  reporting.orders_summary
  script_type: scala
  script_location: etl_pipeline.scripts.my_orders_job.MyOrdersJob # to run

  read_conf:
    # Scalar dates — resolved from --start_date at runtime
    current_date: "{~~~%Y-%m-%d:day=0~~~}" # start_date − 0 days
    load_date: "{~~~%Y-%m-%d:day=0~~~}"
    current_month: "{~~~%Y-%m-01:day=0~~~}" # first of the month

    # Source table A
    transactions_1:
      database: transactions
      table: orders
      filter: "encounter_date = '{~~~%Y-%m-%d:day=1~~~}'"   # yesterday

    # Source table B
    orders_archive:
      database: archive
      table: orders_hist
      filter: "encounter_date >= '{~~~%Y-%m-%d:day=3~~~}' AND encounter_date <= '{~~~%Y-%m-%d:day=1~~~}'"

  write_conf:
    mode: overwrite # overwrite | append
    schema:
      - column_name: order_id
        type: bigint
        constraint: primary_key,not_null
      - column_name: customer_id
        type: varchar(50)
      - column_name: total_amount
        type: decimal(18,2)
      - column_name: order_status
        type: varchar(30)
      - column_name: created_date
        type: varchar(20)
      - column_name: load_ts
        type: timestamp
    options:
      batchsize: 1000
      numPartitions: 5
```

### Field reference

| Field | Required | Description |
|---|---|---|
| `credentials_key` | ✅ | Key in `credentials.yml` to use for JDBC auth |
| `database` | ✅ | Destination database name |
| `table` | ✅ | Destination table name |
| `schema` | ✅ | Destination schema; combined as `schema.table` |
| `script_type` | ✅ | `scala` (Scala object)
| `script_location` | ✅ | Path to the transform file, relative to project root |
| `read_conf` | ✅ | Free-form map passed directly into `processData()` |
| `write_conf.mode` | ✅ | Spark write mode: `overwrite` or `append` |
| `write_conf.options.batchsize` | ❌ | JDBC batch insert size (default: 1000) |
| `write_conf.options.numPartitions` | ❌ | Spark JDBC parallelism (default: 5) |

### Date template syntax

Any string value anywhere in the catalog (including nested maps and filter strings) can use:

```
{~~~<strftime_format>:day=<N>~~~}
```

- `<strftime_format>` — a Python/Java strftime pattern: `%Y`, `%m`, `%d`
- `day=<N>` — subtract N days from `--start_date` before formatting

**Examples with `--start_date 2024-01-15`:**

| Template | Resolved value |
|---|---|
| `{~~~%Y-%m-%d:day=0~~~}` | `2024-01-15` |
| `{~~~%Y-%m-%d:day=1~~~}` | `2024-01-14` |
| `{~~~%Y-%m-%d:day=3~~~}` | `2024-01-12` |
| `{~~~%Y-%m-01:day=0~~~}` | `2024-01-01` |
| `{~~~%Y-%m-01:day=1~~~}` | `2024-01-01` *(Jan 14 → first of Jan)* |

Templates can appear anywhere inside a value string, including inside SQL filter expressions:

```yaml
filter: "encounter_date = '{~~~%Y-%m-%d:day=1~~~}' AND status = 'ACTIVE'"
# → "encounter_date = '2024-01-14' AND status = 'ACTIVE'"
```

---

## 3. Resource Config

**Location:** `catalogs/<env>/resource.yml`

The resource config controls **Spark sizing**. One top-level key = one resource profile. By convention the key name matches the catalog job name.

### Full example

```yaml
# Shared base config — timezone alignment across driver and executors
_base_conf: &base_conf
  spark.sql.session.timeZone: Asia/Jakarta
  spark.driver.extraJavaOptions: "-Duser.timezone=Asia/Jakarta"
  spark.executor.extraJavaOptions: "-Duser.timezone=Asia/Jakarta"

# Preset: small workloads
low: &low
  <<: *base_conf
  spark.executor.cores: 1
  spark.executor.memory: 1g
  spark.executor.instances: 2

# Preset: medium workloads
medium: &medium
  <<: *base_conf
  spark.executor.cores: 2
  spark.executor.memory: 2g
  spark.executor.instances: 3

# Job-specific profile — inherits from a preset, adds overrides
my_orders_job:
  <<: *low
  spark.app.name: my_orders_job
  spark.sql.shuffle.partitions: 10
```

### Field reference

| Field | Description |
|---|---|
| `spark.app.name` | Shown in Spark UI and logs |
| `spark.executor.cores` | CPUs per executor |
| `spark.executor.memory` | RAM per executor (e.g. `1g`, `4g`) |
| `spark.executor.instances` | Number of executors |
| `spark.sql.shuffle.partitions` | Parallelism for shuffle operations (joins, groupBy) |
| `spark.sql.session.timeZone` | Session timezone for date/timestamp functions |
| `spark.driver.extraJavaOptions` | JVM args for the driver |
| `spark.executor.extraJavaOptions` | JVM args for each executor |

Any valid `SparkConf` key can be added here. The engine passes all keys directly to `SparkConf.set(k, v)`.

### Choosing a preset

| Preset | Use for |
|---|---|
| `low` | Lightweight aggregations, small tables (< 10M rows) |
| `medium` | Mid-size joins, moderate shuffle (10M–100M rows) |
| Custom | Override any field directly under your job key |

---

## 4. How It All Fits Together

```
┌─────────────────────────────────────────────────────────────┐
│                        spark-submit                         │
│   --env dev  --catalog my_orders_job  --start_date 2024-…   │
└────────────────────────┬────────────────────────────────────┘
                         │
                    ┌────▼────┐
                    │  Main   │  (entry point)
                    └────┬────┘
                         │
           ┌─────────────┼──────────────┐
           │             │              │
     ┌─────▼──────┐ ┌────▼──────┐ ┌────▼──────────┐
     │ YamlLoader │ │DateResolv.│ │CredentialsLoad│
     │            │ │           │ │               │
     │catalog.yml │ │{~~~...~~~}│ │credentials.yml│
     │resource.yml│ │→ real date│ │→ jdbcUrl+props│
     └─────┬──────┘ └───────────┘ └───────────────┘
           │
     ┌─────▼──────────────────────┐
     │       SparkSession         │
     │  (configured from resource)│
     └─────┬──────────────────────┘
           │
     ┌─────▼──────────────────────┐
     │   MyOrdersJob.processData  │
     │  (reads read_conf, returns │
     │   a DataFrame)             │
     └─────┬──────────────────────┘
           │
     ┌─────▼──────────────────────┐
     │      df.write.jdbc(...)    │
     │  (uses write_conf + creds) │
     └────────────────────────────┘
```

**Runtime sequence:**

1. Parse CLI args (`--env`, `--catalog`, `--resource`, `--start_date`)
2. Load `catalog.yml` and `resource.yml` for the given env
3. Resolve all `{~~~...~~~}` templates using `--start_date`
4. Load JDBC credentials from `credentials.yml` via `credentials_key`
5. Create `SparkSession` with all keys from the resource profile
6. Call `MyOrdersJob.processData(spark, read_conf)` → returns `DataFrame`
7. Write the `DataFrame` to MySQL using `write_conf` settings

---

## 5. How to Run

### Prerequisites

- Docker
- Java 17
- Scala 2.13 + SBT
- Apache Spark 3.3+ (`spark-submit` on PATH)
- MySQL JDBC driver included in the fat jar (via `build.sbt`)
- `credentials/credentials.yml` filled in (copy from `credentials_example.yml`)

### Build

```bash
# Compile and run tests
sbt test

# Build fat jar (includes all dependencies except Spark)
sbt assembly
# → target/scala-2.13/etl-pipeline-assembly-1.0.0.jar
```

### Run locally (dev)

```bash
sudo bash run_spark.sh etl-pipeline-assembly-1.0.0.jar my_orders_job my_orders_job 2024-01-15
```

### Inside run_spark.sh
```
#!/bin/bash

# Usage: ./run_spark.sh <jar_name> <catalog> <resource> <start_date>
JAR_NAME=${1:-"etl_pipeline-assembly-1.0.0.jar"}
CATALOG=${2:-"my_orders_job"}
RESOURCE=${3:-"my_orders_job"}
START_DATE=${4:-"2024-01-15"}

docker run --rm \
  -v "$(pwd)/target/scala-2.13/:/app/" \
  -v "$(pwd)/catalogs:/opt/spark/work-dir/catalogs" \
  -v "$(pwd)/configs:/opt/spark/work-dir/configs" \
  -v "$(pwd)/credentials:/opt/spark/work-dir/credentials" \
  -w /opt/spark/work-dir \
  apache/spark:4.0.2-scala2.13-java17-ubuntu \
  /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --class etl_pipeline.Main \
  /app/"$JAR_NAME" \
  --env dev \
  --catalog "$CATALOG" \
  --resource "$RESOURCE" \
  --start_date "$START_DATE"
```

### CLI arguments

| Argument | Required | Values | Description |
|---|---|---|---|
| `--env` | ✅ | `dev` \| `uat` \| `prod` | Which `catalogs/<env>/` to read from |
| `--catalog` | ✅ | any key in `catalog.yml` | Which job config to run |
| `--resource` | ✅ | any key in `resource.yml` | Which Spark sizing profile to use |
| `--start_date` | ❌ | `YYYY-MM-DD` | Anchor date for `{~~~...~~~}` template resolution |

> **Tip:** If `--start_date` is omitted, no date template resolution is performed. Any unresolved `{~~~...~~~}` strings in the config will be passed as-is to the transform — useful for testing or static configs.

### Credentials setup

```bash
cp credentials/credentials_example.yml credentials/credentials.yml
# Edit credentials.yml — fill in host, port, user, password
# This file is gitignored and never committed
```

`credentials.yml` structure:

```yaml
mysqldev:
  host: localhost
  port: 3306
  database: database
  user: myuser
  password: mypassword
  driver: com.mysql.cj.jdbc.Driver   # optional, this is the default
```

The `credentials_key` in `catalog.yml` must match a top-level key here.

---

*v1 — covers catalog & resource configs, architecture overview, and run instructions.*
