# Design Notes

Design decisions and trade-offs for the NOAA GSOD Vertica loader. Covers the
load pipeline, the schema/projection choices, and the path to scale.

---

## 1. Problem

- 3-node Vertica cluster. Fact data arrives as `.tar.gz` archives on each node's
  local disk. Each archive holds ~300 **gzip-compressed**, headerless,
  tab-delimited TSV members (so there are **two** gzip layers).
- Goal: load the fact data into one table, in parallel from every node, safely
  (no partial loads), plus a small replicated dimension for lookups, and answer
  a "top-10 hottest places" query.

---

## 2. Load pipeline

### 2.1 Two-phase load: untar on node, then COPY from dir
Each archive contains ~300 gzip-compressed TSV members (two layers: outer
`.tar.gz`, inner per-member `.gz`). The load is split into two steps per node:

```
Step 1 — ssh <node> "mkdir -p /tmp/vload_<run>/<node> &&
                     tar -xzf archive1.tar.gz -C /tmp/... &&
                     tar -xzf archive2.tar.gz -C /tmp/..."

Step 2 — COPY weather_fact FROM '/tmp/vload_<run>/<node>/*.gz'
          ON <db_node> GZIP DELIMITER E'\t' STREAM NAME '...' DIRECT
```

- `tar -xzf` extracts the outer `.tar.gz` to disk; inner `.gz` members are left
  as individual files in the work dir.
- A single `COPY ... FROM '*.gz' ON <node> GZIP` reads all `.gz` files on that
  node directly — **no Python process in the data path**.
- **One COPY per node per run = one ROS container per node**, regardless of how
  many archives were extracted. This keeps ROS container counts low and mergeout
  overhead minimal (previously one COPY per archive = one ROS per archive).
- Work dir is always cleaned up in a `finally` block even on failure.

**Trade-off:** requires temporary disk space on each node for the extracted `.gz`
members. The space is proportional to one node's archive batch decompressed to
single-layer gzip.

### 2.2 Per-node atomicity (no partial loads)
The connection runs with **`autocommit=False`**. All archives on a node are
committed in a single transaction — either all land or none do:

```
untar_on_node(...)          # extract all archives to /tmp/work/
cur.execute(COPY_SQL)       # one COPY reads all *.gz — not durable yet
conn.commit()               # whole node batch in one transaction
```

A COPY error (e.g. REJECTMAX exceeded, Vertica error) rolls back everything from
that node; other nodes are unaffected. The failed node is reported `FAILED` and
retried next run (files still in source dir, not archived).

### 2.3 Concurrency: one worker per node
Nodes load concurrently via a thread pool (`max_parallel_loads`, default = node
count). Each worker connects to its own node (making it the COPY initiator) so
parse work spreads across the cluster. `cur.execute` of a large COPY is I/O-bound,
so the GIL is released and threads run genuinely in parallel.

### 2.4 Bad records
- `REJECTED DATA AS TABLE` diverts malformed rows to a reject table instead of
  failing the load — GSOD needs this (sentinels, a stray header member).
- `REJECTMAX` (`max_bad_records`) is the ceiling: an archive tolerates that many
  rejects and still commits; reaching it aborts the `COPY`, which rolls the whole
  archive back. `0` = unlimited.

### 2.5 Idempotency
A `load_audit` row is written per archive (which file, which stream, when,
status). Before loading, already-loaded filenames (basename) are skipped, so the
loader is safe to re-run / schedule. A `check_duplicates` flag disables this for
repeated test loads.

### 2.6 Reporting from Vertica, not the script
Row counts and timing come from `V_MONITOR.LOAD_STREAMS` (keyed by a per-archive
`STREAM NAME`). The script does not re-read files or count rows itself — cheaper,
and the numbers are Vertica's own.

### 2.7 Archiving - Disabled
On success, the archive is `mv`d to a backup dir with a `.archived` suffix (the
suffix marks it loaded). Controlled by `archive_loaded`; dedup does not depend on
the move (it uses the audit table), so files may also be left in place.

---

## 3. Schema & projections

### 3.1 Fact table `weather_fact`
- **VARCHAR identifiers** (`stn`, `wban`): GSOD ids have leading zeros
  (`010010`) and `wban=99999` is a "no-WBAN" sentinel — loading as INT would
  corrupt joins.
- **ISO dates** parsed straight into `DATE` (no `FORMAT` clause needed).
- **`yearmoda NOT NULL`**: it is the partition key (`PARTITION BY YEAR(yearmoda)`).
  A NULL partition expression rolls back the whole load; `NOT NULL` turns a
  null/blank date into a single rejected row instead (and silences warning 9249).
- **Missing values are per-column sentinels** (`9999.9`, `999.9`, `99.99`),
  loaded as-is and stripped with `NULLIF` in queries — a single global `NULL`
  token on `COPY` would be wrong because the sentinel differs by column.

### 3.2 Super-projection (segmentation)
`SEGMENTED BY HASH(stn, wban) ALL NODES`, sorted `stn, wban, yearmoda`:
- Co-locates each station's full history on one node, so `GROUP BY stn, wban`
  (the goal query) is fully local — no cross-node resegmentation.
- Sort order lets `stn`/`wban` RLE collapse to a few runs and `yearmoda` DELTAVAL
  compress sequential dates.

### 3.3 Dimension `weather_station`
`UNSEGMENTED ALL NODES` (replicated) so the join to the fact table is
local and never broadcasts. Loaded via `ssh cat <tsv> | COPY … SKIP 1` (it has a
header), full-refresh, as its own step/mode — separate from table setup and from
the fact load so reference data can be refreshed independently.

---

## 4. Goal query
<TBD>


---

## 5. Scaling 


- **Node-local / apportioned load.** The current `FROM STDIN` path funnels bytes
  through the loader. At scale, expose each archive's members as node-local files
  (or FIFOs) and use `COPY … FROM '<path>' ON <node>` so every node reads and
  parses its own data in parallel — Vertica's fastest bulk path — or `COPY` from
  object storage, which the subcluster parallelizes automatically.
- **Partitioning.** `PARTITION BY YEAR` buys nothing for the goal query but is
  essential operationally (per-partition mergeout, tiering, cheap drops). With
  100+ year-partitions, use **hierarchical partitioning** to bound ROS container
  counts and keep the catalog small.
- **Compute isolation.** Separate load and query subclusters, sized so the query
  working set fits the depot.

---

## 6. Modes & configuration

Everything is config-driven (`config.yaml`); no values are hard-coded. Modes are
split so each concern runs independently:

- `setup-tables` — schema + tables only (DDL in `schemas/create_tables.sql`).
- `load-mapping` — dimension table only.
- `load-fact` — fact archives only.
- `all` — the three above, in order.
- `cleanup-fact` — TRUNCATE fact + reject (keeps structure, the audit history,
  and the dimension, so fact data can be reloaded without re-running `load-mapping`).
- `destroy` — DROP all managed tables (including audit and the dimension) and clear backup dirs.

---

## 7. Limitations & next steps

- Loader is in the data path — fine at this scale, replaced by
  apportioned load at higher scale.
- `max_parallel_loads` is a global cap, not per-node (§2.3).
- Parallel loads share one reject table; under high concurrency a per-node reject
  table would avoid contention.
- The per-archive initiator connects to the node's `ssh_host` address, which
  assumes that address is reachable on the Vertica client port.
- Optional column shrink: `temp/max/min/dewp` are tenths-of-a-degree → storing as
  INT (tenths) roughly halves those columns vs FLOAT; needs a load-time transform,
  so treat as a second-pass optimization.
