# NVertica Multi-Node Loader

Loads the NOAA  weather dataset into a  multi-node Vertica cluster. Fact data arrives as `.tar.gz` archives on each node
(each archive ≈ 300 headerless, tab-delimited TSV members that are themselves
gzip-compressed — two gzip layers); the loader streams each archive straight into
one atomic `COPY ... FROM STDIN` 

Both Options available, Copy to Disk (optimized) or Copy Directly if no Disk(v1).

---

## Contents

| File                        | Purpose |
|-----------------------------|---------|
| `vertica_loader_v1.py`      | The loader — discovery, setup, atomic streaming load, audit, archive, cleanup |
| `test_vertica_loader.py`    | Tests — offline unit tests + opt-in `--live` cluster checks |
| `config.yaml`               | All settings (connection, nodes, paths, fact + dimension) |
| `schemas/create_tables.sql` | DDL — fact table, super-projection, dimension table, replicated projection |

---

## Requirements

- Python 3.8+, `pip install pyyaml vertica-python`
- Passwordless SSH from the host running the script to every cluster node
- A Vertica database **you have already created** (connected to by name)
- `gzip`/`tar` available on the nodes (standard)

---

## How it works (the flow)

```
SETUP                                   (--mode setup-tables)          [ all runs SETUP → MAPPING → LOAD ]
  create_schema                         create the target schema
  create_tables                         ← create_tables.sql  weather_fact, projections, weather_station, load_audit
  validate_objects                      confirm target + audit tables exist

MAPPING  (--mode load-mapping)
  load_dimension   ← weather_station.tsv  full-refresh the replicated mapping table

LOAD                                    (--mode load-fact)
  list_files_on_nodes                   SSH ls  *.tar.gz on every node
  build_manifest                        one entry per archive (archive ↔ node)
  filter_already_loaded                 skip archives already OK in load_audit
  load_tars  ─ archives loaded CONCURRENTLY (thread pool), each atomically:
      load_one_archive                  own conn to the file's node (COPY initiator)
        ssh tar -xzf → /tmp/vload_<stream>/<stem>/*.gz   (untar to work dir on node)
        COPY ... FROM '/tmp/.../*.gz' ON <node> GZIP      (Vertica reads node-local files)
        ssh rm -rf work dir                               (always, even on failure)
        _stream_stats                   read accepted/rejected from LOAD_STREAMS
  archive_file                          ssh mv  →  <name>.tar.gz.archived
  record_audit                          one row per archive (archive ↔ stream)
  print_summary                         run totals + list of archives
```

### Step by step

1. **Discover** — SSH to each node, `ls` the source dir for `*.tar.gz`, build a
   manifest (one entry per archive, archive ↔ node).
2. **Skip already-loaded** — check the audit table (by archive basename) so
   re-runs don't reload. Safe to run from cron.
3. **Load (atomic, server-side)** — for each archive, SSH-untar it into a temp
   work dir on the node (`/tmp/vload_<stream>/<stem>/`), then issue a single
   `COPY <fact> FROM '<work_dir>/*.gz' ON <node> GZIP` so Vertica reads the
   extracted files directly (no Python in the data path). Bad rows go to a
   per-stream reject table. The temp dir is always cleaned up afterwards.
4. **Commit / rollback** — commit the archive **only if** `tar` exits 0 **and**
   the COPY succeeds. Any failure (corrupt/partial tar, COPY error, dropped
   connection) rolls the whole archive back — **no partial loads**.
5. **Read stats** — totals (accepted / rejected) come from
   `V_MONITOR.LOAD_STREAMS` per archive; the script counts nothing itself.
6. **Archive (optional)** — if `paths.archive_loaded` is true, a local `mv` on
   the node moves the loaded `.tar.gz` to the backup dir, renamed
   `<name>.tar.gz.archived`. Set it to `false` to leave files in place — the
   audit table still prevents re-loading them.
7. **Audit + summary** — one audit row per archive (archive ↔ stream), then a
   printed report of whole-run totals and the list of archives.

---

## Atomicity — no partial loads

Each `.tar.gz` is one **all-or-nothing** unit. The load is driven from Python
with `autocommit=False`:

```python
# Step 1: untar the archive into a temp work dir on the node
ssh_run(cfg, host, f"mkdir -p {work_dir} && tar -xzf {archive} -C {work_dir} --strip-components=1")

# Step 2: Vertica reads the extracted .gz members directly (no Python in the data path)
cur.execute(
    f"COPY weather_fact FROM '{work_dir}/*.gz' ON {db_node} GZIP "
    f"DELIMITER E'\\t' STREAM NAME '{stream_name}' "
    f"REJECTED DATA AS TABLE rejectted_{stream_name} REJECTMAX {n} DIRECT"
)
conn.commit()           # whole archive in one transaction

# Step 3: always clean up the temp dir (even on failure — handled in finally)
ssh_run(cfg, host, f"rm -rf {work_dir}")

# Any exception in steps 1-2 is caught → conn.rollback(); nothing lands
```

So a tar that fails halfway can never leave half its  members committed.
Bad **rows** (sentinels, a stray header) are not failures, they are captured in
the per-stream reject table and the load still commits, **up to `max_bad_records`
(REJECTMAX)**; reaching that limit aborts the COPY and rolls the whole archive
back. 


## Parallelism

Archives load **concurrently** (`load_tars` → `ThreadPoolExecutor`, up to
`load.max_parallel_loads`). Each worker (`load_one_archive`) opens its **own**
connection to the **node that holds the file**, making that node the COPY
initiator — so parse work spreads across the cluster instead of funnelling
through one node. `cur.copy` is I/O-bound (network + subprocess), so the GIL is
released and the threads run genuinely in parallel.

> Remaining trade-off: data still hops node → script → Vertica (the Python
> process is in the data path), and it's one COPY per archive. For maximum
> throughput at petabyte scale, move to **apportioned load** (`COPY … FROM
> '<path>' ON <node>` reading node-local files directly) or object-store COPY in
> Eon — see the scaling notes in `build_fact_copy_sql` and the dataset handoff.

---



---

## DDL (`schemas/create_tables.sql`)

Applied automatically at setup (the loader sets `SEARCH_PATH` to
`schema.target_schema`, so the DDL is unqualified). Also runnable directly:
`vsql -f schemas/create_tables.sql`. It creates:

- **`weather_fact`** — fact table, 30 columns, `PARTITION BY YEAR(yearmoda)`
  (`yearmoda` is `NOT NULL` so a null-date row is rejected, not a whole-archive
  rollback). Ids are `VARCHAR` (GSOD ids have leading zeros; `wban=99999` is a
  sentinel). Dates are ISO `DATE` (no `FORMAT` clause needed).
- **`weather_fact_super`** — super-projection `SEGMENTED BY HASH(stn,wban) ALL NODES`,
  sorted `stn, wban, yearmoda`. Co-locates each station's history so
  `GROUP BY stn,wban` is fully local (no resegmentation); RLE + DELTAVAL compress
  the sort columns hard.
- **`weather_station`** + **`weather_station_rep`** — dimension table and its
  `UNSEGMENTED ALL NODES` (replicated) projection, so the join never broadcasts.

> Column order in `weather_fact` must match the TSV exactly 

---

## Usage

```bash
pip install pyyaml vertica-python

python vertica_loader_v1.py --config config.yaml --mode all           # setup + mapping + fact
python vertica_loader_v1.py --config config.yaml --mode setup-tables  # DDL only (no data)
python vertica_loader_v1.py --config config.yaml --mode load-mapping  # load dimension table only
python vertica_loader_v1.py --config config.yaml --mode load-fact     # load fact archives only
python vertica_loader_v1.py --config config.yaml --mode load-fact --dry-run   # show plan, no changes
python vertica_loader_v1.py --config config.yaml --mode cleanup-fact  # TRUNCATE fact + reject (keep audit + dimension)
python vertica_loader_v1.py --config config.yaml --mode destroy       # DROP all tables + clear backups
```

| Mode | Does |
|------|------|
| `setup-tables` | create schema + run `create_tables.sql` (incl. `load_audit`); validate. **No data.** |
| `load-mapping` | load ONLY the mapping/dimension table (`weather_station`), full refresh |
| `load-fact` | load ONLY the fact archives (`weather_fact`): discover → atomic parallel load → audit → archive → summary |
| `all` | `setup-tables` → `load-mapping` → `load-fact` |
| `cleanup-fact` | **TRUNCATE** fact + reject — wipe loaded fact data; keeps structure, the **audit history**, and the dimension |
| `destroy` | **DROP** all managed tables (CASCADE) and clear backup dirs — full teardown |

---

## End-of-job report

```
Run / Stream prefix       - vload_20260629_143210
Total files loaded        - 3
Total Records loaded      - 14897
Rejected Records          - 2
Total Time Taken          - 6.41 sec
Records Loaded per second - 2324

----- Loaded Data File Information -----
Node name              | File name        | Archived File name
v_verticadb_node0001   | batch01.tar.gz   | batch01.tar.gz.archived
v_verticadb_node0002   | batch02.tar.gz   | batch02.tar.gz.archived
```

- **Records / Rejected** come from `LOAD_STREAMS` (summed across archives).
- **Time** is wall-clock of the load phase; per-archive counts are intentionally
  not shown (kept cheap).

---

## Corrupt records & missing-value sentinels

Bad rows are diverted to a per-stream reject table, 

---

## Audit / load history

`load_audit` tracks which **archive** loaded under which **stream**; counts and
timing live in `LOAD_STREAMS`, joined by `stream_name` (not duplicated):

| column | meaning |
|--------|---------|
| `stream_name` | `vload_<timestamp>_NNNN` — join key to `LOAD_STREAMS` |
| `file_name` / `archived_name` | archive basename / its `.archived` name |
| `db_node` / `ssh_host` | which node it came from |
| `status` | `OK` / `FAILED` |
| `load_time` | when the run ran |

```sql
-- archives loaded + counts (join file tracking to Vertica's stream stats)
SELECT a.load_time, a.file_name, a.archived_name, a.status,
       s.accepted_row_count, s.rejected_row_count
FROM   load_audit a
LEFT JOIN v_monitor.load_streams s USING (stream_name)
ORDER  BY a.load_time DESC;
```

---

## Idempotency & scheduling

`filter_already_loaded` skips any archive whose basename already has an `OK` row
in the audit table, so the loader is safe on a schedule. (Set
`load.check_duplicates: false` to disable this — testing only, when you want to
reload the same files repeatedly.)

```cron
*/5 * * * * /usr/bin/python3 /opt/vload/vertica_loader.py \
    --config /opt/vload/config.yaml --mode load-fact >> /var/log/vload.log 2>&1
```

> Producers should drop archives atomically (write to a temp name, then `mv` into
> the source dir) so the loader never sees a half-written `.tar.gz`.

---

## Testing

Unit tests (Python's built-in `unittest`) run **offline** — all I/O (subprocess,
DB connections) is mocked, so no Vertica or SSH is needed. From the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install pyyaml vertica-python

python test_vertica_loader.py        # or:  python -m unittest -v
```

Expected:

```
Ran 7 tests in 0.00s

OK
```

**What they cover (7 tests across 4 classes):**

| Class | Test | What it checks |
|-------|------|----------------|
| `TestConfig` | `test_valid_config_passes` | valid config passes `validate_config` without raising |
| `TestConfig` | `test_missing_key_raises` | a config missing a required key raises `ValueError` |
| `TestSQLBuilders` | `test_fact_copy_sql` | `build_fact_copy_sql` emits correct `COPY … FROM '…/*.gz' ON <node> GZIP`, delimiter, stream name, per-stream reject table, and `DIRECT` |
| `TestDedup` | `test_skips_already_loaded` | `filter_already_loaded` drops files already `OK` in the audit table |
| `TestAtomicLoad` | `test_commit_on_success` | happy path: `commit` called once, `rollback` never, `status=OK` |
| `TestAtomicLoad` | `test_rollback_when_untar_fails` | SSH untar error → `rollback` called, `commit` never, `status=FAILED` |
| `TestAtomicLoad` | `test_rollback_when_copy_fails` | Vertica COPY error → `rollback` called, `commit` never, `status=FAILED` |

End-to-end validation is a real run on the cluster:
`python vertica_loader.py --config config.yaml --mode all`.

---

## Notes / limitations

- The loader connects to an existing database; it does not create the physical DB.
- Counts/timing come from `LOAD_STREAMS`; the script does not re-read files for counts.
- Archives load concurrently, each on its own connection to the file's node, but
  data still passes through the Python process (node → script → Vertica). The
  ceiling is apportioned/node-local `COPY … ON <node>` — see `build_fact_copy_sql`.
- Parallel loads share one `REJECTED DATA AS TABLE`. Under high concurrency, if
  you see contention on it, switch to a per-node reject table.
- `max_parallel_loads` bounds concurrent COPYs; each consumes memory from the
  resource pool, so don't set it far above the node count.
- For the 5 PB design target, see the dataset handoff (Eon Mode, hierarchical
  partitioning, compute isolation, optional INT-tenths column shrink).
