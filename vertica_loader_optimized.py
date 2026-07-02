#!/usr/bin/env python3
"""
vertica_loader.py script loads the GZ files into a Vertica table in parallel from every node.

Modes:
  --mode all           setup-tables + load-mapping + load-fact (full pipeline)
  --mode setup-tables  create schema + tables only (no data)
  --mode load-mapping  load ONLY the mapping/dimension table (weather_station)
  --mode load-fact     load ONLY the fact archives (weather_fact)
  --mode cleanup-fact  TRUNCATE fact + reject (keep audit, dimension + structure)
  --mode destroy       DROP all managed tables (CASCADE) and clear backup dirs

Typical use:
  python vertica_loader.py --config config.yaml --mode all
  python vertica_loader.py --config config.yaml --mode load-fact --dry-run
"""

import argparse
import logging
import os
import shlex
import subprocess
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import yaml
import vertica_python

# Vertica surfaces server NOTICE/INFO messages as Python warnings
# (real problems still raise exceptions).
for _pat in (r".*Nothing was dropped.*",
             r".*nothing was done.*",
             r".*no transaction in progress.*",
             r".*TLS is not configured.*"):
    warnings.filterwarnings("ignore", message=_pat)

log = logging.getLogger("vload")


# Parse the command line arguments #
def parse_args():
    p = argparse.ArgumentParser(description="Vertica multi-node parallel GZ loader")
    p.add_argument("--config", required=True, help="path to config.yaml")
    p.add_argument("--mode", required=True,
                   choices=["all", "setup-tables", "load-mapping", "load-fact",
                            "cleanup-fact", "destroy"])
    p.add_argument("--dry-run", action="store_true",
                   help="discover + print the plan, but do not load/move anything")
    return p.parse_args()


# Load the Configuration File #
def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    validate_config(cfg)
    return cfg


def validate_config(cfg):
    """Fail the job if required parameters are missing in YMAL file"""

    required_params = {
        "vertica": ["host", "port", "user", "password", "database"],
        "cluster": ["ssh_user", "nodes"],
        "paths":   ["source_dir", "backup_dir", "file_glob"],
        "schema":  ["target_schema", "schema_files"],
        "load":    ["target_table", "reject_table","audit_table", "delimiter"],
    }
    for section, keys in required_params.items():
        if section not in cfg:
            raise ValueError(f"config missing section: [{section}]")
        for k in keys:
            if k not in cfg[section]:
                raise ValueError(f"config missing key: {section}.{k}")
    if not cfg["cluster"]["nodes"]:
        raise ValueError("config cluster.nodes is empty")


def fq(cfg, table_key):
    """FQ Table name"""
    return f'{cfg["schema"]["target_schema"]}.{cfg["load"][table_key]}'


def ssh_base(cfg, host):
    """"
    Generates a base SSH command with specific configuration and host details.
    Batch mode=yes,                      skip any feedback, fail fast
    ConnectTimeout=10                   Fail fast is ssh not able to go through
    StrictHostKeyChecking=accept-new    Only allow new host, for known ones strict check.

    Returns:
        list: A list of strings representing the SSH command and its arguments.
    """
    return [
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
        "-o", "StrictHostKeyChecking=accept-new",
        f'{cfg["cluster"]["ssh_user"]}@{host}',
    ]

def ssh_run(cfg, host, command):
    """Run a command on the given node over ssh. Returns stdout (str)."""
    res = subprocess.run(ssh_base(cfg, host) + [command],
                         capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"ssh {host} failed: {res.stderr.strip()}")
    return res.stdout



def list_files_on_nodes(cfg):
    """SSH to each node and list ready-to-load files. -> {db_node: [paths]}."""
    glob = os.path.join(cfg["paths"]["source_dir"], cfg["paths"]["file_glob"])
    node_files = {}

    for node in cfg["cluster"]["nodes"]:
        # ls -1, null-safe if no matches
        out = ssh_run(cfg, node["ssh_host"],
                      f"ls -1 {glob} 2>/dev/null || true")
        files = [line.strip() for line in out.splitlines() if line.strip()]
        node_files[node["db_node"]] = files
        log.info("Cluster Node %s (%s): %d file(s)",
                 node["db_node"], node["ssh_host"], len(files))
    return node_files


def build_manifest(cfg, node_files):
    """Flatten discovery into a list of load entries (one per file)."""

    host_by_node = {n["db_node"]: n["ssh_host"] for n in cfg["cluster"]["nodes"]}
    manifest = []

    for db_node, files in node_files.items():
        for path in files:
            manifest.append({
                "db_node":  db_node,
                "ssh_host": host_by_node[db_node],
                "path":     path,
                "name":     os.path.basename(path),
            })
    return manifest


def filter_already_loaded(conn, cfg, manifest):
    """Drop files already loaded as in the audit table (idempotency)."""
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT file_name FROM {fq(cfg,'audit_table')} "
            f"WHERE status IN ('OK','OK_WITH_REJECTS')")
        done = {row[0] for row in cur.fetchall()}
    except Exception:
        done = set()          # audit table may not exist yet
    finally:
        cur.close()

    fresh = [e for e in manifest if e["name"] not in done]
    skipped = len(manifest) - len(fresh)

    if skipped:
        log.info("Skipping %d already-loaded file(s)", skipped)
    return fresh


def print_manifest(manifest):
    if not manifest:
        log.info("Manifest File List is empty — Nothing to load")
        return
    log.info("---- Files to load (%d) ----", len(manifest))
    for e in manifest:
        log.info("  %-22s  %s", e["db_node"], e["path"])


# Vertica Connection #
def get_connection(cfg, host=None):
    """Open a Vertica connection (autocommit off), leave the commit on the user. Pass host
    to make a specific node the COPY initiator, used so each parallel load parses
    on its own node instead of funnelling every stream through one node."""
    v = cfg["vertica"]

    # Add 30 seconds timeout
    return vertica_python.connect(
        host=host or v["host"], port=v["port"], user=v["user"],
        password=v["password"], database=v["database"],
        connection_timeout=v.get("connection_timeout", 30),
        tlsmode=v.get("tlsmode", "prefer"),   # set 'disable' for plain (untrusted-net) connections
        autocommit=False,
    )



def create_schema(conn, cfg):
    schema = cfg["schema"]["target_schema"]
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM v_catalog.schemata WHERE schema_name = %s", [schema])
    existed = cur.fetchone() is not None
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.commit()
    log.info("Schema %s %s", schema, "already exists" if existed else "created")


def create_tables(conn, cfg):
    """Run every .sql file in the schema_files dir under the target schema."""
    cur = conn.cursor()
    cur.execute(f'SET SEARCH_PATH TO {cfg["schema"]["target_schema"]}')
    sql_dir = cfg["schema"]["schema_files"]

    files = sorted(f for f in os.listdir(sql_dir) if f.endswith(".sql"))
    if not files:
        log.warning("No .sql files found in %s", sql_dir)
    for fname in files:
        with open(os.path.join(sql_dir, fname)) as fh:
            ddl = fh.read()
        for stmt in (s.strip() for s in ddl.split(";")):
            if stmt:
                cur.execute(stmt)
        log.info("Applied schema file: %s", fname)
    conn.commit()


def validate_objects(conn, cfg):
    """Confirm the target + audit tables actually exist; raise if not."""
    cur = conn.cursor()
    schema = cfg["schema"]["target_schema"]
    for tbl in (cfg["load"]["target_table"], cfg["load"]["audit_table"]):
        cur.execute(
            "SELECT COUNT(*) FROM v_catalog.tables "
            "WHERE table_schema = %s AND table_name = %s", [schema, tbl])
        if cur.fetchone()[0] == 0:
            raise RuntimeError(f"validation failed: {schema}.{tbl} not found")
        log.info("validated: %s.%s exists", schema, tbl)


# ── loading Metrics ── #
def _stream_stats(cur, stream_name):
    """Read the load's totals straight from Vertica's V_MONITOR.LOAD_STREAMS,
    so the script computes/counts nothing itself. Returns accepted, rejected and
    duration for the COPY identified by stream_name."""
    try:
        cur.execute(
            "SELECT COALESCE(SUM(accepted_row_count),0), "
            "       COALESCE(SUM(rejected_row_count),0), "
            "       COALESCE(MAX(load_duration_ms),0) "
            "FROM v_monitor.load_streams WHERE stream_name = %s", [stream_name])
        acc, rej, dur_ms = cur.fetchone()
        return {"accepted": int(acc), "rejected": int(rej),
                "duration_sec": round(float(dur_ms)/ 1000.0, 2)}
    except Exception as e:
        log.warning("could not read load_streams for %s: %s", stream_name, e)
        return {"accepted": 0, "rejected": 0, "duration_sec": 0.0}


def build_fact_copy_sql(cfg, stream_name, work_dir, db_node):
    """COPY all .gz files from a server-side directory on db_node.
    Vertica reads the files directly (no Python in the data path).
    Reject table is per-stream to avoid concurrent CREATE collisions."""

    rejectmax     = int(cfg["load"].get("max_bad_records", 0))
    rejectedtable = "rejectted_" + stream_name

    return (
        f"COPY {fq(cfg,'target_table')} "
        f"FROM '{work_dir}/*.gz' ON {db_node} GZIP "
        f"DELIMITER E'{cfg['load']['delimiter']}' "
        f"STREAM NAME '{stream_name}' "
        f"REJECTED DATA AS TABLE {rejectedtable} "
        f"REJECTMAX {rejectmax} "
        f"DIRECT"
    )


def tar_stream_cmd(path):
    """ The files are gzip-compressed, so there are TWO gzip layers:
      tar -xzO  -> peels the outer .tar.gz and concatenates the inner gz members
      zcat      -> decompresses those inner members (multi-member gzip) to TSV
    `pipefail` (via bash) makes the pipeline fail if tar fails, so a corrupt
    archive still aborts the load instead of committing partial data.
    """
    inner = f"tar -xzOf {shlex.quote(path)} | zcat"
    return f"bash -o pipefail -c {shlex.quote(inner)}"


def load_one_archive(cfg, entry, stream_name):
    """Thread worker: atomically load ONE archive on its OWN connection to the
    node that holds it (so parse work spreads across the cluster).

    Steps:
      1. SSH: tar -xzf archive into /tmp/vload_<stream>/<stem>/  (original file untouched)
      2. COPY FROM '/tmp/.../*.gz' ON <db_node> GZIP             (Vertica reads directly)
      3. SSH: rm -rf work dir                                     (always, even on failure)

    A COPY error or untar failure rolls the whole archive back — no partial loads.
    """
    result = {**entry, "stream_name": stream_name, "load_time": datetime.now(),
              "status": "FAILED", "error": None, "accepted": 0, "rejected": 0}

    stem     = entry["name"].replace(".tar.gz", "").replace(".tar", "")
    work_dir = f"/tmp/vload_{stream_name}/{stem}"

    conn = get_connection(cfg, host=entry["ssh_host"])   # initiator = the file's node
    cur  = conn.cursor()
    try:
        # Step 1: untar on the node — original .tar.gz is never moved or deleted.
        # --strip-components=1 peels the leading data-tsv/ directory so all
        # .gz members land directly in work_dir/ for the COPY glob to match.
        ssh_run(cfg, entry["ssh_host"],
                f"mkdir -p {shlex.quote(work_dir)} && "
                f"tar -xzf {shlex.quote(entry['path'])} -C {shlex.quote(work_dir)} "
                f"--strip-components=1")

        gz_count = ssh_run(cfg, entry["ssh_host"],
                           f"ls {shlex.quote(work_dir)}/*.gz 2>/dev/null | wc -l").strip()
        log.info("untar done for %s on %s: %s .gz file(s) in %s",
                 entry["name"], entry["db_node"], gz_count, work_dir)

        # Step 2: COPY all extracted .gz files in one statement
        copy_sql = build_fact_copy_sql(cfg, stream_name, work_dir, entry["db_node"])
        log.info("starting COPY %s on %s: %s", entry["name"], entry["db_node"], copy_sql)
        cur.execute(copy_sql)
        conn.commit()

        stats = _stream_stats(cur, stream_name)
        result.update(status="OK", accepted=stats["accepted"],
                      rejected=stats["rejected"])
        log.info("loaded %s on %s (accepted=%d rejected=%d)",
                 entry["name"], entry["db_node"], stats["accepted"], stats["rejected"])
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass   # connection may already be closed (e.g. socket timeout)
        result["error"] = str(e)
        log.error("archive %s FAILED (rolled back): %s", entry["name"], e)
    finally:
        cur.close()
        conn.close()
        # Step 3: always clean up temp dir, leave original .tar.gz in place
        try:
            ssh_run(cfg, entry["ssh_host"], f"rm -rf {shlex.quote(work_dir)}")
        except Exception:
            pass
    return result


def load_tars(cfg, manifest, run_id):
    """Load archives concurrently and scale threads per node as the number of files, We can control global thread count
     on the coordinator Returns (results, totals)"""

    workers = int(cfg["load"].get("max_parallel_loads", len(cfg["cluster"]["nodes"])))
    log.info("Loading %d archive(s), up to %d in parallel Commands", len(manifest), workers)

    results = []
    totals = {"accepted": 0, "rejected": 0}
    starttime = time.time()

    # Define Global loader pool based on threads in YAML file #
    with ThreadPoolExecutor(max_workers=workers) as pool:

        # Submit the tasks to the thread pool #
        futures = [pool.submit(load_one_archive, cfg, e, f"vload_{run_id}_{i:04d}")
                for i, e in enumerate(manifest)]

        # Wait for the tasks to complete and collect the results
        for fut in as_completed(futures):
            r = fut.result()
            if r["status"] == "OK":
                totals["accepted"] += r["accepted"]
                totals["rejected"] += r["rejected"]
            results.append(r)
    totals["wall_sec"] = round(time.time() - starttime, 2)
    return results, totals


# ── dimension (small replicated mapping table) ─── #
def build_dim_copy_sql(cfg):
    """COPY FROM STDIN for the dimension TSV (has a header -> SKIP)."""
    d = cfg["dimension"]
    tbl = f'{cfg["schema"]["target_schema"]}.{d["table"]}'
    return (f"COPY {tbl} FROM STDIN DELIMITER E'{d['delimiter']}' "
            f"SKIP {d.get('skip', 1)} DIRECT")


def load_dimension(conn, cfg):
    """Full-refresh the replicated mapping table from its .tsv, atomically.
    Streamed via `ssh cat` so it works regardless of which node holds the file
    and replicates to all nodes via the UNSEGMENTED projection."""
    d = cfg.get("dimension")
    if not d or not d.get("enabled", False):
        return
    tbl = f'{cfg["schema"]["target_schema"]}.{d["table"]}'
    cur = conn.cursor()
    proc = subprocess.Popen(
        ssh_base(cfg, d["ssh_host"]) + [f"cat {shlex.quote(d['file'])}"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        cur.execute(f"TRUNCATE TABLE {tbl}")          # reference data: full refresh
        cur.copy(build_dim_copy_sql(cfg), proc.stdout)
        proc.stdout.close()
        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(
                f"cat rc={rc}: {proc.stderr.read().decode(errors='replace').strip()}")
        conn.commit()
        log.info("dimension %s loaded from %s", tbl, d["file"])
    except Exception as e:
        conn.rollback()
        try:
            proc.kill()
        except Exception:
            pass
        log.error("dimension load FAILED (rolled back): %s", e)
        raise
    finally:
        cur.close()


# ── audit / post-load ─────────────────────────────────────────────────────────
def record_audit(conn, cfg, results):
    """Write one audit row per file (single-threaded, after loads complete)."""
    cur = conn.cursor()
    for r in results:
        cur.execute(
            f"""INSERT INTO {fq(cfg,'audit_table')}
                (stream_name, file_name, archived_name, db_node, ssh_host,
                 target_table, status, load_time, error_msg)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [r["stream_name"], r["name"], r.get("archived_name", ""),
             r["db_node"], r["ssh_host"], cfg["load"]["target_table"],
             r["status"], r["load_time"], r["error"]])
    conn.commit()
    log.info("recorded %d audit row(s)", len(results))


def archive_file(cfg, entry):
    """Move a loaded file to backup dir, renamed with a .archived suffix
    (the suffix marks it as loaded to the DB). Returns the archived filename."""
    backup = cfg["paths"]["backup_dir"]
    archived = entry["name"] + ".archived"
    ssh_run(cfg, entry["ssh_host"],
            f"mkdir -p {shlex.quote(backup)} && "
            f"mv {shlex.quote(entry['path'])} "
            f"{shlex.quote(backup + '/' + archived)}")
    log.info("archived %s -> %s on %s", entry["name"], archived, entry["ssh_host"])
    return archived


def print_summary(run_id, results, totals):
    """End-of-job report: aggregated totals (record counts from LOAD_STREAMS,
    time is wall-clock of the load phase) + the list of archives loaded
    (original + archived name only — no per-file counts)."""
    loaded = [r for r in results if r["status"] != "FAILED"]
    failed = [r for r in results if r["status"] == "FAILED"]
    dur = totals["wall_sec"]
    rps = int(totals["accepted"] / dur) if dur > 0 else 0

    print()
    print(f"Run / Stream prefix       - vload_{run_id}")
    print(f"Total files loaded        - {len(loaded)}")
    print(f"Total Records loaded      - {totals['accepted']}")
    print(f"Rejected Records          - {totals['rejected']}")
    print(f"Total Time Taken          - {dur} sec")
    print(f"Records Loaded per second - {rps}")
    print()
    print("----- Loaded Data File Information -----")
    hdr = f"{'Node name':<20} | {'File name':<32} | {'Archived File name':<36}"
    print(hdr)
    print("-" * len(hdr))
    for r in loaded:
        print(f"{r['db_node']:<20} | {r['name']:<32} | "
              f"{r.get('archived_name', ''):<36}")

    if failed:
        print()
        print("----- FAILED files -----")
        for r in failed:
            print(f"{r['db_node']:<20} | {r['name']:<32} | {r['error']}")


# ── orchestration ─────────────────────────────────────────────────────────────
def run_setup(cfg):

    # Create Connection to the Cluster
    conn = get_connection(cfg)

    try:
        # Create Table Schema
        create_schema(conn, cfg)

        # Create Tables on Vertica (schemas/*.sql, incl. the load_audit table)
        create_tables(conn, cfg)

        # Validate the Tables are Created.
        validate_objects(conn, cfg)

    finally:
        conn.close()



def run_load_mapping(cfg):
    """Load ONLY the mapping / dimension table (weather_station) full refresh,
    replicated to all nodes. Kept separate from table setup and the fact load so
    reference data can be refreshed on its own cadence."""
    conn = get_connection(cfg)
    try:
        load_dimension(conn, cfg)
    finally:
        conn.close()


def run_load(cfg, dry_run=False):

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Get Files on the Nodes.
    node_files = list_files_on_nodes(cfg)
    manifest = build_manifest(cfg, node_files)

    conn = get_connection(cfg)
    try:
        # load-fact is fact-only; the mapping table is loaded via --mode load-mapping
        if cfg["load"].get("check_duplicates", True):
            manifest = filter_already_loaded(conn, cfg, manifest)
        else:
            log.warning("check_duplicates=false — dedup disabled, files may reload")
        print_manifest(manifest)
        if not manifest:
            return
        if dry_run:
            log.info("DRY-RUN COPY:\n%s", build_fact_copy_sql(cfg, f"vload_{run_id}_NNNN"))
            for e in manifest:
                log.info("DRY-RUN stream: ssh %s %s | COPY ... FROM STDIN",
                         e["ssh_host"], tar_stream_cmd(e["path"]))
            return

        results, totals = load_tars(cfg, manifest, run_id)   # own conn per archive

        # optionally move loaded archives to backup_dir (+.archived) before auditing.
        # When disabled, files stay put — dedup still skips them via the audit table.

        record_audit(conn, cfg, results)
    finally:
        conn.close()

    print_summary(run_id, results, totals)


def _managed_tables(cfg, include_audit=True, include_dimension=True):
    """Fully qualified names of the tables the loader manages. The audit
    (load history) and dimension (reference data) tables are optional:
    cleanup-fact keeps both; destroy drops everything."""

    tables = [fq(cfg, "target_table")]

    if include_audit:
        tables.append(fq(cfg, "audit_table"))
    if include_dimension and cfg.get("dimension", {}).get("enabled"):
        tables.append(f'{cfg["schema"]["target_schema"]}.{cfg["dimension"]["table"]}')
    return tables


def run_cleanup_fact(cfg):
    """TRUNCATE the fact + reject tables — wipe loaded fact data, keep the
    structure. The audit history and the dimension are left intact, so you can
    reload fact data without re-running load-mapping. Handy for re-testing."""
    conn = get_connection(cfg)
    try:
        cur = conn.cursor()
        for t in _managed_tables(cfg, include_audit=False, include_dimension=False):
            try:
                cur.execute(f"TRUNCATE TABLE {t}")
                conn.commit()
                log.info("truncated %s", t)
            except Exception as e:                    # table may not exist yet
                conn.rollback()
                log.warning("skip truncate %s: %s", t, e)
    finally:
        conn.close()


def run_destroy(cfg):
    """DROP all managed tables (CASCADE drops projections) and clear backup dirs.
    Full teardown."""
    conn = get_connection(cfg)
    schema = "public"

    try:
        cur = conn.cursor()
        for t in _managed_tables(cfg):
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            log.info("dropped %s", t)

        # Drop all per-stream reject tables created by COPY (rejectted_vload_*)
        cur.execute(
            "SELECT table_name FROM v_catalog.tables "
            "WHERE table_schema = %s AND table_name LIKE 'rejectted_vload_%%'",
            [schema])
        reject_tables = [row[0] for row in cur.fetchall()]
        for tname in reject_tables:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{tname} CASCADE")
            log.info("dropped reject table %s.%s", schema, tname)
        if reject_tables:
            log.info("dropped %d reject table(s)", len(reject_tables))
        else:
            log.info("no reject tables found to drop")

        conn.commit()
    finally:
        conn.close()

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S")
    args = parse_args()
    cfg = load_config(args.config)
    log.info("mode=%s  database=%s  dry_run=%s",
             args.mode, cfg["vertica"]["database"], args.dry_run)

    if args.mode == "setup-tables":
        run_setup(cfg)
    elif args.mode == "load-mapping":
        run_load_mapping(cfg)
    elif args.mode == "all":
        run_setup(cfg)
        run_load_mapping(cfg)
        run_load(cfg, dry_run=args.dry_run)
    elif args.mode == "load-fact":
        run_load(cfg, dry_run=args.dry_run)
    elif args.mode == "cleanup-fact":
        run_cleanup_fact(cfg)
    elif args.mode == "destroy":
        run_destroy(cfg)
    log.info("done.")


if __name__ == "__main__":
    sys.exit(main())
