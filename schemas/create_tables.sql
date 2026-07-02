-- ============================================================================
-- DDL definitions for the weather data schema
-- ============================================================================

-- Fact table
CREATE TABLE IF NOT EXISTS weather_fact (
    stn            VARCHAR(6)  ENCODING RLE,
    wban           VARCHAR(5)  ENCODING RLE,
    yearmoda       DATE        NOT NULL ENCODING DELTAVAL,  -- partition key
    temp FLOAT,
    temp_count INT,
    dewp FLOAT,
    dewp_count INT,
    slp  FLOAT,
    slp_count  INT,
    stp  FLOAT,
    stp_count  INT,
    visib FLOAT,
    visib_count INT,
    wdsp FLOAT,
    wdsp_count INT,
    mxspd FLOAT,
    gust FLOAT,
    max_temp FLOAT,
    max_flag VARCHAR(1),
    min_temp FLOAT,
    min_flag VARCHAR(1),
    prcp FLOAT,
    prcp_flag VARCHAR(1),
    sndp FLOAT,
    fog_indctr     INT ENCODING RLE,
    rain_indctr    INT ENCODING RLE,
    snow_indctr    INT ENCODING RLE,
    hail_indctr    INT ENCODING RLE,
    thunder_indctr INT ENCODING RLE,
    tornado_indctr INT ENCODING RLE
)
PARTITION BY YEAR(yearmoda);

-- Fact super-projection
-- HASH(stn,wban) co-locates each station's full history on one node so
-- GROUP BY stn,wban is fully local (no resegmentation). Sort key collapses
-- stn/wban RLE runs and DELTAVAL-compresses sequential dates.
CREATE PROJECTION IF NOT EXISTS weather_fact_super (
  stn ENCODING RLE, wban ENCODING RLE, yearmoda ENCODING DELTAVAL,
  temp, max_temp, min_temp, dewp, slp, stp, visib, wdsp, mxspd, gust, prcp, sndp,
  temp_count, dewp_count, slp_count, stp_count, visib_count, wdsp_count,
  max_flag, min_flag, prcp_flag,
  fog_indctr, rain_indctr, snow_indctr, hail_indctr, thunder_indctr, tornado_indctr
) AS
SELECT stn, wban, yearmoda,
       temp, max_temp, min_temp, dewp, slp, stp, visib, wdsp, mxspd, gust, prcp, sndp,
       temp_count, dewp_count, slp_count, stp_count, visib_count, wdsp_count,
       max_flag, min_flag, prcp_flag,
       fog_indctr, rain_indctr, snow_indctr, hail_indctr, thunder_indctr, tornado_indctr
FROM weather_fact
ORDER BY stn, wban, yearmoda
SEGMENTED BY HASH(stn, wban) ALL NODES;

-- Dimension table
CREATE TABLE IF NOT EXISTS weather_station (
  station_name VARCHAR(64), country VARCHAR(64), fips VARCHAR(2),
  state VARCHAR(2), call VARCHAR(8),
  lat FLOAT, lon FLOAT, elevation FLOAT,
  usaf VARCHAR(6), wban VARCHAR(5)
);

-- Replicated (unsegmented) projection: join is local, never broadcasts.
CREATE PROJECTION IF NOT EXISTS weather_station_rep AS
SELECT station_name, country, fips, state, call, lat, lon, elevation, usaf, wban
FROM weather_station
ORDER BY usaf, wban
UNSEGMENTED ALL NODES;

-- Load Job audit & history
-- One row per archive load (archive <-> stream). Record counts and timing are
-- NOT duplicated here — they live in V_MONITOR.LOAD_STREAMS, joined by
-- stream_name. Must match cfg load.audit_table (default: load_audit).
CREATE TABLE IF NOT EXISTS load_audit (
  load_id        IDENTITY,
  stream_name    VARCHAR(128),
  file_name      VARCHAR(512),
  archived_name  VARCHAR(512),
  db_node        VARCHAR(128),
  ssh_host       VARCHAR(128),
  target_table   VARCHAR(128),
  status         VARCHAR(20),
  load_time      TIMESTAMP,
  error_msg      VARCHAR(4000)
);

