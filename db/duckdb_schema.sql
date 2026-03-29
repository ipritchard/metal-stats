-- DuckDB DDL for metal-stats (analytical store)

CREATE TABLE IF NOT EXISTS bands (
    id            VARCHAR PRIMARY KEY,
    name          VARCHAR NOT NULL,
    country       VARCHAR,
    location_raw  VARCHAR,
    lat           DOUBLE,
    lon           DOUBLE,
    geo_precision VARCHAR,
    formed_year   INTEGER,
    status        VARCHAR,
    genres        VARCHAR[]
);

CREATE TABLE IF NOT EXISTS albums (
    id        VARCHAR PRIMARY KEY,
    band_id   VARCHAR,
    title     VARCHAR NOT NULL,
    year      INTEGER,
    type      VARCHAR
);

CREATE TABLE IF NOT EXISTS members (
    id         INTEGER PRIMARY KEY,
    band_id    VARCHAR,
    name       VARCHAR NOT NULL,
    role       VARCHAR,
    is_current BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS album_rankings (
    id         INTEGER PRIMARY KEY,
    band_id    VARCHAR,
    album_id   VARCHAR,
    source     VARCHAR NOT NULL,
    rank       INTEGER,
    csv_rank   INTEGER,
    score      DOUBLE
);

CREATE TABLE IF NOT EXISTS lastfm_stats (
    band_id    VARCHAR PRIMARY KEY,
    listeners  INTEGER,
    play_count INTEGER,
    tags       VARCHAR[],
    similar    VARCHAR[]
);

CREATE TABLE IF NOT EXISTS lyrics_meta (
    band_id          VARCHAR PRIMARY KEY,
    word_frequencies VARCHAR
);

CREATE TABLE IF NOT EXISTS geo_cache (
    query  VARCHAR PRIMARY KEY,
    lat    DOUBLE,
    lon    DOUBLE
);
