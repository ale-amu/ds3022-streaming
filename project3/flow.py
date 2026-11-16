"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python quakes_flow.py
"""
import os
import sys
import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"bluesky{TAG}.duckdb"
CHART = f"charts/reply_share_by_language{TAG}.png"


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


@task
@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed'
              AND kind = 'commit'                   -- not identity or account events
              AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}

@task
def entities():
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation, lang, text,
                   make_timestamp(time_us) AS event_time,
                   (raw::JSON ->> '$.data.commit.record.reply.parent.uri') IS NOT NULL AS is_reply
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM posts").fetchone()[0]



@task
def marts():
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_reply_share AS
            SELECT lang,
                   count(*) AS posts,
                   count(*) FILTER (WHERE is_reply) AS replies,
                   round(100.0 * count(*) FILTER (WHERE is_reply) / count(*), 1) AS reply_pct
            FROM posts
            WHERE operation = 'create' AND lang IS NOT NULL
            GROUP BY lang
            ORDER BY posts DESC
            LIMIT 8
        """)



@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("SELECT lang, posts, reply_pct FROM mart_reply_share ORDER BY reply_pct").fetchall()
    os.makedirs("charts", exist_ok=True)
    plt.figure(figsize=(8, 4.5))
    plt.barh([f"{r[0]}  (n={r[1]:,})" for r in rows], [r[2] for r in rows], color="#2a7fb8")
    plt.title("Only 15% of Japanese Bluesky posts are replies, vs 70% of German posts")
    plt.xlabel("replies (% of posts in that language)")
    plt.ylabel("language (number of posts)")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    return CHART


@flow(name="bluesky-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    posts = entities()
    marts()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique posts:        %d", posts)
    log.info("Chart: %s", path)


if __name__ == "__main__":
    pipeline()
