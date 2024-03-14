"""
DuckDB is an embeddable SQL OLAP Database Management System, this demo shows how to effortlessly switch
between different technologies within your data wrangling scripts. Starting with a persisted DuckDB, performing
some processing in Pandas, back to an in-memory DuckDB, and finally back to Pandas.
"""

import duckdb

if __name__ == '__main__':
    # use persisted duckdb
    with duckdb.connect(database="sc2data.db") as conn:
        df = conn.sql(f"""
            SELECT
                favorite_race,
                SUM(wins) AS total_wins,
                SUM(losses) AS total_losses,
                MAX(mmr) AS max_mmr,
                AVG(mmr) AS avg_mmr
            FROM ladder
            WHERE favorite_race IN ('protoss', 'terran', 'zerg')
            GROUP BY favorite_race
            ORDER BY total_wins DESC;
        """).df()
        print(df)

    # data wrangling in pandas
    df["win_pct"] = (df["total_wins"] / (df["total_wins"] + df["total_losses"]) * 100)

    # use in-memory duckdb for further processing
    duckdb.sql("""
        CREATE TABLE aggregation AS
        SELECT CASE
            WHEN favorite_race = 'protoss' THEN 'p'
            WHEN favorite_race = 'terran' THEN 't'
            WHEN favorite_race = 'zerg' THEN 'z'
        END AS fav_rc,
        total_wins + total_losses AS total_games,
        win_pct
        FROM df;
    """)

    # back to pandas
    df_agg = duckdb.sql("SELECT * FROM aggregation;").df()
    print(df_agg)
