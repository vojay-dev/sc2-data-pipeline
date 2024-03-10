import streamlit as st
import duckdb

con = duckdb.connect(database="sc2data.db", read_only=True)

st.title("StarCraft 2 Grandmaster Ladder")


@st.cache_data
def load_ladder_data():
    df = con.execute("SELECT * FROM LADDER").df()

    # sort by mmr and move avatar to first column
    df.sort_values("mmr")
    avatar_url = df.pop("avatar_url")
    df.insert(0, "avatar", avatar_url)

    return df


@st.cache_data
def load_favorite_race_distribution_data():
    df = con.execute("""
        SELECT favorite_race, COUNT(*) AS count
        FROM LADDER
        WHERE favorite_race IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
    """).df()
    return df


ladder = load_ladder_data()

st.dataframe(ladder, column_config={
    "avatar": st.column_config.ImageColumn("avatar")
})

distribution_data = load_favorite_race_distribution_data()
st.bar_chart(distribution_data, x="favorite_race", y="count")
