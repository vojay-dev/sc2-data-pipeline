import logging

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DUCK_DB = "sc2data.db"

CLIENT_ID = Variable.get("client_id")
CLIENT_SECRET = Variable.get("client_secret")

BASE_URI = "https://eu.api.blizzard.com"
REGION_ID = 2  # Europe

# retry strategy for contacting the StarCraft 2 API
max_retries = 4
backoff_factor = 2


@dag(start_date=pendulum.now())
def sc2():
    retry_strategy = Retry(total=max_retries, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount('https://', adapter)

    @task
    def get_access_token() -> str:
        data = {
            "grant_type": "client_credentials"
        }

        response = session.post("https://oauth.battle.net/token", data=data, auth=(CLIENT_ID, CLIENT_SECRET))
        return response.json()["access_token"]

    @task
    def get_grandmaster_ladder_data(token: str):
        headers = {
            "Authorization": f"Bearer {token}",
        }

        response = session.get(f"{BASE_URI}/sc2/ladder/grandmaster/{REGION_ID}", headers=headers)
        ladder_teams = response.json()["ladderTeams"]
        return [{
            "id": lt["teamMembers"][0]["id"],
            "realm": lt["teamMembers"][0]["realm"],
            "region": lt["teamMembers"][0]["region"],
            "display_name": lt["teamMembers"][0]["displayName"],
            "clan_tag": lt["teamMembers"][0]["clanTag"] if "clanTag" in lt["teamMembers"][0] else None,
            "favorite_race": lt["teamMembers"][0]["favoriteRace"] if "favoriteRace" in lt["teamMembers"][0] else None,
            "previous_rank": lt["previousRank"],
            "points": lt["points"],
            "wins": lt["wins"],
            "losses": lt["losses"],
            "mmr": lt["mmr"] if "mmr" in lt else None,
            "join_timestamp": lt["joinTimestamp"]
        } for lt in ladder_teams if lt["teamMembers"] and len(lt["teamMembers"]) == 1]

    def get_profile_metadata(token, region, realm, player_id):
        headers = {
            "Authorization": f"Bearer {token}",
        }

        response = session.get(f"{BASE_URI}/sc2/metadata/profile/{region}/{realm}/{player_id}", headers=headers)
        return response.json() if response.status_code == 200 else None

    @task
    def enrich_data(token, data):
        logger.info("Fetching metadata for %d players", len(data))

        i = 0
        for player in data:
            logger.info("Fetching metadata for player %d/%d", i := i + 1, len(data))

            metadata = get_profile_metadata(token, player["region"], player["realm"], player["id"])
            player["profile_url"] = metadata["profileUrl"] if metadata else None
            player["avatar_url"] = metadata["avatarUrl"] if metadata else None
            player["name"] = metadata["name"] if metadata else None

        return data

    @task
    def create_pandas_df(data):
        return pd.DataFrame(data)

    @task
    def store_data(ladder_df: pd.DataFrame):
        conn = duckdb.connect(DUCK_DB)
        conn.sql(f"""
            DROP TABLE IF EXISTS ladder;
            CREATE TABLE ladder AS
            SELECT * FROM ladder_df;
        """)

    access_token = get_access_token()
    ladder_data = get_grandmaster_ladder_data(access_token)
    enriched_data = enrich_data(access_token, ladder_data)
    df = create_pandas_df(enriched_data)
    store_data(df)


sc2()
