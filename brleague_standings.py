from __functions__ import postgres_upsert
from sqlalchemy import create_engine
from tqdm import tqdm
import pandas as pd
import requests
import ast
import sys
import os


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

def get_standings_teams():
    print("\nGetting standings and teams...")
    headers = {"Authorization": "Bearer live_d5a0231e08173f1a50858a385aca94"}
    response = requests.get("https://api.api-futebol.com.br/v1/campeonatos/10/tabela", headers=headers)
    data = ast.literal_eval(response.text)
    teams_list = list()
    standings_list = list()
    for dc in tqdm(data):
        standing = {"team_id": dc["time"]["time_id"],
                    "position": dc["posicao"],
                    "matches": dc["jogos"],
                    "points": dc["pontos"],
                    "wins": dc["vitorias"],
                    "draws": dc["empates"],
                    "losses": dc["derrotas"],
                    "use": dc["aproveitamento"],
                    "pro_goals": dc["gols_pro"],
                    "own_goals": dc["gols_contra"],
                    "goal_difference": dc["saldo_gols"],
                    "last_games": ", ".join(dc["ultimos_jogos"]),
                    "variation": dc["variacao_posicao"]}
        team = {"team_id": int(dc["time"]["time_id"]),
                "team_name": dc["time"]["nome_popular"],
                "abbreviation": dc["time"]["sigla"],
                "team_logo": dc["time"]["escudo"]}
        standings_list.append(standing)
        teams_list.append(team)
    standings = pd.DataFrame(standings_list)
    teams = pd.DataFrame(teams_list)
    return standings, teams


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Data & Code ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

print(f"\nRunnning {os.path.basename(sys.argv[0])}")

df_standings, df_teams = get_standings_teams()
postgres_url = 'postgresql+psycopg2://postgres:SOEGC7rVhI5MeDni7drC@' \
               'andrehaffner.c83efwb545b8.ca-central-1.rds.amazonaws.com/brazilian_soccer'
engine = create_engine(postgres_url)
postgres_upsert(engine, df_standings, 'brazilian_league2023', "team_id")
postgres_upsert(engine, df_teams, 'brazilian_teams', "team_id")

print(f"\nFinished {os.path.basename(sys.argv[0])}")
print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
