import pandas as pd 
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os 
from pathlib import Path

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")


if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Variables de entorno incompletas.")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")


# Read the entire table into a DataFrame
df = pd.read_sql_query(text("SELECT * FROM events2343;"), con=engine)


total_shots = df['type'] == 'Shot'
total_goals = df[(df['type'] == 'Shot') & (df['shot_isGoal'] == True)][['type', 'shot_isGoal']]

goal_percentage_normal = (len(total_goals) / total_shots.sum()) * 100
print(f"Goal Percentage (Normal): {goal_percentage_normal:.2f}%")