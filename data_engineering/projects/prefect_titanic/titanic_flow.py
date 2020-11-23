import os
import pandas as pd
import sqlalchemy
import sqlite3
import prefect
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
from datetime import timedelta
from dotenv import load_dotenv


load_dotenv(os.path.join('.', '.env'))

DELAY = timedelta(minutes=1)
RUNNER_KEY = os.getenv('AGENT_TOKEN')

scheduler = IntervalSchedule(interval=DELAY)

@task
def get_data() -> pd.DataFrame:
    df = pd.read_csv('https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv')
    return df

@task
def calculate_mean_age(df: pd.DataFrame) -> float:
    return df.Age.mean()

@task
def show_mean_age(m: float) -> None:
    logger = prefect.context.get('logger')
    logger.info(f'A media de Idade é de: {m}')

@task
def show_dataframe(df: pd.DataFrame) -> None:
    logger = prefect.context.get('logger')
    logger.info(df.head().to_json())

@task
def write_to_db(df: pd.DataFrame) -> None:
    engine = sqlalchemy.create_engine('sqlite:///../../db/mini_db.db')
    df.to_sql('titanic', con=engine, if_exists='append')


with Flow('titanic01', schedule=scheduler) as flow:
    df = get_data()
    age_mean = calculate_mean_age(df)
    show_mean = show_mean_age(age_mean)
    show_df = show_dataframe(df)
    load_to_dw = write_to_db(df)

flow.register(project_name='igti-titanic', idempotency_key=flow.serialized_hash())
flow.run_agent(token=RUNNER_KEY)
