import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def build_engine_from_env() -> Engine:
    load_dotenv()

    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")

    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_PORT": port,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": pwd,
    }.items() if not v]

    if missing:
        raise RuntimeError(f"Missing required .env keys: {', '.join(missing)}")

    # SQLAlchemy 2.0 + psycopg3 driver
    url = f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(url, future=True, pool_pre_ping=True)