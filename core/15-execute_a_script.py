from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

engine = create_engine("sqlite://", echo=False, future=True)


def run_script(db: Engine) -> None:
    script = """
        select 1;
        select 2;
        select 3;
    """

    # get sqlite connection
    conn = db.raw_connection()
    conn.executescript("")
    conn.close()
