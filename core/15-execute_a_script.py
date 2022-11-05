from sqlalchemy import create_engine, text
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
    conn.executescript(script)
    conn.close()


# for postgres this seems to work
def create_views():
    views = open("sql/create_views.sql").read()
    conn = engine.connect()
    conn.execute(text(views))
    conn.commit()  # important!!!
    conn.close()
