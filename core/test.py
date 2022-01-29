from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, JSON

engine = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()

data_table = Table(
    "data_table", meta, Column("id", Integer, primary_key=True), Column("data", JSON)
)

meta.create_all(engine)

with engine.connect() as conn:
    conn.execute(data_table.insert(), data={"key1": "value1", "key2": "value2"})
