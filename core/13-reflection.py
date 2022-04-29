from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy import select, func


db = create_engine("path/to/file.db", echo=False, future=True)

metadata_obj = MetaData()

table = Table("sqlite:///" + "table_name", metadata_obj, autoload_with=db)

with db.connect() as db:

    stmt = select(table).order_by(func.random())

    for row in db.execute(stmt.limit(100)).all():
        print(row)
