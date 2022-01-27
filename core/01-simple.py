#
# Create an in-memory database with one table (person) and fill it up with some names.
# Do some simple select queries.

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String

fake = Faker()
Faker.seed(0)  # make sure to also create the same data


# create an in-memory table, turn on `echo=True` to see what sql statements
# are emitted by SQLAlchemy.
db = create_engine("sqlite://", echo=True, future=True)

meta = MetaData()

person_table = Table(
    "person",
    meta,
    Column("id", Integer, primary_key=True),
    Column("first", String, nullable=False),
    Column("last", String, nullable=False),
)


def create_db():
    meta.create_all(db)


def insert_one_row():
    with db.connect() as conn:
        ins_stmt = person_table.insert().values(
            first=fake.first_name(), last=fake.last_name()
        )
        conn.execute(ins_stmt)
        conn.commit()


def batch_insert_many_rows():
    # create a list of dicts
    names = [dict(first=fake.first_name(), last=fake.last_name()) for _ in range(10)]

    with db.connect() as conn:
        ins_stmt = person_table.insert()
        conn.execute(ins_stmt, names)
        conn.commit()


def print_all_data():
    with db.connect() as conn:
        sel_stmt = person_table.select()
        for row in conn.execute(sel_stmt).all():
            print(row)


def run():
    create_db()
    insert_one_row()
    batch_insert_many_rows()
    print_all_data()


if __name__ == "__main__":
    run()
