#
# Create an in-memory database with one table (person) and fill it up with some names.
# Do some simple select queries.

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import select, func

fake = Faker()
Faker.seed(0)  # make sure to also create the same data


# create an in-memory table, turn on `echo=True` to see what sql statements
# are emitted by SQLAlchemy.
db = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()

person_table = Table(
    "person",
    meta,
    Column("id", Integer, primary_key=True),
    Column("first", String, nullable=False),
    Column("last", String, nullable=False),
)


def create_db():
    """
    Create the table. Aka, DDL.
    """
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
        sel_stmt = person_table.select().order_by(
            person_table.c.last, person_table.c.first
        )
        for row in conn.execute(sel_stmt).all():
            print(row)


def count_num_rows():
    """
    Print number of rows.

    Note that the standalone `select` is being used.
    """
    with db.connect() as conn:
        count_stmt = select(func.count()).select_from(person_table)
        print("num rows:", conn.execute(count_stmt).scalar())


def run():
    create_db()
    insert_one_row()
    batch_insert_many_rows()
    print_all_data()
    count_num_rows()


if __name__ == "__main__":
    run()
