# Standalone example

# Same as 01-simple.py but with more complex select statements.
# Features shown include like clause, func.row_number


from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import select, func, desc

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


def batch_insert_many_rows(num):
    """
    Batch insert some data into person table.
    """
    # create a list of dicts
    names = [dict(first=fake.first_name(), last=fake.last_name()) for _ in range(num)]

    with db.connect() as conn:
        ins_stmt = person_table.insert()
        conn.execute(ins_stmt, names)
        conn.commit()


def enumerate_names(like_cond):
    """
    Enumerate all names that fullfill the condition.
    """

    with db.connect() as conn:
        last_name_starts_with_a_cond = person_table.c.last.like(like_cond)

        sel_stmt = (
            select(
                func.row_number().over(order_by=["last", "first"]),
                person_table.c.last,
                person_table.c.first,
            )
            .where(last_name_starts_with_a_cond)
            .order_by("last", "first")
        )

        for row in conn.execute(sel_stmt).all():
            print(row)


def run():
    create_db()
    batch_insert_many_rows(1000)

    # print all names that start with "ag".
    enumerate_names("ag%")


if __name__ == "__main__":
    run()
