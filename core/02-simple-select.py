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
    # a label like "ron_num" can be used like a field when dealing with a row object.
    row_number_stmt = (
        func.row_number().over(order_by=["last", "first"]).label("row_num")
    )

    last_name_starts_with_a_cond = person_table.c.last.like(like_cond)

    with db.connect() as conn:

        sel_stmt = (
            select(
                row_number_stmt,
                person_table.c.last,
                person_table.c.first,
            )
            .where(last_name_starts_with_a_cond)
            .order_by("last", "first")
        )

        for row in conn.execute(sel_stmt).all():
            print(row.row_num, row.last, row.first)


def count_names_by_starting_letters():
    """
    Count the names by its first two letters.

    In SQLAlchemy a condition can be its own object, like substr_stmt.
    """
    substr_stmt = func.substr(person_table.c.last, 0, 3).label("starting_letters")
    sel_stmt = select(substr_stmt, func.count().label("count")).group_by(substr_stmt)

    with db.connect() as conn:
        for row in conn.execute(sel_stmt.limit(10)).all():
            print(row)


def run():
    create_db()
    batch_insert_many_rows(1000)

    # print all names that start with "ag".
    enumerate_names("ag%")

    # print the count
    count_names_by_starting_letters()


if __name__ == "__main__":
    run()
