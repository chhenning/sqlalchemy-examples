# Standalone example

# Same as 01-simple.py but with pretty printing a result set using
# "tabulate". That library can print in various formats, such as Markdown, HTML, etc.

# see https://pyneng.readthedocs.io/en/latest/book/12_useful_modules/tabulate.html#

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String

from tabulate import tabulate

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


def print_all_data(format):
    """
    Sort the names and print out the first 10 names using the tabulate package.

    Note, that a row object comes with "_asdict()" function which can be used to retrieve
    the column names.
    """
    sel_stmt = person_table.select().order_by("last", "first").limit(10)

    with db.connect() as conn:

        result = conn.execute(sel_stmt).all()
        if result:
            headers = result[0]._asdict().keys()
            data = [row for row in result]
            print(tabulate(data, headers=headers, tablefmt=format))


def run():
    create_db()
    batch_insert_many_rows(1000)

    # also try "pipe" (for markdown) or "html"
    print_all_data("grid")


if __name__ == "__main__":
    run()
