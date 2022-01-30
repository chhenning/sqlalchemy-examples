# Standalone example
import json

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, JSON, String
from sqlalchemy import select, func, desc

fake = Faker()
Faker.seed(0)  # make sure to also create the same data


# create an in-memory table, turn on `echo=True` to see what sql statements
# are emitted by SQLAlchemy.
db = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()

person_tbl = Table(
    "person",
    meta,
    Column("id", Integer, primary_key=True),
    Column("data", JSON),
)


def create_db():
    """
    Create the table. Aka, DDL.
    """
    meta.create_all(db)


def create_person():
    address = dict(
        street=fake.street_address(), postcode=fake.postcode(), city=fake.city()
    )

    return dict(
        first_name=fake.first_name(), last_name=fake.last_name(), address=address
    )


def insert_one_row():
    ins_stmt = person_tbl.insert()
    data = {"data": create_person()}

    with db.connect() as conn:

        conn.execute(ins_stmt, data)
        conn.commit()


def run():
    create_db()
    insert_one_row()

    with db.connect() as conn:

        # retrieve top level "first_name    "
        for row in conn.execute(select(person_tbl.c.data["first_name"])):
            print(row)

        # retrieve path: address->street
        for row in conn.execute(select(person_tbl.c.data[("address", "street")])):
            print(row)


if __name__ == "__main__":
    run()
