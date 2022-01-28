# Standalone example

# Create an in-memory database with three tables, person, credit-card, address.
# The relationship goes as follows, a person can have multiple (0-N) credit-cards and
# addresses.

# The SQLAlchemy documention is full of foreign keys examples with two tables. Hence
# I'm choosing more then two to illustrate certain concepts.

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy import select, func, desc, insert

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
    Column("first", String, nullable=False),
    Column("last", String, nullable=False),
)

cc_tbl = Table(
    "credit_card",
    meta,
    Column("id", Integer, primary_key=True),
    Column("person_id", ForeignKey("person.id"), nullable=False),
    Column("provider", String, nullable=False),
    Column("number", String, nullable=False),
    Column("security_code", String, nullable=False),
    Column("expire", String, nullable=False),
)

address_tbl = Table(
    "address",
    meta,
    Column("id", Integer, primary_key=True),
    Column("person_id", ForeignKey("person.id"), nullable=False),
    Column("street_address", String, nullable=False),
    Column("postcode", String, nullable=False),
    Column("city", String, nullable=False),
)


def create_db():
    """
    Create the table. Aka, DDL.
    """
    meta.create_all(db)


def create_cc(person_id):
    return dict(
        person_id=person_id,
        provider=fake.credit_card_provider(),
        number=fake.credit_card_number(),
        security_code=fake.credit_card_security_code(),
        expire=fake.credit_card_expire(),
    )


def create_address(person_id):
    return dict(
        person_id=person_id,
        street_address=fake.street_address(),
        postcode=fake.postcode(),
        city=fake.city(),
    )


def insert_one_person():
    p = dict(first=fake.first_name(), last=fake.last_name())

    person_ins_stmt = insert(person_tbl)
    cc_ins_stmt = insert(cc_tbl)
    address_ins_stmt = insert(address_tbl)

    with db.connect() as conn:

        person_id = conn.execute(person_ins_stmt, p).lastrowid

        conn.execute(cc_ins_stmt, create_cc(person_id))
        conn.execute(cc_ins_stmt, create_cc(person_id))

        conn.execute(address_ins_stmt, create_address(person_id))

        conn.commit()


def print_all():
    with db.connect() as conn:
        sel_stmt = (
            select(person_tbl, cc_tbl, address_tbl)
            .select_from(person_tbl)
            .join(cc_tbl)
            .join(address_tbl)
        )

        for row in conn.execute(sel_stmt).all():
            print(row)


def run():
    create_db()
    insert_one_person()

    print_all()


if __name__ == "__main__":
    run()
