# Standalone example

# Create an in-memory database with three tables, person, credit-card, address.
# The relationship goes as follows, a person can have multiple (0-N) credit-cards and
# addresses.

# The SQLAlchemy documention is full of foreign keys examples with two tables. Hence
# I'm choosing more then two to illustrate certain concepts.

from random import choice, seed

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy import select, func, insert, delete

fake = Faker()
Faker.seed(0)  # make sure to also create the same data

seed(0)

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
    Column("country", String, nullable=False),
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
        country=fake.country(),
    )


def insert_one_person():
    p = dict(first=fake.first_name(), last=fake.last_name())

    person_ins_stmt = insert(person_tbl)
    cc_ins_stmt = insert(cc_tbl)
    address_ins_stmt = insert(address_tbl)

    with db.connect() as conn:

        p_id = conn.execute(person_ins_stmt, p).lastrowid

        conn.execute(cc_ins_stmt, create_cc(p_id))
        conn.execute(cc_ins_stmt, create_cc(p_id))

        conn.execute(address_ins_stmt, create_address(p_id))

        conn.commit()


def batch_insert_many_people(num_people):
    # generate some random people with varying numbers of addresses and credit
    # cards.
    person_ins_stmt = insert(person_tbl)
    cc_ins_stmt = insert(cc_tbl)
    address_ins_stmt = insert(address_tbl)

    with db.connect() as conn:
        for _ in range(num_people):
            p = dict(first=fake.first_name(), last=fake.last_name())
            p_id = conn.execute(person_ins_stmt, p).lastrowid

            for _ in range(choice(range(0, 10))):
                conn.execute(cc_ins_stmt, create_cc(p_id))

            for _ in range(choice(range(0, 10))):
                conn.execute(address_ins_stmt, create_address(p_id))

        conn.commit()


def print_all():
    with db.connect() as conn:
        sel_stmt = (
            select(person_tbl, cc_tbl, address_tbl)
            .select_from(person_tbl)
            .join(cc_tbl)
            .join(address_tbl)
            .order_by("last", "first")
        )

        for row in conn.execute(sel_stmt).all():
            print(row)


def count_all():
    """
    Make a list of all people by name and their count of credit cards and addresses.

    All persons need to show up, hence the left outer join.

    Use a CTE to stack the queries.
    """
    count_cc_stmt = (
        select(person_tbl.c.id.label("pid"), func.count(cc_tbl.c.id).label("cc_count"))
        .select_from(person_tbl)
        .join(cc_tbl, isouter=True)
        .group_by(person_tbl.c.id)
    ).cte("count_cc_stmt")

    count_address_stmt = (
        select(
            person_tbl.c.id.label("pid"),
            func.count(address_tbl.c.id).label("address_count"),
        )
        .select_from(person_tbl)
        .join(address_tbl, isouter=True)
        .group_by(person_tbl.c.id)
    ).cte("count_address_stmt")

    counts_stmt = (
        select(
            person_tbl.c.first,
            person_tbl.c.last,
            count_cc_stmt.c.cc_count,
            count_address_stmt.c.address_count,
        )
        .select_from(person_tbl)
        .join(count_cc_stmt, onclause=person_tbl.c.id == count_cc_stmt.c.pid)
        .join(count_address_stmt, onclause=person_tbl.c.id == count_address_stmt.c.pid)
        .order_by(person_tbl.c.last, person_tbl.c.first)
    )

    with db.connect() as conn:
        for row in conn.execute(counts_stmt).all():
            print(row)


def delete_one_person():
    with db.connect() as conn:
        delete(person_tbl).where
        # db.execute()

        # conn.commit()


def run():
    create_db()
    insert_one_person()
    batch_insert_many_people(10)

    print_all()
    count_all()

    delete_one_person()


if __name__ == "__main__":
    run()
